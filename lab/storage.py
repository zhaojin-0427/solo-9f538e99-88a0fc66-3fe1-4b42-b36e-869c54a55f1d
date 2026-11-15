"""SQLite 持久化与“文件 + 登记”原子发布。"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import tempfile
import threading
from contextlib import contextmanager

try:  # POSIX 文件锁；用于跨进程串行化发布与清理。
    import fcntl
except ImportError:  # pragma: no cover - 非 POSIX 平台退化为线程锁
    fcntl = None
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from .errors import ConflictError, NotFoundError, PublishError, WithdrawnError
from .parsing import (
    Calibration,
    ResultRow,
    Sample,
    calibration_payload,
    compute_results,
    decimal_plain,
    iso_utc,
    parse_utc,
    payload_json,
    results_to_csv,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    current_revision INTEGER,
    current_version_id INTEGER,
    withdrawn_at TEXT
);
CREATE TABLE IF NOT EXISTS samples (
    batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    line_number INTEGER NOT NULL,
    sample_id TEXT NOT NULL,
    timestamp_text TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    absorbance TEXT NOT NULL,
    PRIMARY KEY (batch_id, sample_id)
);
CREATE TABLE IF NOT EXISTS versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    calibration_json TEXT NOT NULL,
    calibration_hash TEXT NOT NULL,
    calibration_rows INTEGER NOT NULL,
    result_sha256 TEXT NOT NULL,
    result_size INTEGER NOT NULL,
    result_rows INTEGER NOT NULL,
    result_content BLOB NOT NULL,
    file_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (batch_id, revision)
);
CREATE TABLE IF NOT EXISTS results (
    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    sample_id TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    absorbance TEXT NOT NULL,
    slope TEXT NOT NULL,
    intercept TEXT NOT NULL,
    concentration TEXT NOT NULL,
    PRIMARY KEY (version_id, seq)
);
-- 撤回时逐份登记的待删文件路径：只有确认实际删除后才移除对应行，
-- 因此崩溃/重启后可按留存路径继续回收，且与数据卷文件逐份对应。
CREATE TABLE IF NOT EXISTS withdrawn_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    token TEXT NOT NULL,
    file_path TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    UNIQUE (batch_id, file_path)
);
CREATE INDEX IF NOT EXISTS idx_versions_batch ON versions(batch_id, revision);
CREATE INDEX IF NOT EXISTS idx_withdrawn_files_batch ON withdrawn_files(batch_id);
"""


@dataclass(frozen=True)
class BatchInfo:
    id: int
    token: str
    label: str
    created_at: str
    current_revision: int | None
    current_version_id: int | None
    sample_count: int
    result_rows: int
    result_sha256: str
    withdrawn_at: str | None = None
    pending_files: int = 0


@dataclass(frozen=True)
class VersionInfo:
    id: int
    batch_id: int
    revision: int
    calibration_rows: int
    result_rows: int
    result_size: int
    result_sha256: str
    file_path: str
    created_at: str
    calibration_json: str


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


def version_from_row(row: sqlite3.Row) -> VersionInfo:
    return VersionInfo(
        id=row["id"],
        batch_id=row["batch_id"],
        revision=row["revision"],
        calibration_rows=row["calibration_rows"],
        result_rows=row["result_rows"],
        result_size=row["result_size"],
        result_sha256=row["result_sha256"],
        file_path=row["file_path"],
        created_at=row["created_at"],
        calibration_json=row["calibration_json"],
    )


def calibrations_from_json(source: str) -> list[Calibration]:
    calibrations = [
        Calibration(
            row_number=0,
            start=parse_utc(item["start"], "起始 UTC 时刻"),
            end=parse_utc(item["end"], "结束 UTC 时刻"),
            slope=Decimal(item["slope"]),
            intercept=Decimal(item["intercept"]),
        )
        for item in json.loads(source)
    ]
    calibrations.sort(key=lambda item: item.start)
    return calibrations


def normalized_payload(calibrations: list[Calibration]) -> str:
    return payload_json(calibration_payload(calibrations))


class Store:
    def __init__(self, database: str | Path, results_dir: str | Path):
        self.database = str(database)
        self.results_dir = Path(results_dir)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_schema()
        self._lock_file = self._open_lock_file()
        # 启动即按 SQLite 留存的待删路径继续回收（崩溃恢复），再清理未登记文件。
        self.resume_withdrawals()
        self.cleanup_unpublished()

    def _open_lock_file(self):
        # 锁文件句柄需与 Store 同生命周期，不能由 with 提前关闭。
        path = self.results_dir / ".lab.lock"
        return open(path, "a+b")

    @contextmanager
    def _cross_process_lock(self, exclusive: bool):
        with self._lock:
            if fcntl is None:
                yield
                return
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)

    # ---------- 连接 ----------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database, timeout=30, isolation_level=None, check_same_thread=False
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _read(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def write_transaction(self):
        """BEGIN IMMEDIATE：并发提交串行化，持有写锁后再读取当前修订。"""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    def _init_schema(self) -> None:
        with self._read() as db:
            # journal_mode 持久化在库文件中，只需在此设置一次，
            # 避免每条连接都请求独占锁。
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(SCHEMA)
            # 兼容既有数据卷：老库缺少撤回状态列时原地补齐。
            columns = {row["name"] for row in db.execute("PRAGMA table_info(batches)").fetchall()}
            if "withdrawn_at" not in columns:
                db.execute("ALTER TABLE batches ADD COLUMN withdrawn_at TEXT")

    # ---------- 查询 ----------

    def get_batch(self, token: str) -> BatchInfo | None:
        with self._read() as db:
            batch = db.execute("SELECT * FROM batches WHERE token = ?", (token,)).fetchone()
            if batch is None:
                return None
            sample_count = db.execute(
                "SELECT COUNT(*) AS n FROM samples WHERE batch_id = ?", (batch["id"],)
            ).fetchone()["n"]
            pending_files = db.execute(
                "SELECT COUNT(*) AS n FROM withdrawn_files WHERE batch_id = ?", (batch["id"],)
            ).fetchone()["n"]
            current = db.execute(
                "SELECT result_rows, result_sha256 FROM versions WHERE id = ?",
                (batch["current_version_id"],),
            ).fetchone()
            return BatchInfo(
                id=batch["id"],
                token=batch["token"],
                label=batch["label"],
                created_at=batch["created_at"],
                current_revision=batch["current_revision"],
                current_version_id=batch["current_version_id"],
                sample_count=sample_count,
                result_rows=current["result_rows"] if current else 0,
                result_sha256=current["result_sha256"] if current else "",
                withdrawn_at=batch["withdrawn_at"],
                pending_files=pending_files,
            )

    def list_batches(self) -> list[BatchInfo]:
        with self._read() as db:
            rows = db.execute(
                """
                SELECT b.*, COALESCE(s.n, 0) AS sample_count,
                       COALESCE(v.result_rows, 0) AS result_rows,
                       COALESCE(v.result_sha256, '') AS result_sha256,
                       COALESCE(w.n, 0) AS pending_files
                FROM batches b
                LEFT JOIN (SELECT batch_id, COUNT(*) AS n FROM samples GROUP BY batch_id) s
                       ON s.batch_id = b.id
                LEFT JOIN versions v ON v.id = b.current_version_id
                LEFT JOIN (SELECT batch_id, COUNT(*) AS n FROM withdrawn_files GROUP BY batch_id) w
                       ON w.batch_id = b.id
                ORDER BY b.created_at DESC, b.id DESC
                """
            ).fetchall()
        return [
            BatchInfo(
                id=row["id"],
                token=row["token"],
                label=row["label"],
                created_at=row["created_at"],
                current_revision=row["current_revision"],
                current_version_id=row["current_version_id"],
                sample_count=row["sample_count"],
                result_rows=row["result_rows"],
                result_sha256=row["result_sha256"],
                withdrawn_at=row["withdrawn_at"],
                pending_files=row["pending_files"],
            )
            for row in rows
        ]

    @staticmethod
    def _raise_if_withdrawn(batch_row: sqlite3.Row) -> None:
        if batch_row["withdrawn_at"] is not None:
            raise WithdrawnError("该批次已撤回，详情、下载与更正均已阻断")

    def get_version(self, token: str, revision: int | None = None) -> tuple[BatchInfo, VersionInfo] | None:
        with self._read() as db:
            batch_row = db.execute("SELECT * FROM batches WHERE token = ?", (token,)).fetchone()
            if batch_row is None:
                return None
            self._raise_if_withdrawn(batch_row)
            if revision is None:
                version_row = db.execute(
                    "SELECT * FROM versions WHERE id = ?", (batch_row["current_version_id"],)
                ).fetchone()
            else:
                version_row = db.execute(
                    "SELECT * FROM versions WHERE batch_id = ? AND revision = ?",
                    (batch_row["id"], revision),
                ).fetchone()
            if version_row is None:
                return None
            sample_count = db.execute(
                "SELECT COUNT(*) AS n FROM samples WHERE batch_id = ?", (batch_row["id"],)
            ).fetchone()["n"]
            return (
                BatchInfo(
                    id=batch_row["id"],
                    token=batch_row["token"],
                    label=batch_row["label"],
                    created_at=batch_row["created_at"],
                    current_revision=batch_row["current_revision"],
                    current_version_id=batch_row["current_version_id"],
                    sample_count=sample_count,
                    result_rows=version_row["result_rows"],
                    result_sha256=version_row["result_sha256"],
                ),
                version_from_row(version_row),
            )

    def list_versions(self, token: str) -> list[VersionInfo] | None:
        with self._read() as db:
            batch_row = db.execute("SELECT * FROM batches WHERE token = ?", (token,)).fetchone()
            if batch_row is None:
                return None
            self._raise_if_withdrawn(batch_row)
            rows = db.execute(
                "SELECT * FROM versions WHERE batch_id = ? ORDER BY revision DESC",
                (batch_row["id"],),
            ).fetchall()
            return [version_from_row(row) for row in rows]

    def get_samples(self, token: str) -> tuple[BatchInfo, list[Sample], VersionInfo] | None:
        with self._read() as db:
            batch_row = db.execute("SELECT * FROM batches WHERE token = ?", (token,)).fetchone()
            if batch_row is None:
                return None
            self._raise_if_withdrawn(batch_row)
            if batch_row["current_version_id"] is None:
                raise NotFoundError("该批次尚无已发布版本")
            rows = db.execute(
                "SELECT * FROM samples WHERE batch_id = ? ORDER BY line_number",
                (batch_row["id"],),
            ).fetchall()
            samples = [
                Sample(
                    row_number=row["line_number"],
                    sample_id=row["sample_id"],
                    timestamp_text=row["timestamp_text"],
                    timestamp=parse_utc(row["timestamp_utc"], "留存 UTC 时刻"),
                    absorbance=Decimal(row["absorbance"]),
                )
                for row in rows
            ]
            version_row = db.execute(
                "SELECT * FROM versions WHERE id = ?", (batch_row["current_version_id"],)
            ).fetchone()
            batch = BatchInfo(
                id=batch_row["id"],
                token=batch_row["token"],
                label=batch_row["label"],
                created_at=batch_row["created_at"],
                current_revision=batch_row["current_revision"],
                current_version_id=batch_row["current_version_id"],
                sample_count=len(samples),
                result_rows=version_row["result_rows"],
                result_sha256=version_row["result_sha256"],
            )
            return batch, samples, version_from_row(version_row)

    def download(self, token: str, revision: int | None = None) -> tuple[BatchInfo, VersionInfo, bytes] | None:
        # 共享锁包住“撤回检查 + 读取并校验完整文件”：撤回回收持独占锁，
        # 会等待进行中的下载取得完整原文件；撤回登记后的新下载在
        # get_version 处即被拒绝，绝不会读到被回收的文件。
        with self._cross_process_lock(False):
            found = self.get_version(token, revision)
            if found is None:
                return None
            batch, version = found
            content = self._read_published(version)
            return batch, version, content

    def _read_published(self, version: VersionInfo) -> bytes:
        """校验磁盘文件与发布记录，任何不一致都视为该版本不可用。"""
        with self._read() as db:
            row = db.execute(
                "SELECT result_content, result_sha256, result_size FROM versions WHERE id = ?",
                (version.id,),
            ).fetchone()
        if row is None:
            raise NotFoundError("发布记录不存在")
        content = bytes(row["result_content"])
        if _sha256(content) != row["result_sha256"] or len(content) != row["result_size"]:
            raise PublishError("已发布结果记录校验失败，请联系管理员")
        path = (self.results_dir / version.file_path).resolve()
        root = self.results_dir.resolve()
        if root not in path.parents:
            raise PublishError("发布记录的文件路径非法")
        try:
            file_bytes = path.read_bytes()
        except FileNotFoundError:
            raise PublishError("已发布结果文件缺失，请联系管理员") from None
        except OSError as exc:
            raise PublishError(f"已发布结果文件不可读：{exc}") from exc
        if file_bytes != content or _sha256(file_bytes) != version.result_sha256:
            raise PublishError("已发布结果文件与登记内容不一致，请联系管理员")
        return content

    def result_rows(self, token: str, revision: int | None = None) -> list[sqlite3.Row] | None:
        found = self.get_version(token, revision)
        if found is None:
            return None
        _, version = found
        with self._read() as db:
            return db.execute(
                "SELECT * FROM results WHERE version_id = ? ORDER BY seq", (version.id,)
            ).fetchall()

    # ---------- 发布：首次 ----------

    def publish_first(
        self, samples: list[Sample], calibrations: list[Calibration], revision: int, label: str
    ) -> tuple[BatchInfo, VersionInfo]:
        results = compute_results(samples, calibrations)
        content = results_to_csv(results)
        token = secrets.token_hex(8)
        file_rel = f"{token}/revision-{revision}.csv"
        final_path = self.results_dir / file_rel
        temp_path: Path | None = None
        committed = False
        try:
            with self._cross_process_lock(True), self.write_transaction() as db:
                # 临时文件在锁内创建，避免与清理流程竞争。
                temp_path = self._write_temp(content)
                now = _now()
                db.execute(
                    "INSERT INTO batches (token, label, created_at) VALUES (?, ?, ?)",
                    (token, label, now),
                )
                batch_id = db.execute(
                    "SELECT id FROM batches WHERE token = ?", (token,)
                ).fetchone()["id"]
                for sample in samples:
                    db.execute(
                        """INSERT INTO samples
                           (batch_id, line_number, sample_id, timestamp_text, timestamp_utc, absorbance)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            batch_id,
                            sample.row_number,
                            sample.sample_id,
                            sample.timestamp_text,
                            iso_utc(sample.timestamp),
                            decimal_plain(sample.absorbance),
                        ),
                    )
                version_id = self._insert_version(
                    db, batch_id, revision, calibrations, results, content, file_rel, now
                )
                db.execute(
                    "UPDATE batches SET current_revision = ?, current_version_id = ? WHERE id = ?",
                    (revision, version_id, batch_id),
                )
                self._commit_file(temp_path, final_path)
                temp_path = None
                committed = True
        except BaseException:
            # 首次发布失败：清理临时文件；仅当本次已落地文件时才删除它，
            # 并移除该批次残留登记，保证失败不产生可见的半成品。
            if temp_path is not None:
                _unlink(temp_path)
            if committed:
                _unlink(final_path)
            self._discard_batch(token)
            raise

        batch = self.get_batch(token)
        found = self.get_version(token, revision)
        assert batch is not None and found is not None
        return batch, found[1]

    # ---------- 发布：更正 ----------

    def publish_correction(
        self, token: str, calibrations: list[Calibration], revision: int
    ) -> tuple[BatchInfo, VersionInfo, bool]:
        """返回 (批次, 版本, 是否为同号同内容幂等命中)。"""
        loaded = self.get_samples(token)
        if loaded is None:
            raise NotFoundError("批次不存在")
        _, samples, _ = loaded
        results = compute_results(samples, calibrations)
        content = results_to_csv(results)
        payload = normalized_payload(calibrations)
        file_rel = f"{token}/revision-{revision}.csv"
        final_path = self.results_dir / file_rel
        temp_path: Path | None = None
        committed = False
        idempotent = False

        try:
            with self._cross_process_lock(True), self.write_transaction() as db:
                temp_path = self._write_temp(content)
                batch_row = db.execute(
                    "SELECT * FROM batches WHERE token = ?", (token,)
                ).fetchone()
                if batch_row is None:
                    raise NotFoundError("批次不存在")
                # 与撤回交错：撤回登记与本事务都持独占锁串行提交，
                # 此处再次确认，保证撤回生效后绝不重新发布。
                self._raise_if_withdrawn(batch_row)
                batch_id = batch_row["id"]
                current_revision = batch_row["current_revision"]
                if current_revision is None:
                    raise PublishError("批次状态异常：缺少当前修订")

                if revision < current_revision:
                    raise ConflictError(
                        f"修订号 {revision} 低于当前修订号 {current_revision}；更正必须使用更高的修订号",
                        current_revision,
                    )
                if revision == current_revision:
                    existing = db.execute(
                        "SELECT * FROM versions WHERE batch_id = ? AND revision = ?",
                        (batch_id, revision),
                    ).fetchone()
                    if existing is None:
                        raise PublishError("当前修订缺少发布记录，请联系管理员")
                    if normalized_payload(
                        calibrations_from_json(existing["calibration_json"])
                    ) != payload:
                        raise ConflictError(
                            f"修订号 {revision} 已存在且校准内容不同；请使用更高的修订号",
                            current_revision,
                        )
                    # 同号同内容：幂等返回既有版本，不产生新版本。
                    idempotent = True
                    _unlink(temp_path)
                    temp_path = None
                else:
                    now = _now()
                    version_id = self._insert_version(
                        db, batch_id, revision, calibrations, results, content, file_rel, now
                    )
                    db.execute(
                        "UPDATE batches SET current_revision = ?, current_version_id = ? WHERE id = ?",
                        (revision, version_id, batch_id),
                    )
                    self._commit_file(temp_path, final_path)
                    temp_path = None
                    committed = True
        except BaseException:
            # 更正未登记成功：删除本次临时文件；只有本次确实落地了最终文件
            # 才删除它，绝不触碰已发布的其他修订文件。旧版本始终可查看下载。
            if temp_path is not None:
                _unlink(temp_path)
            if committed:
                _unlink(final_path)
            raise

        batch = self.get_batch(token)
        found = self.get_version(token, revision)
        assert batch is not None and found is not None
        return batch, found[1], idempotent

    # ---------- 撤回：登记 + 逐份回收 ----------

    def begin_withdrawal(self, token: str) -> tuple[BatchInfo, list[str], bool]:
        """登记撤回：同一事务内持久写入撤回状态与每个待删文件路径。

        提交瞬间该批即被阻断（详情/下载/更正），随后由 resume_withdrawals
        按登记路径逐份回收。返回 (批次, 本次登记的待删路径, 是否此前已撤回)。
        """
        with self._cross_process_lock(True), self.write_transaction() as db:
            batch_row = db.execute(
                "SELECT * FROM batches WHERE token = ?", (token,)
            ).fetchone()
            if batch_row is None:
                raise NotFoundError("批次不存在")
            if batch_row["withdrawn_at"] is not None:
                already = True
                paths: list[str] = []
            else:
                already = False
                rows = db.execute(
                    "SELECT file_path FROM versions WHERE batch_id = ? ORDER BY revision",
                    (batch_row["id"],),
                ).fetchall()
                now = _now()
                for row in rows:
                    db.execute(
                        """INSERT OR IGNORE INTO withdrawn_files
                           (batch_id, token, file_path, registered_at)
                           VALUES (?, ?, ?, ?)""",
                        (batch_row["id"], token, row["file_path"], now),
                    )
                db.execute(
                    "UPDATE batches SET withdrawn_at = ? WHERE id = ?",
                    (now, batch_row["id"]),
                )
                paths = [row["file_path"] for row in rows]
        batch = self.get_batch(token)
        assert batch is not None
        return batch, paths, already

    def _reclaim_path(self, file_rel: str) -> bool:
        """删除单个待回收文件；确认磁盘上已不存在才返回 True。"""
        root = self.results_dir.resolve()
        path = (root / file_rel).resolve()
        if root not in path.parents:
            return False
        try:
            path.unlink()
        except FileNotFoundError:
            return True  # 文件已不存在，视为删除完成
        except OSError:
            return False
        return True

    def resume_withdrawals(self) -> dict[str, list[str]]:
        """按 SQLite 留存的待删路径逐份回收，随后清除该批样本/结果/版本记录。

        撤回登记后、回收完成前进程中断也没关系：重启或再次触发时按
        withdrawn_files 中留存的路径继续；只有确认实际删除的路径才移除登记，
        全部路径回收完毕才清除 SQLite 内的样本、结果正文与版本记录。
        独占锁与下载的共享锁互斥，进行中的下载可完整读完后回收才开始。
        """
        summary: dict[str, list[str]] = {"deleted": [], "failed": [], "purged": []}
        with self._cross_process_lock(True):
            with self._read() as db:
                rows = db.execute(
                    "SELECT id, file_path FROM withdrawn_files ORDER BY id"
                ).fetchall()
            for row in rows:
                if not self._reclaim_path(row["file_path"]):
                    summary["failed"].append(row["file_path"])
                    continue  # 删除失败：留存路径，等待下次回收
                with self.write_transaction() as db:
                    db.execute("DELETE FROM withdrawn_files WHERE id = ?", (row["id"],))
                summary["deleted"].append(row["file_path"])

            # 待删路径已全部回收的撤回批次：清除样本、结果正文与版本记录。
            purged_tokens: list[str] = []
            with self.write_transaction() as db:
                finished = db.execute(
                    """SELECT b.id, b.token FROM batches b
                       WHERE b.withdrawn_at IS NOT NULL
                         AND NOT EXISTS (
                             SELECT 1 FROM withdrawn_files w WHERE w.batch_id = b.id
                         )"""
                ).fetchall()
                for item in finished:
                    db.execute("DELETE FROM samples WHERE batch_id = ?", (item["id"],))
                    # results 经外键 ON DELETE CASCADE 随 versions 一并清除。
                    db.execute("DELETE FROM versions WHERE batch_id = ?", (item["id"],))
                    db.execute(
                        "UPDATE batches SET current_version_id = NULL WHERE id = ?",
                        (item["id"],),
                    )
                    purged_tokens.append(item["token"])
            summary["purged"] = purged_tokens
            for purged in purged_tokens:
                try:
                    (self.results_dir / purged).rmdir()
                except OSError:
                    pass
        return summary

    # ---------- 内部 ----------

    def _discard_batch(self, token: str) -> None:
        """删除首次发布失败后的批次登记；失败仅记录，不掩盖原始异常。"""
        try:
            with self._cross_process_lock(True), self.write_transaction() as db:
                db.execute("DELETE FROM batches WHERE token = ?", (token,))
        except sqlite3.Error:
            pass

    @staticmethod
    def _commit_file(temp_path: Path, final_path: Path) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temp_path, final_path)

    def _write_temp(self, content: bytes) -> Path:
        try:
            self.results_dir.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix=".publish-", suffix=".tmp", dir=str(self.results_dir))
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
            except BaseException:
                try:
                    os.unlink(name)
                except OSError:
                    pass
                raise
            return Path(name)
        except OSError as exc:
            raise PublishError(f"结果文件写入失败：{exc}") from exc

    def _insert_version(
        self,
        db: sqlite3.Connection,
        batch_id: int,
        revision: int,
        calibrations: list[Calibration],
        results: list[ResultRow],
        content: bytes,
        file_rel: str,
        created_at: str,
    ) -> int:
        payload = normalized_payload(calibrations)
        cursor = db.execute(
            """
            INSERT INTO versions
                (batch_id, revision, calibration_json, calibration_hash, calibration_rows,
                 result_sha256, result_size, result_rows, result_content, file_path, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                revision,
                payload,
                _sha256(payload.encode("utf-8")),
                len(calibrations),
                _sha256(content),
                len(content),
                len(results),
                sqlite3.Binary(content),
                file_rel,
                created_at,
            ),
        )
        version_id = cursor.lastrowid
        for seq, row in enumerate(results, start=1):
            db.execute(
                """INSERT INTO results
                   (version_id, seq, sample_id, timestamp_utc, absorbance, slope, intercept, concentration)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    version_id,
                    seq,
                    row.sample_id,
                    row.timestamp_text,
                    row.absorbance,
                    row.slope,
                    row.intercept,
                    row.concentration,
                ),
            )
        return version_id

    def cleanup_unpublished(self) -> list[str]:
        """删除结果目录中未被已发布版本登记的文件（含崩溃遗留的临时文件）。"""
        removed: list[str] = []
        with self._cross_process_lock(True):
            with self._read() as db:
                rows = db.execute("SELECT file_path FROM versions").fetchall()
            root = self.results_dir.resolve()
            referenced = {(root / row["file_path"]).resolve() for row in rows}
            if not root.exists():
                return removed
            for path in sorted(root.rglob("*")):
                if not path.is_file() or path.name == ".lab.lock":
                    continue
                resolved = path.resolve()
                if root not in resolved.parents:
                    continue
                if resolved in referenced:
                    continue
                try:
                    path.unlink()
                    removed.append(str(path))
                except FileNotFoundError:
                    pass
            for directory in sorted(
                (p for p in root.rglob("*") if p.is_dir()),
                key=lambda p: len(p.parts),
                reverse=True,
            ):
                try:
                    directory.rmdir()
                except OSError:
                    pass
            return removed
