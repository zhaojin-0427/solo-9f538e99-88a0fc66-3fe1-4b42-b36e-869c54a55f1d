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

from .errors import ConflictError, NotFoundError, PublishError
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
    current_version_id INTEGER
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
CREATE INDEX IF NOT EXISTS idx_versions_batch ON versions(batch_id, revision);
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

    # ---------- 查询 ----------

    def get_batch(self, token: str) -> BatchInfo | None:
        with self._read() as db:
            batch = db.execute("SELECT * FROM batches WHERE token = ?", (token,)).fetchone()
            if batch is None:
                return None
            sample_count = db.execute(
                "SELECT COUNT(*) AS n FROM samples WHERE batch_id = ?", (batch["id"],)
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
            )

    def list_batches(self) -> list[BatchInfo]:
        with self._read() as db:
            rows = db.execute(
                """
                SELECT b.*, COALESCE(s.n, 0) AS sample_count,
                       COALESCE(v.result_rows, 0) AS result_rows,
                       COALESCE(v.result_sha256, '') AS result_sha256
                FROM batches b
                LEFT JOIN (SELECT batch_id, COUNT(*) AS n FROM samples GROUP BY batch_id) s
                       ON s.batch_id = b.id
                LEFT JOIN versions v ON v.id = b.current_version_id
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
            )
            for row in rows
        ]

    def get_version(self, token: str, revision: int | None = None) -> tuple[BatchInfo, VersionInfo] | None:
        with self._read() as db:
            batch_row = db.execute("SELECT * FROM batches WHERE token = ?", (token,)).fetchone()
            if batch_row is None:
                return None
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
            batch_row = db.execute("SELECT id FROM batches WHERE token = ?", (token,)).fetchone()
            if batch_row is None:
                return None
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
