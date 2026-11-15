"""整批撤回：阻断、逐份文件回收、崩溃恢复与交错语义测试。"""

import io
import sqlite3
from pathlib import Path

import pytest

from lab.app import create_app
from lab.errors import WithdrawnError
from lab.parsing import parse_calibrations
from lab.storage import Store

SAMPLE_CSV = (
    "样本号,UTC时刻,吸光度\n"
    "S1,2024-01-01T00:00:00Z,1.5\n"
    "S2,2024-01-02T00:00:00Z,2\n"
).encode()
CAL_CSV = (
    "起始UTC时刻,结束UTC时刻,斜率,截距\n"
    "2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,2,0.5\n"
    "2024-01-02T00:00:00Z,2024-01-03T00:00:00Z,1,1\n"
).encode()
OTHER_CAL_CSV = (
    "起始UTC时刻,结束UTC时刻,斜率,截距\n"
    "2024-01-01T00:00:00Z,2024-01-03T00:00:00Z,10,0\n"
).encode()


def build_store(tmp_path) -> Store:
    return Store(tmp_path / "db" / "lab.sqlite3", tmp_path / "results")


def publish(store: Store, revision="1", cal=CAL_CSV):
    from lab.parsing import parse_samples

    return store.publish_first(
        parse_samples(SAMPLE_CSV), parse_calibrations(cal), int(revision), "t"
    )


def listing(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def pending_paths(tmp_path):
    db = sqlite3.connect(tmp_path / "db" / "lab.sqlite3")
    try:
        return sorted(row[0] for row in db.execute("SELECT file_path FROM withdrawn_files"))
    finally:
        db.close()


def table_counts(tmp_path, token):
    db = sqlite3.connect(tmp_path / "db" / "lab.sqlite3")
    try:
        batch_id = db.execute("SELECT id FROM batches WHERE token = ?", (token,)).fetchone()[0]
        counts = {}
        for table in ("samples", "versions", "withdrawn_files"):
            counts[table] = db.execute(
                f"SELECT COUNT(*) FROM {table} WHERE batch_id = ?", (batch_id,)
            ).fetchone()[0]
        counts["results"] = db.execute(
            "SELECT COUNT(*) FROM results WHERE version_id IN "
            "(SELECT id FROM versions WHERE batch_id = ?)",
            (batch_id,),
        ).fetchone()[0]
        withdrawn_at = db.execute(
            "SELECT withdrawn_at FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()[0]
        return counts, withdrawn_at
    finally:
        db.close()


# ---------- 存储层：登记、回收、清除 ----------


def test_withdrawal_registers_paths_then_reclaims_files_and_purges(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)
    results = tmp_path / "results"
    assert listing(results) == [
        ".lab.lock",
        f"{batch.token}/revision-1.csv",
        f"{batch.token}/revision-2.csv",
    ]

    # 第一步：登记撤回（模拟“撤回标记”与“文件回收”之间的中断点）。
    _, paths, already = store.begin_withdrawal(batch.token)
    assert already is False
    expected = [f"{batch.token}/revision-1.csv", f"{batch.token}/revision-2.csv"]
    assert paths == expected
    # SQLite 留存的待删路径与数据卷实际文件逐份对应。
    assert pending_paths(tmp_path) == expected
    assert all((results / rel).is_file() for rel in expected)
    # 登记即刻阻断，即使文件尚未删除。
    with pytest.raises(WithdrawnError):
        store.download(batch.token)
    with pytest.raises(WithdrawnError):
        store.get_version(batch.token)

    # 第二步：回收——逐份删除实际文件并确认后移除路径，最终清除 SQLite 记录。
    summary = store.resume_withdrawals()
    assert summary["failed"] == []
    assert sorted(summary["deleted"]) == expected
    assert summary["purged"] == [batch.token]
    assert listing(results) == [".lab.lock"]
    counts, withdrawn_at = table_counts(tmp_path, batch.token)
    assert counts == {"samples": 0, "versions": 0, "results": 0, "withdrawn_files": 0}
    # 撤回状态持久登记：批次行保留 withdrawn_at。
    assert withdrawn_at is not None
    info = store.get_batch(batch.token)
    assert info is not None and info.withdrawn_at is not None


def test_restart_resumes_reclamation_from_retained_paths(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)
    store.begin_withdrawal(batch.token)
    # 模拟容器在“撤回标记”与“文件回收”之间崩溃：文件仍在，路径已留存。
    results = tmp_path / "results"
    assert len(listing(results)) == 3

    reopened = build_store(tmp_path)  # 重启：按留存路径继续回收
    assert listing(results) == [".lab.lock"]
    assert pending_paths(tmp_path) == []
    counts, withdrawn_at = table_counts(tmp_path, batch.token)
    assert counts["versions"] == 0 and counts["samples"] == 0 and counts["results"] == 0
    assert withdrawn_at is not None
    with pytest.raises(WithdrawnError):
        reopened.download(batch.token)


def test_failed_file_deletion_keeps_path_and_blocks_until_retried(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)
    store.begin_withdrawal(batch.token)

    # 模拟 revision-2 文件删除失败（如权限/占用）。
    real_reclaim = store._reclaim_path
    failing = f"{batch.token}/revision-2.csv"
    store._reclaim_path = lambda rel: False if rel == failing else real_reclaim(rel)
    summary = store.resume_withdrawals()
    assert summary["failed"] == [failing]
    assert summary["deleted"] == [f"{batch.token}/revision-1.csv"]
    assert summary["purged"] == []
    # 失败路径留存，成功路径已确认删除后移除。
    assert pending_paths(tmp_path) == [failing]
    assert (tmp_path / "results" / failing).is_file()
    assert not (tmp_path / "results" / f"{batch.token}/revision-1.csv").exists()
    # 版本/样本记录尚未清除，期间该批始终不可读。
    counts, _ = table_counts(tmp_path, batch.token)
    assert counts["versions"] == 2 and counts["samples"] == 2
    with pytest.raises(WithdrawnError):
        store.download(batch.token, 1)

    # 故障恢复后（或重启）按留存路径继续回收，最终完成清除。
    store._reclaim_path = real_reclaim
    summary = store.resume_withdrawals()
    assert summary["deleted"] == [failing]
    assert summary["purged"] == [batch.token]
    assert listing(tmp_path / "results") == [".lab.lock"]
    counts, _ = table_counts(tmp_path, batch.token)
    assert counts == {"samples": 0, "versions": 0, "results": 0, "withdrawn_files": 0}


def test_missing_file_counts_as_confirmed_deleted(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    store.begin_withdrawal(batch.token)
    # 崩溃前已删文件但未来得及移除登记路径：重启后视为已确认删除。
    (tmp_path / "results" / batch.token / "revision-1.csv").unlink()
    summary = store.resume_withdrawals()
    assert summary["failed"] == []
    assert pending_paths(tmp_path) == []
    assert summary["purged"] == [batch.token]


def test_withdrawal_is_idempotent(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    _, paths, already = store.begin_withdrawal(batch.token)
    assert already is False and len(paths) == 1
    _, paths2, already2 = store.begin_withdrawal(batch.token)
    assert already2 is True and paths2 == []
    store.resume_withdrawals()
    assert listing(tmp_path / "results") == [".lab.lock"]


def test_other_batches_unaffected_by_withdrawal(tmp_path):
    store = build_store(tmp_path)
    batch_a, _ = publish(store)
    batch_b, _ = publish(store)
    store.begin_withdrawal(batch_a.token)
    store.resume_withdrawals()
    # 其他批次仍可下载、更正。
    assert store.download(batch_b.token) is not None
    _, version, _ = store.publish_correction(
        batch_b.token, parse_calibrations(OTHER_CAL_CSV), 2
    )
    assert version.revision == 2
    assert store.download(batch_b.token, 2) is not None
    with pytest.raises(WithdrawnError):
        store.download(batch_a.token)


def test_correction_after_withdrawal_never_republishes(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    store.begin_withdrawal(batch.token)
    store.resume_withdrawals()
    with pytest.raises(WithdrawnError):
        store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)
    assert listing(tmp_path / "results") == [".lab.lock"]
    counts, _ = table_counts(tmp_path, batch.token)
    assert counts["versions"] == 0


def test_correction_racing_withdrawal_is_rejected_inside_transaction(tmp_path, monkeypatch):
    """更正预检通过后撤回才生效：写事务内必须再次判定，不得重新发布。"""
    store = build_store(tmp_path)
    batch, _ = publish(store)
    stale = store.get_samples(batch.token)  # 撤回前读到的样本
    store.begin_withdrawal(batch.token)
    monkeypatch.setattr(store, "get_samples", lambda token: stale)
    with pytest.raises(WithdrawnError):
        store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)
    store.resume_withdrawals()
    assert listing(tmp_path / "results") == [".lab.lock"]
    counts, _ = table_counts(tmp_path, batch.token)
    assert counts["versions"] == 0


def test_download_interleaving(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    # 撤回前开始的下载取得完整原文件。
    _, _, before = store.download(batch.token)
    assert before.startswith("样本号".encode())
    store.begin_withdrawal(batch.token)
    # 撤回登记后、文件尚未回收：新下载即被拒绝。
    assert (tmp_path / "results" / batch.token / "revision-1.csv").is_file()
    with pytest.raises(WithdrawnError):
        store.download(batch.token)
    store.resume_withdrawals()
    with pytest.raises(WithdrawnError):
        store.download(batch.token, 1)


def test_cleanup_unpublished_never_touches_pending_withdrawal_files(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    store.begin_withdrawal(batch.token)
    # 待回收文件仍登记在 versions 中，清理流程不得越权删除。
    assert store.cleanup_unpublished() == []
    assert (tmp_path / "results" / batch.token / "revision-1.csv").is_file()
    store.resume_withdrawals()
    assert listing(tmp_path / "results") == [".lab.lock"]


def test_withdrawal_of_unknown_batch_raises(tmp_path):
    store = build_store(tmp_path)
    from lab.errors import NotFoundError

    with pytest.raises(NotFoundError):
        store.begin_withdrawal("deadbeef")


# ---------- HTTP 层：浏览器入口与阻断 ----------


@pytest.fixture
def app(tmp_path):
    return create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "db" / "lab.sqlite3"),
            "RESULTS_DIR": str(tmp_path / "results"),
        }
    )


@pytest.fixture
def client(app):
    return app.test_client()


def upload(client, samples=SAMPLE_CSV, cal=CAL_CSV, revision="1", label="批次"):
    data = {
        "revision": revision,
        "label": label,
        "samples_csv": (io.BytesIO(samples), "s.csv"),
        "calibration_csv": (io.BytesIO(cal), "c.csv"),
    }
    response = client.post("/batches", data=data, content_type="multipart/form-data")
    assert response.status_code == 302
    return response.headers["Location"]  # /batches/<token>


def test_http_withdraw_flow_blocks_everything(client, tmp_path):
    location = upload(client)
    token = location.rsplit("/", 1)[-1]
    client.post(
        location + "/corrections",
        data={"revision": "2", "calibration_csv": (io.BytesIO(OTHER_CAL_CSV), "c.csv")},
        content_type="multipart/form-data",
    )
    assert (tmp_path / "results" / token / "revision-2.csv").is_file()

    # 确认页：列出将逐份删除的文件。
    page = client.get(location + "/withdraw")
    assert page.status_code == 200
    text = page.get_data(as_text=True)
    assert "确认撤回" in text and "revision-1.csv" in text and "revision-2.csv" in text

    # 确认标识不匹配：不执行，批次仍可读。
    bad = client.post(location + "/withdraw", data={"confirm_token": "wrong"})
    assert bad.status_code == 400
    assert client.get(location).status_code == 200

    # 按批次确认后撤回。
    ok = client.post(location + "/withdraw", data={"confirm_token": token})
    assert ok.status_code == 302

    # 当前与历史详情、下载一律 410。
    assert client.get(location).status_code == 410
    assert client.get(location + "/revisions/1").status_code == 410
    assert client.get(location + "/revisions/2").status_code == 410
    assert client.get(location + "/download").status_code == 410
    assert client.get(location + "/download/1").status_code == 410
    assert client.get(location + "/download/2").status_code == 410
    # 更正被阻断，不会重新发布。
    blocked = client.post(
        location + "/corrections",
        data={"revision": "3", "calibration_csv": (io.BytesIO(OTHER_CAL_CSV), "c.csv")},
        content_type="multipart/form-data",
    )
    assert blocked.status_code == 410

    # 数据卷文件已逐份删除，SQLite 记录已清除，撤回状态持久可见。
    assert listing(tmp_path / "results") == [".lab.lock"]
    counts, withdrawn_at = table_counts(tmp_path, token)
    assert counts == {"samples": 0, "versions": 0, "results": 0, "withdrawn_files": 0}
    assert withdrawn_at is not None
    index = client.get("/").get_data(as_text=True)
    assert "已撤回" in index and token in index

    # 重复撤回幂等：确认页与再次提交都安全。
    assert client.get(location + "/withdraw").status_code == 302
    again = client.post(location + "/withdraw", data={"confirm_token": token})
    assert again.status_code == 302
    assert client.get(location).status_code == 410


def test_http_withdraw_keeps_other_batches_usable(client):
    first = upload(client, label="甲")
    second = upload(client, label="乙")
    token_a = first.rsplit("/", 1)[-1]
    assert client.post(first + "/withdraw", data={"confirm_token": token_a}).status_code == 302
    # 其他批次照常：详情、下载、更正、新上传。
    assert client.get(second).status_code == 200
    assert client.get(second + "/download").status_code == 200
    response = client.post(
        second + "/corrections",
        data={"revision": "2", "calibration_csv": (io.BytesIO(OTHER_CAL_CSV), "c.csv")},
        content_type="multipart/form-data",
    )
    assert response.status_code == 302
    assert upload(client, label="丙")


def test_http_withdraw_unknown_batch_404(client):
    assert client.get("/batches/deadbeef/withdraw").status_code == 404
    response = client.post("/batches/deadbeef/withdraw", data={"confirm_token": "deadbeef"})
    assert response.status_code == 404
