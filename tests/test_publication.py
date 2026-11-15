"""发布原子性、失败回滚、未发布文件清理与并发更正测试。"""

import io
import threading
from pathlib import Path

import pytest

from lab.app import create_app
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


def upload(client, samples=SAMPLE_CSV, cal=CAL_CSV, revision="1"):
    data = {
        "revision": revision,
        "label": "t",
        "samples_csv": (io.BytesIO(samples), "s.csv"),
        "calibration_csv": (io.BytesIO(cal), "c.csv"),
    }
    return client.post("/batches", data=data, content_type="multipart/form-data")


def publish(store: Store, revision="1", cal=CAL_CSV):
    from lab.parsing import parse_calibrations, parse_samples

    return store.publish_first(
        parse_samples(SAMPLE_CSV), parse_calibrations(cal), int(revision), "t"
    )


def listing(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def test_temp_file_failure_leaves_no_record_or_file(tmp_path, monkeypatch):
    store = build_store(tmp_path)

    def broken_write(content):
        raise OSError("disk full")

    monkeypatch.setattr(store, "_write_temp", broken_write)
    with pytest.raises(OSError):
        publish(store)
    assert store.list_batches() == []
    assert listing(tmp_path / "results") == [".lab.lock"]


def test_commit_failure_leaves_no_record_or_file(tmp_path, monkeypatch):
    store = build_store(tmp_path)
    monkeypatch.setattr(store, "_commit_file", lambda a, b: (_ for _ in ()).throw(OSError("rename failed")))
    with pytest.raises(OSError):
        publish(store)
    assert store.list_batches() == []
    assert listing(tmp_path / "results") == [".lab.lock"]


def test_registration_failure_removes_published_file(tmp_path, monkeypatch):
    store = build_store(tmp_path)
    monkeypatch.setattr(store, "_commit_file", lambda a, b: (_ for _ in ()).throw(OSError("rename failed")))
    with pytest.raises(OSError):
        publish(store)
    assert store.list_batches() == []
    assert listing(tmp_path / "results") == [".lab.lock"]


def test_correction_failure_keeps_old_version(tmp_path, monkeypatch):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    _, _, old_body = store.download(batch.token)

    monkeypatch.setattr(store, "_commit_file", lambda a, b: (_ for _ in ()).throw(OSError("boom")))
    from lab.parsing import parse_calibrations

    with pytest.raises(OSError):
        store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)

    current = store.get_batch(batch.token)
    assert current.current_revision == 1
    found = store.download(batch.token)
    assert found is not None and found[2] == old_body
    assert listing(tmp_path / "results") == [".lab.lock", f"{batch.token}/revision-1.csv"]


def test_cleanup_removes_orphans_but_keeps_published(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    results = tmp_path / "results"
    orphan = results / "abandoned" / "revision-9.csv"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"junk")
    temp = results / ".publish-dead.tmp"
    temp.write_bytes(b"junk")
    assert listing(results) == sorted(
        [
            ".lab.lock",
            ".publish-dead.tmp",
            f"{batch.token}/revision-1.csv",
            "abandoned/revision-9.csv",
        ]
    )

    removed = store.cleanup_unpublished()
    assert len(removed) == 2
    assert listing(results) == [".lab.lock", f"{batch.token}/revision-1.csv"]
    # 清理后已发布版本依旧可下载。
    assert store.download(batch.token) is not None


def test_cleanup_on_startup(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    (tmp_path / "results" / "stray.csv").write_bytes(b"x")
    reopened = Store(tmp_path / "db" / "lab.sqlite3", tmp_path / "results")
    assert listing(tmp_path / "results") == [".lab.lock", f"{batch.token}/revision-1.csv"]
    assert reopened.download(batch.token) is not None


def test_file_record_mismatch_is_detected(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    stored = tmp_path / "results" / batch.token / "revision-1.csv"
    stored.write_bytes(stored.read_bytes().replace(b"3.5", b"9.9"))
    from lab.errors import PublishError

    with pytest.raises(PublishError):
        store.download(batch.token)


def test_missing_file_is_detected(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    (tmp_path / "results" / batch.token / "revision-1.csv").unlink()
    from lab.errors import PublishError

    with pytest.raises(PublishError):
        store.download(batch.token)


def test_concurrent_corrections_use_current_revision_at_commit(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    from lab.errors import ConflictError
    from lab.parsing import parse_calibrations

    outcomes = []
    barrier = threading.Barrier(6)

    def worker(revision):
        barrier.wait()
        try:
            store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), revision)
            outcomes.append(("ok", revision))
        except ConflictError:
            outcomes.append(("conflict", revision))

    threads = [threading.Thread(target=worker, args=(revision,)) for revision in range(2, 8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    accepted = sorted(revision for status, revision in outcomes if status == "ok")
    conflicts = sorted(revision for status, revision in outcomes if status == "conflict")
    # 每个高修订号只被接受一次，其余被判冲突。
    assert sorted(accepted + conflicts) == list(range(2, 8))
    assert len(accepted) == len(set(accepted))
    current = store.get_batch(batch.token)
    assert current.current_revision == max(accepted)
    versions = store.list_versions(batch.token)
    assert {item.revision for item in versions} == {1, *accepted}
    for version in versions:
        assert store.download(batch.token, version.revision) is not None


def test_concurrent_same_revision_same_content_single_publish(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    from lab.parsing import parse_calibrations

    barrier = threading.Barrier(5)
    errors = []

    def worker():
        barrier.wait()
        try:
            store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)
        except Exception as exc:  # noqa: BLE001 - 测试中记录所有异常
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    versions = store.list_versions(batch.token)
    assert [item.revision for item in versions] == [2, 1]
    assert listing(tmp_path / "results") == [
        ".lab.lock",
        f"{batch.token}/revision-1.csv",
        f"{batch.token}/revision-2.csv",
    ]


def test_http_flow_rejects_bad_batch_and_keeps_site_usable(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "DATABASE": str(tmp_path / "db" / "lab.sqlite3"),
            "RESULTS_DIR": str(tmp_path / "results"),
        }
    )
    client = app.test_client()
    bad_cal = (
        "起始UTC时刻,结束UTC时刻,斜率,截距\n"
        "2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,1,0\n"
        "2024-01-01T12:00:00Z,2024-01-03T00:00:00Z,1,0\n"
    ).encode()
    response = upload(client, cal=bad_cal)
    assert response.status_code == 400
    assert client.get("/").status_code == 200
    assert "尚无批次" in client.get("/").get_data(as_text=True)
    assert listing(tmp_path / "results") == [".lab.lock"]


def test_idempotent_replay_writes_no_extra_file(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    from lab.parsing import parse_calibrations

    _, version, idempotent = store.publish_correction(
        batch.token, parse_calibrations(OTHER_CAL_CSV), 2
    )
    assert idempotent is False
    before = listing(tmp_path / "results")
    for _ in range(3):
        _, replay, again = store.publish_correction(
            batch.token, parse_calibrations(OTHER_CAL_CSV), 2
        )
        assert again is True
        assert replay.id == version.id
        assert listing(tmp_path / "results") == before


def test_cleanup_never_removes_published_files(tmp_path):
    store = build_store(tmp_path)
    batch, _ = publish(store)
    from lab.parsing import parse_calibrations

    store.publish_correction(batch.token, parse_calibrations(OTHER_CAL_CSV), 2)
    before = listing(tmp_path / "results")
    assert store.cleanup_unpublished() == []
    assert listing(tmp_path / "results") == before
    for revision in (1, 2):
        assert store.download(batch.token, revision) is not None
