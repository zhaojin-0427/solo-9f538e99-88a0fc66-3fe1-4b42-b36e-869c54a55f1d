import io

import pytest

from lab.app import create_app


def sample_csv(*rows, header="样本号,UTC时刻,吸光度"):
    lines = [header] + [",".join(map(str, row)) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def cal_csv(*rows, header="起始UTC时刻,结束UTC时刻,斜率,截距"):
    lines = [header] + [",".join(map(str, row)) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


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


@pytest.fixture
def post(client):
    def _post(path, **fields):
        data = {
            key: (io.BytesIO(value), f"{key}.csv") if isinstance(value, bytes) else value
            for key, value in fields.items()
        }
        return client.post(path, data=data, content_type="multipart/form-data", follow_redirects=False)

    return _post


BASE_SAMPLES = sample_csv(
    ("S1", "2024-01-01T00:00:00Z", "1.5"),
    ("S2", "2024-01-01T12:00:00Z", "2"),
    ("S3", "2024-01-02T00:00:00Z", "0.5"),
)
BASE_CAL = cal_csv(
    ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2", "0.5"),
    ("2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z", "1", "1"),
)


def create(client, post, samples=BASE_SAMPLES, cal=BASE_CAL, revision="1", label="批次"):
    response = post(
        "/batches",
        revision=revision,
        label=label,
        samples_csv=samples,
        calibration_csv=cal,
    )
    assert response.status_code == 302, response.get_data(as_text=True)
    return response.headers["Location"]


def download(client, location, revision=None):
    path = location + "/download" + (f"/{revision}" if revision else "")
    response = client.get(path)
    assert response.status_code == 200
    return response.get_data(as_text=True)


def test_first_upload_computes_and_publishes(client, post):
    location = create(client, post)
    body = download(client, location, 1)
    lines = body.strip().splitlines()
    assert lines[0] == "样本号,UTC时刻,吸光度,斜率,截距,浓度"
    assert lines[1:] == [
        "S1,2024-01-01T00:00:00Z,1.5,2,0.5,3.5",
        "S2,2024-01-01T12:00:00Z,2,2,0.5,4.5",
        "S3,2024-01-02T00:00:00Z,0.5,1,1,1.5",
    ]
    page = client.get(location).get_data(as_text=True)
    assert "3" in page


def test_start_inclusive_end_exclusive(client, post):
    samples = sample_csv(
        ("START", "2024-01-01T00:00:00Z", "0"),
        ("END", "2024-01-02T00:00:00Z", "0"),
    )
    location = create(client, post, samples=samples)
    body = download(client, location)
    rows = {line.split(",")[0]: line.split(",")[-1] for line in body.strip().splitlines()[1:]}
    # 00:00 起点属于第一段（截距 0.5），结束时刻属于第二段（截距 1）。
    assert rows["START"] == "0.5"
    assert rows["END"] == "1"


def test_negative_concentration_rejected_entire_batch(client, post):
    samples = sample_csv(("OK", "2024-01-01T00:00:00Z", "5"))
    response = post(
        "/batches",
        revision="1",
        label="",
        samples_csv=samples,
        calibration_csv=cal_csv(("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "1", "-10")),
    )
    assert response.status_code == 400
    text = response.get_data(as_text=True)
    assert "浓度为负" in text
    assert "尚无批次" in client.get("/").get_data(as_text=True)


def test_duplicate_sample_id_rejected(client, post):
    samples = sample_csv(
        ("S1", "2024-01-01T00:00:00Z", "1"),
        ("S1", "2024-01-01T01:00:00Z", "2"),
    )
    response = post("/batches", revision="1", label="", samples_csv=samples, calibration_csv=BASE_CAL)
    assert response.status_code == 400
    assert "重复" in response.get_data(as_text=True)


def test_timestamp_without_calibration_rejected(client, post):
    samples = sample_csv(("S1", "2023-12-31T23:59:59Z", "1"))
    response = post("/batches", revision="1", label="", samples_csv=samples, calibration_csv=BASE_CAL)
    assert response.status_code == 400
    assert "未命中任何校准区间" in response.get_data(as_text=True)


def test_overlapping_calibration_rejected(client, post):
    cal = cal_csv(
        ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "1", "0"),
        ("2024-01-01T12:00:00Z", "2024-01-03T00:00:00Z", "1", "0"),
    )
    response = post("/batches", revision="1", label="", samples_csv=BASE_SAMPLES, calibration_csv=cal)
    assert response.status_code == 400
    assert "重叠" in response.get_data(as_text=True)


def test_adjacent_calibrations_do_not_overlap(client, post):
    cal = cal_csv(
        ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "1", "0"),
        ("2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z", "1", "0"),
    )
    create(client, post, cal=cal)


@pytest.mark.parametrize("bad", ["nan", "inf", "-inf", "1e999999999", "abc", "", "1,2"])
def test_non_finite_numbers_rejected(client, post, bad):
    samples = sample_csv(("S1", "2024-01-01T00:00:00Z", "1"))
    cal = cal_csv(("2024-01-01T00:00:00Z", "2024-01-03T00:00:00Z", bad, "0"))
    response = post("/batches", revision="1", label="", samples_csv=samples, calibration_csv=cal)
    assert response.status_code in (400, 500)
    if bad == "1,2":
        assert response.status_code == 400
        assert "字段" in response.get_data(as_text=True)
    else:
        assert response.status_code == 400


@pytest.mark.parametrize("bad", ["0", "-1", "abc", "1.5", ""])
def test_revision_must_be_positive_integer(client, post, bad):
    response = post("/batches", revision=bad, label="", samples_csv=BASE_SAMPLES, calibration_csv=BASE_CAL)
    assert response.status_code == 400
    assert "正整数" in response.get_data(as_text=True)


def test_row_limit_500(client, post):
    rows = [(f"S{i}", "2024-01-01T00:00:00Z", "1") for i in range(500)]
    create(client, post, samples=sample_csv(*rows))
    rows = rows + [("S500", "2024-01-01T00:00:00Z", "1")]
    response = post("/batches", revision="1", label="", samples_csv=sample_csv(*rows), calibration_csv=BASE_CAL)
    assert response.status_code == 400
    assert "500" in response.get_data(as_text=True)


def test_correction_recomputes_from_stored_samples(client, post):
    location = create(client, post)
    cal = cal_csv(("2024-01-01T00:00:00Z", "2024-01-03T00:00:00Z", "10", "0"))
    response = post(location + "/corrections", revision="2", calibration_csv=cal)
    assert response.status_code == 302
    body = download(client, location, 2)
    assert "S1,2024-01-01T00:00:00Z,1.5,10,0,15" in body
    # 旧版本仍可查看与下载。
    assert client.get(location + "/revisions/1").status_code == 200
    assert download(client, location, 1) != body
    assert download(client, location) == body


def test_lower_revision_conflict(client, post):
    location = create(client, post, revision="5")
    cal = cal_csv(("2024-01-01T00:00:00Z", "2024-01-03T00:00:00Z", "10", "0"))
    for revision in ("4", "1"):
        response = post(location + "/corrections", revision=revision, calibration_csv=cal)
        assert response.status_code == 409
    assert download(client, location) != ""


def test_same_revision_different_content_conflict(client, post):
    location = create(client, post, revision="5")
    cal = cal_csv(("2024-01-01T00:00:00Z", "2024-01-03T00:00:00Z", "10", "0"))
    response = post(location + "/corrections", revision="5", calibration_csv=cal)
    assert response.status_code == 409
    assert "内容不同" in response.get_data(as_text=True)


def test_same_revision_same_content_is_idempotent(client, app, post):
    location = create(client, post, revision="5")
    reversed_cal = cal_csv(
        ("2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z", "1", "1"),
        ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2", "0.5"),
    )
    response = post(location + "/corrections", revision="5", calibration_csv=reversed_cal)
    assert response.status_code == 302
    store = app.extensions["store"]
    versions = store.list_versions(location.rsplit("/", 1)[-1])
    assert len(versions) == 1


def test_old_version_remains_available_after_correction(client, post):
    location = create(client, post)
    old = download(client, location, 1)
    post(
        location + "/corrections",
        revision="2",
        calibration_csv=cal_csv(("2024-01-01T00:00:00Z", "2024-01-03T00:00:00Z", "2", "2")),
    )
    assert download(client, location, 1) == old
    assert download(client, location, 2) != old


def test_missing_revision_returns_404(client, post):
    location = create(client, post)
    assert client.get(location + "/download/99").status_code == 404
    assert client.get(location + "/revisions/99").status_code == 404


def test_unknown_batch_returns_404(client):
    assert client.get("/batches/deadbeef").status_code == 404
    assert client.get("/batches/deadbeef/download").status_code == 404
