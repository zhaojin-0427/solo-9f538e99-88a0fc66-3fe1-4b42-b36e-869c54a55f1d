"""Flask 网页入口：上传、校验、发布与查询下载。"""

from __future__ import annotations

import io
import os
from pathlib import Path

from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.exceptions import RequestEntityTooLarge

from .errors import ConflictError, InputError, LabError, NotFoundError, PublishError
from .parsing import (
    MAX_DATA_ROWS,
    parse_calibrations,
    parse_positive_revision,
    parse_samples,
)
from .storage import Store, calibrations_from_json

MAX_UPLOAD_BYTES = 5 * 1024 * 1024
PREVIEW_ROWS = 100


def create_app(config: dict | None = None) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.config.update(
        SECRET_KEY=os.environ.get("LAB_SECRET_KEY", "lab-dev-secret-change-me"),
        MAX_CONTENT_LENGTH=MAX_UPLOAD_BYTES,
        DATABASE=os.environ.get("LAB_DATABASE", str(Path(app.root_path).parent / "data" / "lab.sqlite3")),
        RESULTS_DIR=os.environ.get("LAB_RESULTS_DIR", str(Path(app.root_path).parent / "data" / "results")),
        MAX_DATA_ROWS=MAX_DATA_ROWS,
        PREVIEW_ROWS=PREVIEW_ROWS,
    )
    if config:
        app.config.update(config)

    Path(app.config["DATABASE"]).parent.mkdir(parents=True, exist_ok=True)
    store = Store(app.config["DATABASE"], app.config["RESULTS_DIR"])
    app.extensions["store"] = store

    @app.context_processor
    def inject_globals():
        return {"max_rows": app.config["MAX_DATA_ROWS"], "preview_rows": app.config["PREVIEW_ROWS"]}

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/")
    def index():
        return render_template("index.html", batches=store.list_batches())

    @app.post("/batches")
    def create_batch():
        label = (request.form.get("label") or "").strip()[:120]
        revision_text = request.form.get("revision")
        revision = None
        samples_file = request.files.get("samples_csv")
        calibration_file = request.files.get("calibration_csv")
        errors: list[str] = []

        try:
            revision = parse_positive_revision(revision_text)
        except InputError as exc:
            errors.extend(exc.messages)
        if samples_file is None or not samples_file.filename:
            errors.append("请上传原始样本 CSV")
        if calibration_file is None or not calibration_file.filename:
            errors.append("请上传校准 CSV")
        if errors:
            return render_template(
                "index.html",
                batches=store.list_batches(),
                errors=errors,
                form={"label": label, "revision": revision_text or ""},
            ), 400

        try:
            samples = parse_samples(samples_file.read())
            calibrations = parse_calibrations(calibration_file.read())
            batch, version = store.publish_first(samples, calibrations, revision, label)
        except InputError as exc:
            return render_template(
                "index.html",
                batches=store.list_batches(),
                errors=exc.messages,
                form={"label": label, "revision": revision_text or ""},
            ), 400
        except PublishError as exc:
            return render_template(
                "index.html",
                batches=store.list_batches(),
                errors=[str(exc)],
                form={"label": label, "revision": revision_text or ""},
            ), 500

        flash(f"批次已发布：修订 {version.revision}，共 {version.result_rows} 行结果。", "success")
        return redirect(url_for("batch_detail", token=batch.token))

    @app.post("/batches/<token>/corrections")
    def correct_batch(token: str):
        revision_text = request.form.get("revision")
        calibration_file = request.files.get("calibration_csv")
        errors: list[str] = []
        revision = None
        try:
            revision = parse_positive_revision(revision_text)
        except InputError as exc:
            errors.extend(exc.messages)
        if calibration_file is None or not calibration_file.filename:
            errors.append("请上传完整的校准 CSV")
        if errors:
            return _render_detail(token, errors=errors, form={"revision": revision_text or ""}, status=400)

        try:
            calibrations = parse_calibrations(calibration_file.read())
            batch, version, idempotent = store.publish_correction(token, calibrations, revision)
        except InputError as exc:
            return _render_detail(token, errors=exc.messages, form={"revision": revision_text or ""}, status=400)
        except ConflictError as exc:
            return _render_detail(
                token,
                errors=[str(exc)],
                form={"revision": revision_text or ""},
                status=409,
            )
        except PublishError as exc:
            return _render_detail(token, errors=[str(exc)], form={"revision": revision_text or ""}, status=500)
        except NotFoundError as exc:
            return _render_error(str(exc), 404)

        if idempotent:
            flash(f"修订 {version.revision} 与已发布内容一致，未产生重复发布。", "success")
        else:
            flash(f"已按修订 {version.revision} 重算并发布，共 {version.result_rows} 行结果。", "success")
        return redirect(url_for("batch_detail", token=batch.token))

    @app.get("/batches/<token>")
    def batch_detail(token: str):
        return _render_detail(token)

    @app.get("/batches/<token>/revisions/<int:revision>")
    def batch_revision(token: str, revision: int):
        return _render_detail(token, revision=revision)

    @app.get("/batches/<token>/download")
    def download_current(token: str):
        return _download(token, None)

    @app.get("/batches/<token>/download/<int:revision>")
    def download_revision(token: str, revision: int):
        return _download(token, revision)

    @app.post("/maintenance/cleanup")
    def maintenance_cleanup():
        removed = store.cleanup_unpublished()
        flash(f"已清理 {len(removed)} 个未发布文件。", "success")
        return redirect(url_for("index"))

    def _render_detail(token: str, revision: int | None = None, errors=None, form=None, status: int = 200):
        found = store.get_version(token, revision)
        if found is None:
            return _render_error("批次或修订不存在", 404)
        batch, version = found
        versions = store.list_versions(token) or []
        current_version = store.get_version(token, None)
        rows = store.result_rows(token, revision) or []
        calibrations = calibrations_from_json(version.calibration_json) if version.calibration_json else []
        return (
            render_template(
                "batch.html",
                batch=batch,
                version=version,
                versions=versions,
                rows=rows,
                calibrations=calibrations,
                is_current=current_version is not None and current_version[1].id == version.id,
                errors=errors or [],
                form=form or {"revision": ""},
            ),
            status,
        )

    def _download(token: str, revision: int | None):
        try:
            found = store.download(token, revision)
        except PublishError as exc:
            return _render_error(str(exc), 500)
        if found is None:
            return _render_error("批次或修订不存在", 404)
        batch, version, content = found
        name = f"batch-{batch.token}-rev-{version.revision}.csv"
        return send_file(
            io.BytesIO(content),
            mimetype="text/csv; charset=utf-8",
            as_attachment=True,
            download_name=name,
            max_age=0,
        )

    def _render_error(message: str, status: int):
        return render_template("error.html", message=message), status

    @app.errorhandler(413)
    def too_large(_error):
        return _render_error(f"上传内容超过 {MAX_UPLOAD_BYTES // (1024 * 1024)} MB 上限", 413)

    @app.errorhandler(RequestEntityTooLarge)
    def too_large_request(_error):
        return _render_error(f"上传内容超过 {MAX_UPLOAD_BYTES // (1024 * 1024)} MB 上限", 413)

    @app.errorhandler(LabError)
    def lab_error(error):
        return _render_error(str(error), 400)

    return app


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))


if __name__ == "__main__":
    main()
