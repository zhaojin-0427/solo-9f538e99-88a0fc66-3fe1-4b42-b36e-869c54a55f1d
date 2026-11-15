FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY lab ./lab
COPY templates ./templates
COPY static ./static

RUN mkdir -p /app/data && chown -R 1000:1000 /app/data

USER 1000:1000

ENV LAB_DATABASE=/app/data/lab.sqlite3 \
    LAB_RESULTS_DIR=/app/data/results \
    PORT=8000 \
    WEB_CONCURRENCY=2

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status == 200 else 1)"

# 多 worker 下，发布依赖 BEGIN IMMEDIATE + 结果目录文件锁串行化，跨进程安全。
CMD ["sh", "-c", "exec gunicorn --workers ${WEB_CONCURRENCY} --bind 0.0.0.0:${PORT} --access-logfile - --error-logfile - lab.wsgi:app"]
