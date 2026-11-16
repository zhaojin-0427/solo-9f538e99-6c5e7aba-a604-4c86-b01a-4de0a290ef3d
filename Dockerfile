FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    DB_PATH=/instance/fermenter.db \
    PORT=8000

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY run.py ./

RUN mkdir -p /data /instance

EXPOSE 8000

# 容器内用 Flask 内置服务器即可满足演示/质控场景；生产可换 gunicorn。
# 注意：不要加 -I。隔离模式会把脚本所在目录（此处 /app）从 sys.path 移除，
# 导致 `from app import create_app` 失败。run.py 只在容器内执行可信代码。
CMD ["python", "run.py"]
