# Dockerfile cho JDL Warehouse Sync & Zalo Bot Service 24/7
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Khởi chạy WMS Drive Sync kèm Zalo Bot nền 24/7
CMD ["python", "-u", "sync_wms_local.py", "--daemon", "--interval", "60"]
