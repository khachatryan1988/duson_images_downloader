# syntax=docker/dockerfile:1
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости и ставим их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходники
COPY src ./src

# Точка входа
CMD ["python", "app/src/download_images.py"]
