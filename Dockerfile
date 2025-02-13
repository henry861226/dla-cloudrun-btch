# 使用官方 Python 基礎映像
FROM python:3.9-slim

# env variable
ENV LANG=C.UTF-8
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir --default-timeout=0 -r requirements.txt

COPY . .

ENTRYPOINT ["sh", "/app/start.sh"]
