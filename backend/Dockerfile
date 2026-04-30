FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg + pandas/numpy
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY migrations ./migrations
COPY scripts ./scripts

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# FastAPI default port
EXPOSE 8000

# Start command set per-service in Railway dashboard
