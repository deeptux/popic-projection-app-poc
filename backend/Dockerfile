# Cloud Run: listen on 0.0.0.0:$PORT (see CMD). Local: docker run -e PORT=8080 -e CORS_ALLOWED_ORIGINS=...
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY analytics ./analytics
COPY ingestion ./ingestion
COPY main.py .

EXPOSE 8080

CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
