FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    celery==5.3.4 \
    redis==5.0.1

COPY worker.py .

CMD ["celery", "-A", "worker", "worker", "--loglevel=info", "--concurrency=1"]
