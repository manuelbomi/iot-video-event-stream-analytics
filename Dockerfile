FROM python:3.11-slim

WORKDIR /app

# Install dependencies first so this layer is cached across code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Default command; overridden per-service in docker-compose.yml.
CMD ["python", "-m", "src.processor.stream_processor"]
