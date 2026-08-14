FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static
COPY tools ./tools

EXPOSE 8000

# One worker on purpose. The sender is designed as the single owner of outbound
# calls so the 10-per-60s limit cannot be raced; two uvicorn workers would mean
# two senders, two limiters, and a burst straight through the rate limit.
# Scaling out would mean moving the sender to its own process with a database
# lease -- see FAILURES.md.
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]
