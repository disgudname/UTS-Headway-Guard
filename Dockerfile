# Lightweight Python image
FROM ghcr.io/library/python:3.12-slim

# Prevent Python from writing .pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Create app directory
WORKDIR /app

# System deps (add curl for basic debug/health checks if needed)
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /app/
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy app
COPY . /app

# Non-root user for running the app, but keep root for startup tasks
RUN useradd -m appuser

# Fly will provide $PORT (defaults to 8080). We must use a shell form to expand it.
ENV PORT=8080

# Expose port (doc-only; Fly ignores EXPOSE but it’s still useful)
EXPOSE 8080

# Start via helper script that ensures /data permissions before dropping to appuser
CMD ["/app/start.sh"]
