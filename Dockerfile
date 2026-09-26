# Lightweight Python image
FROM python:3.12-slim

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

# Tailscale: reaches the self-hosted Valhalla router on the home LAN (see
# ROUTING_ENGINE.md). Only activates at runtime if TS_AUTHKEY is set; harmless to ship
# in every image otherwise.
RUN curl -fsSL https://tailscale.com/install.sh | sh

# Install Python deps
COPY requirements.txt /app/
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt

# Headless Chromium for solving PulsePoint's AWS WAF challenge (see
# _mint_pulsepoint_waf_token in app.py). Full Chromium rather than --only-shell: the
# challenge was verified against Chrome's new headless mode, which the shell lacks.
# Shared path so appuser can run what root installed.
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright     PULSEPOINT_BROWSER_CHANNEL=chromium
RUN python -m playwright install --with-deps chromium &&     rm -rf /var/lib/apt/lists/*

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
