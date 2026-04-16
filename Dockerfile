FROM python:3.11.9-slim

# Install Chromium + driver (Debian Bookworm versions — guaranteed compatible)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Verify binaries exist (build fails early if not found)
RUN which chromium && which chromedriver && \
    chromium --version && chromedriver --version

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# Copy all app source files
COPY . .

# Chrome / Chromedriver paths — used by app.py and it_suite.py
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PYTHONUNBUFFERED=true
ENV RENDER=true

EXPOSE 10000

# --disable-dev-shm-usage is already set in app.py Chrome opts.
# We also pass it at container level via shm-size in render.yaml.
CMD gunicorn app:app \
    --bind 0.0.0.0:${PORT:-10000} \
    --workers 2 \
    --threads 4 \
    --timeout 300 \
    --worker-class gthread
