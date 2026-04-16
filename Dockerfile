FROM python:3.11.9-slim

# Install Chromium + driver + minimal fonts/libs Chrome needs
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    fonts-liberation \
    libdbus-1-3 \
    libglib2.0-0 \
    libnss3 \
    libxss1 \
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
# Disable Chrome sandbox at OS level (belt+braces on top of opts flags)
ENV CHROMIUM_FLAGS="--no-sandbox --disable-setuid-sandbox"

EXPOSE 10000

# Single worker with many threads: Chrome is the bottleneck, not Python workers.
# Multiple workers = multiple Chrome instances in parallel = Render OOM crash.
# 1 worker + 8 threads handles concurrent requests safely.
CMD gunicorn app:app \
    --bind 0.0.0.0:${PORT:-10000} \
    --workers 1 \
    --threads 8 \
    --timeout 600 \
    --worker-class gthread \
    --keep-alive 5
