FROM python:3.11.9-slim

# Install Chromium + driver (works on Debian slim)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# Copy app source
COPY . .

# Environment
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PYTHONUNBUFFERED=true
ENV RENDER=true

EXPOSE 10000

CMD gunicorn app:app \
    --bind 0.0.0.0:${PORT:-10000} \
    --workers 2 \
    --threads 4 \
    --timeout 300 \
    --worker-class gthread
