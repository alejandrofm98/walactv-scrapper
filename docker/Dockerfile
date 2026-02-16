FROM python:3.12-alpine

WORKDIR /app

RUN apk add --no-cache \
    build-base \
    chromium \
    chromium-chromedriver \
    linux-headers \
    python3-dev \
    tzdata && \
    cp /usr/share/zoneinfo/Europe/Madrid /etc/localtime && \
    echo "Europe/Madrid" > /etc/timezone
# copy requirements and install
COPY docker/config/requirements.txt .
RUN pip install --no-cache-dir 'setuptools>=68' && \
    pip install --no-cache-dir -r requirements.txt

# copy the script
COPY scripts/ .