FROM python:3.12-alpine

WORKDIR /app

RUN apk add --no-cache \
    build-base \
    linux-headers \
    python3-dev \
    tzdata && \
    cp /usr/share/zoneinfo/Europe/Madrid /etc/localtime && \
    echo "Europe/Madrid" > /etc/timezone

# copy requirements and install with retries
COPY docker/config/requirements.txt .
RUN pip install --no-cache-dir --timeout 300 --retries 5 -r requirements.txt || \
    pip install --no-cache-dir --timeout 300 --retries 5 -r requirements.txt || \
    pip install --no-cache-dir --timeout 600 --retries 10 -r requirements.txt

# copy the script
COPY scripts/ .