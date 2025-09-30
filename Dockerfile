FROM ghcr.io/astral-sh/uv:alpine3.21
WORKDIR /app

# Install system dependencies untuk build pandas dan dependencies
RUN apk add --no-cache \
    build-base \
    python3-dev \
    libffi-dev \
    openssl-dev \
    cargo \
    && apk add --no-cache \
    musl-dev \
    linux-headers \
    && apk add --no-cache \
    g++ \
    gfortran \
    openblas-dev \
    && apk add --no-cache \
    libxml2-dev \
    libxslt-dev

COPY . .
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "main.py"]