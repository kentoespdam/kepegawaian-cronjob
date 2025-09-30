FROM ghcr.io/astral-sh/uv:debian-slim
WORKDIR /app

COPY . .
RUN uv sync --no-cache --frozen --no-dev

CMD ["uv", "run", "main.py"]