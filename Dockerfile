FROM ghcr.io/astral-sh/uv:debian-slim
WORKDIR /app

COPY . .
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "main.py"]