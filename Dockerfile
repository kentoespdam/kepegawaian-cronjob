FROM ghcr.io/astral-sh/uv:debian-slim AS base
WORKDIR /app

FROM base AS builder
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --no-cache --frozen --no-dev

FROM base AS runner
COPY --from=builder /app /app
COPY . .
CMD ["uv", "run", "main.py"]