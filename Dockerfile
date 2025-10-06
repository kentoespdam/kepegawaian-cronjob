FROM ghcr.io/astral-sh/uv:debian-slim AS base
WORKDIR /app

FROM base AS builder
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --no-cache --frozen --no-dev

FROM base AS runner
# Create a non-root user and switch to it
RUN adduser --disabled-password --gecos "" appuser && \
    chown -R appuser:appuser /app
USER appuser

# Copy application files with the correct ownership
COPY --from=builder --chown=appuser:appuser /app /app
COPY --chown=appuser:appuser . .

CMD ["uv", "run", "main.py"]