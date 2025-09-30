FROM ghcr.io/astral-sh/uv:alpine3.21
WORKDIR /app
COPY . .
RUN uv sync --locked
CMD ["uv", "run", "main.py"]