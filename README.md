# Clone Project
```bash
git clone https://github.com/kentoespdam/kepegawaian-cronjob.git
```

---

# Environment
```bash
cp env.example to .env
```

---

# Installation
```bash
# Install semua dependencies
uv sync --no-cache --frozen 

# Install tanpa dev dependencies
uv sync --no-cache --frozen --no-dev

# install group dev dependencies
uv sync --group dev
```

---

# Running APP

## Manual Running
```bash
# dev 
uv run uvicorn app.main:app --reload --port 8080

# start 
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000

#prod 
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## Docker
```bash
docker compose up -d --build
```
---