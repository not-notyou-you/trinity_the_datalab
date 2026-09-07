# DEPLOYMENT GUIDE
## Sentinel-1 Flood Detection Data Pipeline

**Author:** Julius Marselinus (BRONTO) — NIM 00000111989

---

## Option A: Docker Compose (Recommended)

```bash
# Start database + API
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f api
docker-compose logs -f db

# Teardown (keep data)
docker-compose down

# Teardown (delete data)
docker-compose down -v
```

The compose file auto-runs schema migrations and seed data on first start via `docker-entrypoint-initdb.d/`.

---

## Option B: systemd Service (Linux Production)

### 1. Create service file

```bash
sudo nano /etc/systemd/system/sentinel1-api.service
```

```ini
[Unit]
Description=Sentinel-1 Data Pipeline API
After=network.target postgresql.service

[Service]
Type=simple
User=sentinel1
WorkingDirectory=/opt/sentinel1-flood-detection
EnvironmentFile=/opt/sentinel1-flood-detection/.env
ExecStart=/opt/sentinel1-flood-detection/venv/bin/uvicorn \
    api.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 2
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 2. Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable sentinel1-api
sudo systemctl start sentinel1-api
sudo systemctl status sentinel1-api
```

---

## Option C: Manual (Development)

```bash
# Terminal 1: database (already running)
# Terminal 2: API
source venv/bin/activate
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

---

## Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_HOST` | Yes | localhost | PostgreSQL host |
| `DB_PORT` | No | 5432 | PostgreSQL port |
| `DB_NAME` | Yes | sentinel1_flood | Database name |
| `DB_USER` | Yes | postgres | Database user |
| `DB_PASSWORD` | Yes | — | Database password |
| `DB_POOL_SIZE` | No | 5 | Connection pool size |
| `DB_MAX_OVERFLOW` | No | 10 | Extra connections above pool |
| `DB_ECHO` | No | false | Log all SQL (dev only) |
| `API_HOST` | No | 0.0.0.0 | API bind host |
| `API_PORT` | No | 8000 | API bind port |
| `OUTPUT_DIR` | No | processed | ETL output base directory |
| `TEST_DATABASE_URL` | No | — | Separate DB for pytest |

---

## Monitoring

Check API health:
```bash
curl http://localhost:8000/api/health
```

Check unresolved alerts:
```sql
SELECT alert_id, severity, title, triggered_at
FROM alert_events
WHERE is_resolved = FALSE
ORDER BY triggered_at DESC;
```

Check pipeline failures:
```sql
SELECT j.job_id, s.stage_name, j.error_message, j.completed_at
FROM processing_jobs j
JOIN processing_stages s ON s.stage_id = j.stage_id
WHERE j.status = 'FAILED'
ORDER BY j.completed_at DESC;
```
