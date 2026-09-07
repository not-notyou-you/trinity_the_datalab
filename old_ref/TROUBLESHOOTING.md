# TROUBLESHOOTING
## Sentinel-1 Flood Detection Data Pipeline

---

## Database Issues

**`psycopg2.OperationalError: could not connect`**
- Check `DB_HOST`, `DB_PORT`, `DB_PASSWORD` in `.env`
- Verify PostgreSQL is running: `pg_isready -h localhost -p 5432`

**`ERROR: function uuid_generate_v4() does not exist`**
```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

**`ERROR: type "geometry" does not exist`**
```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

**`TimescaleDB extension not found`**
Schema falls back gracefully — all features work without it. Only automatic partitioning is disabled. Install from [docs.timescale.com](https://docs.timescale.com) if needed.

**`UNIQUE constraint violation on product_identifier`**
Normal behavior — `insert_satellite_scene()` is idempotent. Duplicate scenes are silently skipped and the existing `scene_id` is returned.

---

## API Issues

**`422 Unprocessable Entity`**
Parameter validation failed. Check the `/docs` Swagger UI for correct parameter types and formats.

**`404 on /api/quality/{scene_id}`**
Quality metrics haven't been computed yet. Run Module 6 (`QUALITY_ANALYTICS`) for this scene first, or run `python -m etl.seed_data` to insert sample data.

**`500 Internal Server Error`**
Check API logs: `docker-compose logs -f api` or `journalctl -u sentinel1-api -f`

---

## ETL Pipeline Issues

**`NotImplementedError` from module1–4/6**
These modules are stubs awaiting implementation. The orchestrator and database integration layer (Modules 5 + database_client) are fully functional. Wire your existing processing code into the `run()` functions in each module.

**Pipeline resumes from wrong stage**
Check `processing_jobs` table: only `status='SUCCESS'` jobs count as checkpoints. `FAILED` jobs are ignored and will be re-attempted.

---

## Testing Issues

**`pytest` fails with `connection refused`**
Create the test database first:
```bash
createdb sentinel1_flood_test
psql sentinel1_flood_test < database/schema.sql
```
Then set: `export TEST_DATABASE_URL=postgresql+psycopg2://postgres:pass@localhost:5432/sentinel1_flood_test`

**`PostGIS not available in test DB`**
```bash
psql sentinel1_flood_test -c "CREATE EXTENSION IF NOT EXISTS postgis;"
```
