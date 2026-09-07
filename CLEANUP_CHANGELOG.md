# Cleanup Changelog

## Date: 2026-08-28

### Deleted Files
- [x] `check.txt` — Debug output file, no active references. Backed up to `backup/check.txt.backup_20260828_162258` before removal.
- [x] `config/config_real.json` — No code references (`grep` across `api/`, `etl/`, `tests/` found none). Superseded by `config/config.json` + environment variables via `etl/config.py`.
- [x] `config/config_locations_real.json` — Same as above, no references found.
- [x] `etl/scheduler.py` (296 lines) — Old standalone pipeline runner. Its only caller, `api/routes/pipeline.py`'s `/trigger` endpoint, was rewritten to retry failed dataset jobs via `DatasetManager` instead of importing `etl.scheduler`. No remaining references anywhere in `api/`, `etl/`, `tests/`, `web/`.
- [x] `database/migrations/004_add_datasets.sql` — Superseded by `database/migrations/004_add_dataset_management.sql` (same version number, so only one could stand — the `_management` variant is the one wired into `schema_migrations`).
- [x] `web/Sentinel1Dashboard.html` — Old standalone dashboard page, superseded by `web/index.html`. No references in remaining HTML/JS/Py.
- [x] `web/icons.js` — Only consumed by the now-deleted dashboard page. No references found.
- [x] `processed/{bronze,silver,gold}/.gitkeep`, `recovered_temp/.gitkeep` — Placeholder files for empty runtime data dirs; the directories themselves are gitignored and are recreated on demand by `PipelineConfig.ensure_dirs()` in `etl/config.py`, so the gitkeeps are not required for the app to run. **Note:** since these were the only tracked files keeping the empty dirs in git, the directories will no longer appear in a fresh checkout until the pipeline runs once.

### Not Deleted (correcting earlier assumptions)
- `old_ref/` — **Still present**, not removed. Contains 7 markdown docs (`API_DOCUMENTATION.md`, `DATABASE_DESIGN.md`, `DATA_DICTIONARY.md`, `DEPLOYMENT.md`, `SETUP_GUIDE.md`, `THESIS_CHAPTERS.md`, `TROUBLESHOOTING.md`), all modified as recently as today. This is documentation, not reference *code*, and is still current — left in place pending an explicit decision on whether `DOCS/` (`INTERFACE.md`, `PRD.md`, `README.md`) should absorb/replace it.
- `checkpoints_pipeline/` — **Still present**, empty except `.gitkeep`. Already gitignored (`checkpoints_pipeline/*` with `.gitkeep` excepted) and used at runtime by `PipelineConfig.checkpoint_dir`. No reason to delete.
- `config/config.json`, `config/config_locations.json` — **Not deleted.** These are the live, active config files (both modified in this pass, not removed). There is no `config.development.json` / `config.production.json` split in this repo — `etl/config.py` loads defaults from env vars (optionally overridden via `Config.from_json`), it does not do ENVIRONMENT-based file switching. The "consolidate into config.development/production.json" plan does not apply to this codebase as it currently stands.

### New Files (added, not cleanup deletions)
- `etl/constants.py` — Shared MODIS/GPM dedup-key constants (`source`, `product_short_name`, `tile_id`), pulled out so `etl/live_scheduler.py` and `etl/module9_fusion.py` can't drift and silently create duplicate/orphaned `nasa_scenes` rows for the same scene.
- `database/migrations/006_fix_cleanup_operations_cascade.sql` — Drops the `ON DELETE CASCADE` FK from `cleanup_operations.dataset_id`. Paired with the `004_add_dataset_management.sql` edit below: a completed `cleanup_operations` row was being cascade-deleted the instant its parent `datasets` row was deleted, so `GET /datasets/{id}/deletion-progress` returned 404 right after a delete finished instead of the completed record.
- `tests/test_credentials.py` — Standalone script to sanity-check DB / Copernicus CDSE / NASA CMR / API credentials from `.env`.
- `backup/` (untracked, local only) — `check.txt.backup_20260828_162258`, `scheduler.log.backup_20260828_162258`. Pre-deletion safety copies; not intended to be committed (consider adding `backup/` to `.gitignore` if it should stay local-only).

### Code Changes
- [x] `api/routes/pipeline.py` — Removed the `_trigger_running` thread-lock + `etl.scheduler` background-thread trigger; `/trigger` now retries the last FAILED job for a dataset via `DatasetManager`.
- [x] `database/migrations/004_add_dataset_management.sql` — `cleanup_operations.dataset_id` changed from `INTEGER NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE` to a plain `INTEGER NOT NULL` column (see migration 006 rationale above).
- [x] `etl/dataset_manager.py`, `etl/live_scheduler.py`, `etl/metadata_manager.py`, `etl/module9_fusion.py`, `etl/database_client.py` — Updated to use the shared constants in `etl/constants.py` and the retry/cascade changes above.
- [x] `web/index.html` — New CSS custom properties (`--panel-alt`, `--hairline`, `--text`, etc.), scene-tier-ring rendering now takes a `requiredTiers` argument instead of assuming all four tiers apply, added `openScenes` UI state.
- [ ] Updated `etl/config.py` to use ENVIRONMENT-based config loading — **not applicable**, see "Not Deleted" above.
- [ ] Updated `.gitignore` to ignore old config backups — not needed, no config backup files were created; `backup/` (see above) is untracked but not yet gitignored.
- [ ] Updated `.env` examples to include ENVIRONMENT=production/development — not applicable, this repo doesn't branch on `ENVIRONMENT`.

### Verification Performed
- [x] `ast.parse()` on every changed/added Python file (`api/routes/pipeline.py`, `etl/dataset_manager.py`, `etl/live_scheduler.py`, `etl/metadata_manager.py`, `etl/module9_fusion.py`, `etl/database_client.py`, `etl/constants.py`) — all parse cleanly, no syntax errors.
- [x] Repo-wide grep for `etl.scheduler` / `etl/scheduler` imports across `api/`, `etl/`, `tests/`, `database/`, `web/` — zero hits, safe to delete.
- [x] Repo-wide grep for `config_real` / `config_locations_real` references across `api/`, `etl/`, `tests/` — zero hits, safe to delete.
- [x] Repo-wide grep for `Sentinel1Dashboard` / `icons.js` references across `*.html`, `*.py`, `*.js` — zero hits, safe to delete.
- [ ] Ran scheduler with `ENVIRONMENT=development` / `ENVIRONMENT=production` — not applicable (see above), not run.
- [ ] Live API endpoint smoke test / live DB connection test — **not run in this pass**. Everything above is currently uncommitted (staged deletes + unstaged edits + untracked new files); recommend running `pytest tests/` and starting the API against a real Postgres before committing.

### Recovery Notes
If anything breaks after this cleanup:
1. `check.txt` → `cp backup/check.txt.backup_20260828_162258 check.txt`
2. Any tracked file → `git restore <filename>` (nothing has been committed yet, so this reverts cleanly from the index/HEAD)
3. Untracked additions (`etl/constants.py`, `tests/test_credentials.py`, `database/migrations/006_fix_cleanup_operations_cascade.sql`, `backup/`) are new and not tracked — delete manually if they need to be rolled back.
