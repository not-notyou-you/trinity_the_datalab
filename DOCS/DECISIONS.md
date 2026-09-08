# Architecture Decisions

## D1: Fusion Strategy as User Parameter (Not Hardcoded)

**Decision**: Let users choose CO_OCCURRENCE, FULL_COVERAGE, or HYBRID at dataset creation time.

**Why**: The "best" fusion strategy depends on the research question. Co-occurrence gives perfect temporal alignment (better for ML training) but fewer samples. Full coverage maximizes temporal density (better for monitoring). Making this a user parameter eliminates the need to pick one — and is the core novelty of DataLab (no competitor offers this).

**Trade-off**: More complex pipeline orchestration. Fusion stage must branch on strategy.

## D2: Per-Satellite Processing Level (Not Global Toggle)

**Decision**: Each satellite has its own RAW/PROCESSED definition, configured independently. Users can request RAW, PROCESSED, or both per source.

**Why**: "Processing" means fundamentally different things for each sensor. Sentinel-1 RAW vs PROCESSED is about speckle filtering (Lee filter ablation). MODIS RAW vs PROCESSED is about whether derived indices (NDVI, NDWI) are computed. GPM RAW vs PROCESSED is about single-day rainfall vs multi-day accumulation windows. A single global toggle would force all three to the same level, preventing mixed configurations like "filtered S1 + raw MODIS flood map + accumulated GPM rainfall."

**Per-satellite definitions**:
- **S1 RAW**: Calibrate + reproject + crop (no Lee filter, no QA) → BRONZE
- **S1 PROCESSED**: + Lee filter 7×7 + QA analytics + COG export → SILVER → GOLD
- **MODIS RAW**: Flood map only (no NDVI/NDWI) → BRONZE
- **MODIS PROCESSED**: + NDVI + NDWI from reflectance → SILVER → GOLD
- **GPM RAW**: Daily rainfall only → BRONZE
- **GPM PROCESSED**: + 24h/72h/7d accumulation → SILVER → GOLD

**Trade-off**: More complex UI (per-source checkboxes instead of one toggle) and more complex pipeline orchestration. Mitigated by a "Pilih Semua" master toggle for users who don't need fine control.

**Schema impact**: Processing config moves from a TEXT[] column on `datasets` to a separate `dataset_source_config` junction table with per-source `processing_levels`.

## D3: Lakehouse (6 Tiers) Instead of Flat Storage

**Decision**: RAW → BRONZE → SILVER → GOLD → PREVIEW → FUSION tier hierarchy with automatic cleanup.

**Why**:
- RAW enables re-calibration without re-downloading (~1.6 GB/scene saved)
- BRONZE is the checkpoint before expensive filtering
- SILVER enables quality inspection on filtered output
- GOLD is the analysis-ready single-sensor format (COG)
- FUSION is the ML-ready multi-modal format (HDF5)
- Users only keep tiers they need; cleanup frees disk automatically

## D4: HDF5 for Fusion (Not Multi-Band GeoTIFF)

**Decision**: Fused output is HDF5 with grouped datasets, not a single multi-band GeoTIFF.

**Why**: HDF5 supports named groups (`/sentinel1/VV`, `/modis/NDVI`), mixed dtypes (uint8 for FLOOD, float32 for backscatter), internal chunking for efficient slicing, and gzip compression. Multi-band GeoTIFF would flatten everything into anonymous bands with uniform dtype. HDF5 is natively supported by PyTorch, TensorFlow, h5py, xarray.

## D5: PostgreSQL + PostGIS + TimescaleDB (Not NoSQL)

**Decision**: Relational database with spatial and time-series extensions.

**Why**: Structured metadata (scene→product→lineage) maps naturally to relational schema. PostGIS enables spatial queries (bbox intersection). TimescaleDB optimizes time-series queries (alert events, access logs). ACID guarantees protect lineage integrity during parallel processing. TimescaleDB is optional — schema degrades gracefully to plain tables.

## D6: Single-Machine Architecture (MAX_CONCURRENT=2)

**Decision**: No distributed computing, no cloud auto-scaling. Pipeline runs on one machine with a 2-scene concurrency limit.

**Why**: This is a research tool, not a production service competing with GEE or MPC. Single-machine keeps deployment simple, avoids cloud vendor lock-in, and runs offline. The concurrency limit prevents OOM on 8–16 GB machines during memory-intensive stages (Lee filter, HDF5 fusion).

**What we do NOT claim**: Computational superiority over GEE/MPC. They have planet-scale infrastructure; we have parametric configurability on a single machine.

## D7: APScheduler (Not System Cron)

**Decision**: In-process Python scheduler, not OS-level cron.

**Why**: Easy pause/resume, Python-native event handling, no external dependency. PostgreSQL advisory lock ensures only one API worker runs the scheduler in multi-worker deployments.

## D8: Vanilla JS Frontend (No React/Vue)

**Decision**: No frontend framework, no build step.

**Why**: The dashboard is a monitoring/configuration UI, not a complex SPA. Vanilla JS + Leaflet keeps deployment trivial (static files served by FastAPI), reduces build complexity, and avoids framework churn. The 4-step wizard and dataset cards don't justify React overhead.

## D9: SHA-256 Lineage Tracking

**Decision**: Every data product gets a SHA-256 checksum. Every transformation edge in `data_lineage` records input and output checksums.

**Why**: Reproducibility guarantee. Given a fusion HDF5 file, the lineage graph traces back to the exact RAW downloads with cryptographic proof that no intermediate was tampered with. No competitor (GEE, openEO, MPC) provides this level of provenance tracking.

## D10: Non-Fatal Auxiliary Sources

**Decision**: MODIS and GPM failures don't fail the pipeline. Missing layers are filled with NaN in fusion output.

**Why**: Optical MODIS is cloud-contaminated during monsoon season. GPM may have latency gaps. Blocking the entire pipeline on auxiliary data would drastically reduce output. NaN fill preserves array shape so downstream consumers (PyTorch DataLoader, etc.) don't crash. Fusion metadata records temporal offsets so consumers know which layers are same-day vs. offset.

## D11: Terminology — "Automated Data Ingestion" (Never "Scraping")

**Decision**: All documentation and code comments use "automated data ingestion" or "ETL harvester."

**Why**: CDSE and NASA Earthdata are authorized APIs with explicit authentication. "Scraping" implies unauthorized access and is factually incorrect. This matters for academic integrity and institutional cooperation.

## D12: No Authentication on API (Deferred)

**Decision**: API ships without auth. Add before any public exposure.

**Why**: Development velocity. Auth is orthogonal to the core research contribution and can be bolted on (API key middleware or OAuth) without architectural changes. Documented in deployment checklist as a required step.

## D13: Clone Last Config via Backend (Hybrid Strategy)

**Decision**: "Pakai Config Sebelumnya" button fetches last dataset config from DB via `GET /api/datasets/last-config`. Falls back gracefully if unavailable.

**Why**: 
- **DB-persisted, not session-dependent** — user's config survives browser close, multiple devices
- **API-first** — aligns with architecture (REST is single source of truth)
- **Graceful fallback** — if fetch fails, button still visible but shows error, doesn't break UX
- **Minimal cost** — single SELECT query on datasets table, ordered by created_at DESC LIMIT 1

**Alternative (not chosen)**: Pure LocalStorage would be faster (no network call) but loses data if user clears cache or switches device. Hybrid (try backend, fallback to localStorage) adds complexity for marginal benefit.

**Constraint**: Endpoint returns only config fields (region_id, sources, fusion_strategy, preview_options), **not** datasets table keys like dataset_id or name. This ensures user deliberately re-enters dates and dataset name (not duplicating).

**UI Behavior**:
- Button only shows if at least 1 dataset exists
- Click triggers API call; loading state shown
- On success: form fields auto-populate, focus first editable field
- On failure: inline error message, user can still proceed manually
- User can edit any field after populate (clone is preset, not locked)
