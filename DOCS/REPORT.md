# Dataset Report

What `GET /api/datasets/{id}/report` produces, where every number in it comes from, and what it deliberately does not claim. Code: `etl/report_generator.py` (layout, charts, narrative), `etl/report_stats.py` (all statistics), `etl/report_forecast.py` (forecast), `api/routes/report.py` (endpoints). Rationale: DOCS/DECISIONS.md D27.

> This file replaces the original report *specification*. That spec asked for content the pipeline cannot measure (MODIS land-surface temperature, interferometric coherence, ground-truth station RMSE, geolocation error in metres, thermal-noise and phase-wrapping artifacts) and illustrated it with invented numbers. The implementation reports only measured values and, where the spec asked for something unmeasurable, says so and shows the nearest real proxy. A copy of the old spec survives as `report/report-detailed.md` (the generator's docstrings still cite it).

## Access

| Endpoint | Returns |
|---|---|
| `GET /api/datasets/{id}/report?force=false` | PDF, downloaded as `{slug}_report.pdf` |
| `GET /api/datasets/{id}/report/json?force=false` | JSON export (same content as section 11), `{slug}_report.json` |

- Generated **synchronously** on request; no job, no polling.
- Written to `data/datasets/{id}_{slug}/reports/report_{YYYYMMDDTHHMMSSZ}.pdf` with the `.json` next to it. The newest PDF is reused while its mtime is newer than `datasets.updated_at`; `force=true` always regenerates.
- Read-only: queries the DB and reads COGs/HDF5 attributes; never writes products, lineage or schema. Any failure becomes `ReportGenerationError` → HTTP 400.
- Works for normal datasets and for Live Area datasets (Live Monitoring card: **Report / JSON / Regenerate**). In Dataset Catalog it is under the card's "⋯" menu.

## PDF Format

- A4, ReportLab `multiBuild` (two passes, so the auto-generated table of contents and PDF bookmarks have correct page numbers).
- Font: DejaVu Sans / Sans Mono (bundled with matplotlib) so ✓ ⚠ ✗ ° ± and the ASCII diagrams render; falls back to Helvetica/Courier if missing.
- Styles: title 24 pt, section 16 pt, subsection 13 pt, body 10.5 pt, captions/notes 9 pt italic.
- Header/footer on every page: dataset name and page number.
- Charts are matplotlib PNGs with a consistent palette per source (Sentinel-1 blue, MODIS orange, GPM green, Fusion purple). A new section starts on a new page only when less than about a third of the page is left.
- Report version constant: `REPORT_VERSION = "2.0"`.

## Data Sources

Everything comes from two places (`report_stats.collect()`):

1. **Aggregate SQL** — `quality_metrics` (Sentinel-1 backscatter mean/std/min/max in dB, nodata %, speckle index, quality score/flag), `data_products` (tiers, sizes, processing levels per source), `fusion_products` (dates, strategy, offsets), `processing_logs` / jobs (stage durations), `satellite_scenes` (orbit direction, acquisition time).
2. **Pixel statistics from the dataset's own COGs** — MODIS FLOOD/NDVI/NDWI and GPM rainfall have no scalar columns in the DB, so per-date AOI means, valid fractions and class shares are computed from the rasters while the report is built (~10 ms per file).

Rules applied throughout:
- Sentinel-1 statistics use only scenes flagged `PASS` and in dB units (`is_linear_units()` detects legacy linear-sigma0 rows and excludes them from dB statistics).
- MODIS/GPM have no quality flag in the DB; one is derived from the valid-pixel fraction.
- Missing data yields an empty table or "—", never a placeholder number.
- Seasons follow the Indonesian monsoon: DJF wet, MAM transition I, JJA dry, SON transition II. A "rainy day" is ≥ 0.1 mm; a data gap is > 10 days without observation.

## Sections

| # | Title | Contents |
|---|---|---|
| 1 | Cover Page & Executive Summary | Dataset name, generation time, dataset ID, report version; auto-written summary; key metrics (period, sources, scenes, completeness, size); key findings to watch |
| 2 | Configuration & Data Ingestion Summary | 2.1 dataset overview (region, bbox, area km², created, status); 2.2 per-source configuration (product, resolution, levels); 2.3 spatial & temporal coverage — ASCII bbox map in context, daily ASCII timeline and month grid per source; 2.4 ingestion summary per stage, validation checks performed, quality-flag distribution |
| 3 | Processing Level Comparison & Ablation Study | Per source: ASCII processing flow, RAW vs PROCESSED products and on-disk sizes, processing time per stage; 3.4 effectiveness chart (date retention and size reduction per tier); RAW vs PROCESSED fusion stacks when both levels exist |
| 4 | Fusion Strategy & Temporal Alignment | 4.1 strategy and pseudocode (following `fusion_strategies.py` / `module9_fusion.py`); 4.2 case studies of real dates (all 3 sources, 2 of 3, gap-filled); 4.3 alignment statistics, gap-fill/coverage summary, Sentinel-1 acquisition time of day (UTC) |
| 5 | Overall Trends & Patterns | 5.1 time series per variable with season shading, daily rain heatmap (month × day), seasonal z-score heatmap, monthly summary across sources, linear trend per 30 days; 5.2 summary findings |
| 6 | Sentinel-1 Deep Dive | 6.1 VV/VH statistics, box plots, VV/VH ratio, monthly VV table; 6.2 orbit analysis (ascending/descending, revisit intervals) and acquisition timeline; 6.3 radiometric quality (score histogram), geometric/coverage quality via valid-pixel fraction, and in place of coherence the seasonal stability (σ, coefficient of variation) of VV; 6.4 data artifacts & known issues actually observed (FAIL scenes, low coverage, linear-unit rows); coloured previews |
| 7 | MODIS Deep Dive | 7.1 product characteristics (MCDWD flood, NDVI/NDWI composites) and surface-index statistics; 7.2 cloud cover / valid-fraction assessment and monthly FLOOD class composition; 7.3 validation via **cross-sensor consistency** — Pearson r for NDWI ↔ % flood, 7-day rain ↔ % flood, 7-day rain ↔ NDWI (expected positive), 7-day rain ↔ VV (expected negative); previews |
| 8 | GPM Deep Dive | 8.1 IMERG summary (run used, rainy days, totals), monthly precipitation, multi-day accumulation; 8.2 extreme events (top days, percentiles); 8.3 seasonal breakdown (wet vs dry season); previews |
| 9 | Data Quality & Warnings | 9.1 health scorecard; 9.2 issues per source with severity (including `audit_dataset_coverage` findings for fusion stacks missing an S1 frame); 9.3 recommendations — suitable / use with caution / not suitable — and storage per tier |
| 10 | Conclusions: Current Conditions & Forecast | 10.1 method; 10.2 forecast panels; 10.3 current conditions; 10.4 *N*-day forecast table; 10.5 rainfall outlook & inundation risk; 10.6 conclusions written from the forecast numbers and their confidence |
| 11 | JSON Summary & Machine-Readable Export | 11.1 the JSON export, also served by `/report/json` |

Sections or tables for sources not configured in the dataset are omitted.

## Health Scorecard (9.1)

| Score | Computed as |
|---|---|
| Completeness | Mean of S1 completed/total scenes and MODIS/GPM date completeness (capped at 100) |
| Radiometric Quality | Sentinel-1 radiometric score from `/api/quality/dataset/{id}/by-source` |
| Spatial Coverage | Mean valid-pixel fraction over S1 products and MODIS/GPM rasters × 100 |
| Fusion Success | % of S1 dates that have a fusion stack with all 3 sources (multi-source datasets only) |
| **Overall Data Health** | Mean of the scores that exist |

Labels: ≥ 90 Excellent, ≥ 75 Good, ≥ 60 Acceptable, otherwise Poor.

## Forecast (section 10)

`report_forecast.py`, numpy only, uses this dataset's data and nothing external.

- **Horizon** = period length / 3 days (10-day dataset → 3 days, 90-day → 30 days).
- **Variables**: Sentinel-1 VV and VH (dB, PASS scenes), MODIS NDVI, NDWI (days with ≥ 50 % valid pixels), MODIS flood extent (% of pixels), GPM rainfall (mm/day, modelled in log1p space). Irregular observations are interpolated to a daily grid.
- **Model choice by backtest**: naive (last value), period mean, SES (α from a grid) and Holt damped trend (φ 0.9) are trained on the first 2/3 and tested on the last 1/3 — the same length as the real horizon. Skill = 1 − MAE(model) / MAE(naive); a model that does not beat naive is not used.
- **Bands**: 80 % and 95 %, calibrated from backtest errors, clipped to physical ranges.
- **Rainfall outlook**: empirical probability of rainy and heavy-rain (≥ 20 mm) days. **Flood scenario**: linear regression of MODIS flood % on 7-day rain (clear days only), used to translate the rain outlook into an indicative inundation range.

## JSON Export

Top-level keys: `report_metadata` (including an `implementation` summary of what was rendered), `dataset`, `spatial_bounds`, `temporal_range`, `sources` (per source: enabled, processing levels, records in/out/loss %, unique acquisition dates, ascending/descending passes, quality metrics), `fusion_strategy`, `processing_levels`, `forecast` (`horizon_days`, rule, start/end, per-variable forecasts, `rain_outlook`, `flood_scenario`), `quality_scores`, `storage` (tier breakdown), `quality`, `warnings` (`source`, `severity`, `message`), `recommendations`.

## Deliberately Not Reported

| Asked for by the original spec | Why not | What is shown instead |
|---|---|---|
| MODIS land-surface temperature | No MOD11 product in the pipeline | NDVI/NDWI and MCDWD flood statistics |
| Interferometric coherence / phase wrapping | GRD intensity products, not SLC | Seasonal backscatter stability |
| Geolocation accuracy (m) | No ground control points in the schema | Valid-pixel fraction as a footprint-coverage proxy |
| Ground-truth RMSE / bias | No station data in the schema | Cross-sensor consistency correlations |
| Report owner e-mail | No users/auth (D12) | Dataset ID and generation time |
