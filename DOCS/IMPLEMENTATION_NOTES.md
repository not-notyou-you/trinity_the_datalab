# Implementation Notes

Breadcrumbs for whoever touches the per-satellite processing model next. This
file records **decisions that are not obvious from the code**, the traps that
are already paid for, and the limits that are deliberate rather than accidental.

`DOCS/ETL.md` describes what the pipeline does. This file describes why it does
it that way, and what will bite you if you change it.

---

## 1. There is exactly one place that decides levels

`etl/processing_plan.py` is the only module allowed to answer:

| Question | Function |
|---|---|
| Which sources run, at which levels? | `load_processing_plan()` → `ProcessingPlan` |
| How many cross-source outputs (fusion files, preview folders)? | `ProcessingPlan.output_levels()` |
| Within one run, what level does source X contribute at? | `SourcePlan.level_for_run(run_level)` |
| Which tier do I read for that? | `SourcePlan.tier_for_run(run_level)` |
| How do I tag `data_products.processing_level`? | `SourcePlan.level_for_tier(tier)` |

Fusion (module 9), preview (module 10) and the orchestrator (module 5) all call
these rather than re-deriving the rules. That is the point: before this module
existed, each module had its own reading of "what RAW means", and those readings
drifted the moment one of them changed. If you need a new rule, add it here, not
at the call site.

`processing_plan.py` deliberately imports nothing from other ETL modules so it
can be imported from anywhere without a cycle. Keep it that way.

### The `output_levels()` rule, in one sentence

Two runs if any single source was configured `{RAW, PROCESSED}`; otherwise one.

The tempting alternative — "one run per distinct level across all sources" —
looks more general and is wrong. For `s1[RAW] + modis[PROCESSED]` it produces two
stacks whose contents are byte-identical, because neither source has a second
level to contribute. Twice the disk, no extra information.

---

## 2. Fusion is anchored to Sentinel-1, on purpose

`create_fusion_stack()` raises if `SENTINEL1` is not configured.

The S1 scene provides two things nothing else in the pipeline provides:

1. **The reference grid.** Every other layer is reprojected onto the S1 raster's
   CRS, transform and shape. MODIS (250 m sinusoidal) and GPM (~11 km) are both
   far coarser; picking either as the reference would throw away the resolution
   that makes the stack useful.
2. **The fusion dates.** The orchestrator drives everything per S1 scene
   (`_process_scene`), so "which dates exist" is literally the set of S1
   acquisitions. MODIS and GPM are daily and would otherwise fuse every day in
   the range.

`DOCS/ETL.md` mentions "or largest-extent source if no S1" as a design intent.
That path is **not implemented**. Implementing it means more than picking a grid:
you also need a date driver for a pipeline that currently has no scene loop for
non-S1 sources. Do not add a half version that silently produces stacks on an
arbitrary grid.

---

## 3. Traps already paid for

These were real failures found by `tests/test_per_satellite_config.py`. Each has
a regression test; don't "simplify" them away.

### 3.1 `band_name` carries the level for FUSION products

`data_products` de-duplicates via `is_latest` on
`(scene_id, band_name, product_tier, dataset_id)`. A RAW+PROCESSED dataset writes
two stacks for the same date, same scene, same tier. With a shared `band_name`
of `FUSION`, registering the second marks the first `is_latest = False` — the RAW
stack still exists on disk but disappears from every API listing.

Hence `FUSION_RAW` / `FUSION_PROCESSED`, and migration 018 widening
`data_products.band_name` to `VARCHAR(20)` (`FUSION_PROCESSED` is 16 characters;
at `VARCHAR(10)` the INSERT failed *after* the HDF5 was already on disk, so the
stage failed leaving an orphan file).

`quality_metrics.band_name` stays `VARCHAR(10)` — it only ever holds `VV`/`VH`.

### 3.2 `fusion_products` unique key includes `processing_level`

Migration 018 replaces `uq_fusion_date_region` with
`uq_fusion_date_region_level`. Same reason: without it the second stack of a
date overwrites the first row, discarding exactly the comparison the dataset was
configured to produce.

### 3.3 Date matching must be done in UTC

`acquisition_datetime` is `TIMESTAMPTZ`; psycopg2 returns it in the **database
session's** timezone. MODIS/GPM files are keyed by UTC date (the orchestrator
uses `acq_date.date()` from the download result, which is UTC).

In Asia/Jakarta, a 22:50 UTC acquisition comes back as `05:50+07:00` the *next*
day. Deriving candidate dates from that value shifts the whole search one day and
every auxiliary layer fills with NaN — with no error anywhere, because "no file
found" is a supported condition. `_as_utc()` in `module9_fusion.py` normalizes
first.

### 3.4 The alignment window is the full 24 hours

`ALIGNMENT_WINDOW_HOURS = 24`, and `_find_nearest_daily_file()` uses all of it,
not `// 2`.

Half the window means "nearest midnight". The descending S1 pass over
Jabodetabek is at ~22:50 UTC: 22.8 h from that day's midnight, 1.2 h from the
next day's. The pipeline only downloads *that day's* auxiliary data
(`ensure_aux_inputs_for_date` receives `s1_date`), so a 12 h window excludes the
only file that exists. Candidates are still ranked by smallest difference, so a
next-day file wins when it is actually present.

### 3.5 Unconfigured sources get no group; configured-but-missing get NaN

`fusion_layers_for()` builds the layer list from the configured sources only.
A consumer must be able to distinguish "this sensor was never requested" from
"requested, no data that day" — a NaN-filled group for an unrequested sensor
erases that difference and quietly teaches a model that the sensor is always
absent.

---

## 4. Naming conventions worth knowing

| Thing | Pattern | Defined in |
|---|---|---|
| Fusion HDF5 | `fusion_{YYYYMMDD}_{level}.h5` | `module9_fusion.fusion_h5_name()` |
| Fusion sidecar | `fusion_metadata_{level}.json` | `module9_fusion.fusion_metadata_name()` |
| MODIS raster | `modis_{YYYYMMDD}_{band}.tif` | `module7_modis_download.band_filename()` |
| GPM raster | `gpm_rain_{window}_{YYYYMMDD}.tif` | `module8_gpm_download.band_filename()` |
| S1 BRONZE | `*_{BAND}_crop.tif` | `module2_crop.run()` |
| S1 SILVER/GOLD | `*_{BAND}_lee.tif` | `module3_lee_filter.run()` |

Never hardcode these at a call site — module 9 and module 10 both look files up
through the defining function so a rename stays a one-line change.

**GPM RAW is the one place where the on-disk name and the HDF5 name differ on
purpose.** The file is `gpm_rain_24h_*.tif` (the pipeline's window key), but the
HDF5 layer is `/gpm/rainfall_daily`. The layer name is honest about content —
one day's rainfall, no accumulation — and stops a consumer from assuming a RAW
stack's "24h" is comparable to a PROCESSED stack's genuinely accumulated 24h
window. `_AuxLayer.name` vs `_AuxLayer.file_key` encodes this split.

---

## 5. Preview levels mirror fusion levels

Preview renders once per `output_levels()` entry, so preview folders and fusion
files always come in matching sets. If you change the level rule, both follow
automatically — that is why they share `output_levels()` instead of each having
its own logic.

The level is a **path segment** (`preview/{date}/{LEVEL}/{kind}/`) because both
levels emit identical filenames (`s1_vv.png`). The composite lives in its own
`composite/` folder rather than in `colored/` because its sidecar schema is
genuinely different: channel mapping instead of colormap + value range.

`preview_options = []` means "the user wants no previews" and `NULL` used to mean
"unspecified". Migration 018 backfills NULL to all three variants and sets the
column `NOT NULL`, so the empty array is now unambiguous — the orchestrator can
skip the whole stage on it without guessing.

---

## 6. Changed signatures (if you are updating a caller)

| Function | Was | Now |
|---|---|---|
| `module9_fusion.create_fusion_stack()` | returned `fusion_id: int` | returns `list[FusionRun]`, one per run level; takes `plan=` and `fusion_strategy=` |
| `module9_fusion._find_s1_gold()` | GOLD only | `_find_s1_products(..., tier=...)` |
| `module10.resolve_gold_inputs()` | GOLD only | kept as a PROCESSED-level alias of `resolve_source_inputs(..., processing_level=...)` |
| `module10.generate_previews()` | — | added `processing_level=`, `s1_files=`, `options=`; `s1_gold_files=` still accepted |
| `folder_manager.get_preview_kind_dir()` | `(id, name, scene, kind)` | added trailing `processing_level=` (defaults to `PROCESSED`) |

---

## 7. Running the tests

```
pytest tests/ -v --cov=etl --cov=api
```

`tests/test_per_satellite_config.py` is the end-to-end one: it stubs only the
network layer and the per-pixel math, writes **real** GeoTIFFs, and then opens
the resulting HDF5. `tests/test_pipeline_branching.py` covers tier/level tagging
with cheap byte placeholders and never reaches fusion.

If you add a stub that writes placeholder bytes where fusion will read, fusion
will fail in `rasterio.open` — that is the tests telling you the stub is in the
wrong layer, not a bug in module 9.

A note for debugging on Windows: pytest's default temp dir plus this project's
long dataset folder names sits close to the 260-character `MAX_PATH` limit.
Passing a deep `--basetemp` can produce `FileNotFoundError` on file writes that
have nothing to do with the code under test.
