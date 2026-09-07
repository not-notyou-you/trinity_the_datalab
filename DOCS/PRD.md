# Product Requirements Document (PRD)
## The Trinity — Data Platform for ML/DL Flood Prediction

---

## 1. Executive Summary

**The Trinity** is a production-grade data pipeline platform designed for **machine learning and deep learning practitioners** building flood prediction and emergency response systems for Jakarta metropolitan area. The platform automates acquisition, processing, and delivery of multi-sensor geospatial data (Sentinel-1 SAR, MODIS, GPM) in ML-ready formats with complete data lineage, quality assurance, and API-first access.

**Target Users**: Data scientists, ML engineers, geospatial analysts, civil engineers specializing in disaster management.

---

## 2. Core Goals & Value Propositions

### For ML Model Development
- **Curated datasets**: Pre-filtered, quality-assured satellite imagery
- **Reproducibility**: Full lineage tracking enables model auditability
- **Multi-modal fusion**: Stack Sentinel-1 + MODIS + GPM in single HDF5 for training
- **Auto-refresh**: Live ingestion ensures models train on current data patterns

### For Operations & Deployment
- **24/7 monitoring**: Automated daily data collection (Sentinel-1, MODIS, GPM)
- **Sub-daily latency**: Within 24 hours of satellite overpass for flood detection
- **Scalable storage**: Tiered data retention (keep GOLD, auto-delete intermediate tiers)
- **API-ready**: Direct ingestion into Fiona/GeoPandas/Rasterio workflows

### For Data Governance
- **Provenance**: Every pixel traceable to raw source with processing parameters
- **Quality flagging**: QA/FAIL verdict on every scene + alerting
- **Compliance**: MD5/SHA-256 checksums, version control, audit logs

---

## 3. Data Specification for ML/DL

### Input Data Tiers

| Tier | Audience | Use Case | Latency | Storage |
|------|----------|----------|---------|---------|
| **RAW** | Research only | Inspect original calibration | varies | 400–800 MB/scene |
| **BRONZE** | Calibration validation | Verify atmospheric correction | low | 45–50 MB/scene |
| **SILVER** | Preprocessing pipelines | Train speckle-robust models | low | 45–50 MB/scene |
| **GOLD** | Production ML inference | Deploy flood detection classifier | low | 40–45 MB/scene |
| **PREVIEW** | Humans, not models | Figures, QA-by-eye, dataset browsing | low | 9–30 MB/date |
| **FUSION** | Multi-modal deep learning | Train on S1 + MODIS + GPM stacks | low | 12–15 MB/date |

### PREVIEW Tier Specifications (Visual Products — Not for Analysis)

PNG renders of the GOLD COGs for one acquisition date, written to
`data/datasets/{id}_{slug}/preview/{YYYYMMDD}/`. Produced by
`etl/module10_generate_preview.py`, which runs **after GOLD_EXPORT and before
FUSION** — a dataset that only requested FUSION deletes `gold/` at cleanup, so
that is the only window in which every source raster still exists.

**Two variants, two audiences:**

| | `grayscale/` | `colored/` |
|--|--------------|------------|
| Purpose | Scientific reading of a single file | Publication, presentation, cross-date comparison |
| PNG mode | LA (luminance + alpha) | RGBA |
| Value range | Percentile 2–98, recomputed **per file** | Per source; MODIS/GPM pinned to fixed ranges |
| Comparable across dates | **No** — the scale moves with the file | Yes, for MODIS and GPM |

**Layers** (one `{key}.png` in each subfolder):

| key | Source | Band | `colored/` colormap | Range | Reading |
|-----|--------|------|---------------------|-------|---------|
| `s1_vv` | Sentinel-1 | VV | viridis | percentile 2–98 (dB) | dark = smooth surface (calm water, asphalt, flooded paddy) |
| `s1_vh` | Sentinel-1 | VH | viridis | percentile 2–98 (dB) | volume scattering; sharper water/land contrast than VV |
| `modis_ndvi` | MODIS | NDVI | RdYlGn | fixed −1..1 | red = water/bare, green = dense canopy |
| `modis_ndwi` | MODIS | NDWI | BrBG | fixed −1..1 | brown = dry land, blue-green = open water |
| `gpm_rain_24h` | GPM | RAIN_24H | YlGnBu | 0..p98 | immediate flash-flood trigger |
| `gpm_rain_72h` | GPM | RAIN_72H | YlGnBu | 0..p98 | multi-day soil saturation |
| `gpm_rain_7d` | GPM | RAIN_7D | YlGnBu | 0..p98 | antecedent moisture proxy |

Plus `colored/s1_rgb_composite.png` — false colour with R = VV, G = VH,
B = VV−VH (dB). Open water reads dark/blue, vegetation green, built-up areas
pink to white; useful for separating standing water from topographic shadow,
which look identical in any single band.

**Conventions**

- Max width 1024 px (aspect preserved), PNG compression level 6.
- NoData is **transparent** (`alpha=0`), never black — black is a legitimate
  value for low backscatter, so mapping NoData to black would erase the
  difference between "water" and "no data".
- Sentinel-1 is converted to decibels (`10*log10`) before stretching, because
  GOLD stores **linear** sigma0 whose distribution is extremely right-skewed.
  Auto-detected: data already in dB is left alone.
- Missing layers (e.g. a failed MODIS download) are skipped and listed under
  `skipped` in `preview_metadata.json`, never fatal.

**Not for quantitative analysis.** Colours are quantised to 8 bits and ranges
are clipped. Use `gold/*.tif` or `fusion/*.h5` for measurements.

PREVIEW is a *derived* tier: it is not in the RAW→FUSION lineage, is not
registered in `data_products`, and is never removed by tier auto-deletion.

**Opting out.** `datasets.generate_preview` (BOOLEAN NOT NULL DEFAULT TRUE,
migration 016) turns the stage off per dataset — surfaced as the "Buat Preview
(Grayscale + Berwarna)" checkbox on the create form, ticked by default. It is
sent as a top-level `generate_preview` field on `POST /api/datasets`, not
inside `quality_settings`, which holds data-quality thresholds rather than
pipeline switches. Disabling previews does not affect FUSION.

### GOLD Tier Specifications (Recommended for Training & Inference)

**Format**: Cloud-Optimized GeoTIFF (COG)
- **Geolocation**: WGS84 (EPSG:4326)
- **Resolution**: 10 m × 10 m
- **Projection**: Cartographic (UTM 48S available via COG reprojection)
- **Encoding**: float32 (linear sigma-0 or dB, user-selectable)
- **Compression**: LZW (lossless)
- **Overviews**: Levels 2, 4, 8, 16 (enables efficient pan-zoom in viewers, zoom-aware inference)

**Bands**:
- **VV (co-pol)**: Linear sigma-0 in dB → typical range [-25 to +2 dB]
  - Water detection: strong negative backscatter (-18 to -22 dB)
  - Urban areas: moderate (+0 to -5 dB)
  - Vegetation: variable (-5 to -15 dB)
  
- **VH (cross-pol)**: Linear sigma-0 in dB → typical range [-25 to -5 dB]
  - Vegetation scattering: enhanced cross-pol response
  - Texture & roughness: superior to co-pol for classification

**Geospatial Metadata**:
- **CRS**: EPSG:4326 (WGS84 lat/lon)
- **Bounds**: Jakarta metropolitan area (106.4°–107.2°E, 5.9°–6.7°S)
- **Pixel registration**: Upper-left corner
- **NoData value**: NaN (IEEE 754 float32)

### Quality Metrics

Every GOLD product shipped with automated QA report:

```python
{
    "scene_id": 12345,
    "product_id": 67890,
    "acquisition_datetime": "2024-01-15T22:50:41Z",
    "band_name": "VV",
    "quality_score": 82.4,              # 0–100, composite metric
    "quality_flag": "PASS",              # PASS | FAIL | WARNING
    "total_pixels": 31_900_000,
    "valid_pixels": 31_580_000,          # After NoData mask
    "nodata_percent": 1.0,               # Typically << 5% for Sentinel-1
    "backscatter_mean_db": -12.37,       # Mean SAR backscatter
    "backscatter_std_db": 2.81,          # Std dev (speckle measure)
    "backscatter_min_db": -28.44,
    "backscatter_max_db": 1.92,
    "speckle_index": 0.227,              # Std / |Mean| (normalized noise)
    "radiometric_consistency": true,     # Within calibration spec
    "notes": "Good acquisition, low cloud. Ready for inference."
}
```

**QA Scoring Formula**:
```
QA_score = (50 × valid_ratio) + (30 × (1 – speckle_index)) + (20 × radiometric_ok)
```

**Pass Thresholds**:
- Valid pixels ≥ 70%
- Speckle index ≤ 0.4
- Radiometric consistency verified
- Backscatter mean within [-35, +5] dB (Sentinel-1 IW mode spec)

---

## 4. API & Data Access Layer

### REST API Endpoints

#### List Datasets
```http
GET /api/datasets?limit=50&offset=0&include_deleted=false
```
Returns: pagination of user's dataset definitions (metadata only, not scene data).

#### Create Dataset
```http
POST /api/datasets
Content-Type: application/json

{
  "location": "Jakarta" | bbox_wkt,
  "date_start": "2024-01-01",
  "date_end": "2024-12-31",
  "tiers": ["GOLD"],                    # Minimal set for inference
  "name": "Flood Detection Jan-Dec 2024",
  "quality_settings": {
    "min_quality_score": 60,
    "min_cloud_cover": 20
  }
}
```
Returns: `{dataset_id, job_id, status: "QUEUED"}`

#### Download Dataset
```http
GET /api/datasets/{dataset_id}/download?format=zip
```
Streams .zip containing all GOLD COG files + metadata JSON.

#### Query Products (Advanced)
```http
GET /api/products?tier=GOLD&dataset_id={id}&status=valid&limit=100
```
Returns: Individual product records with file paths + quality metrics.

#### Get Scene Status
```http
GET /api/datasets/{dataset_id}/status
```
Returns: Detailed per-scene progress (current stage, % complete, error messages).

#### Live Ingestion Status
```http
GET /api/live
POST /api/live/toggle?enabled=true|false
POST /api/live/clear
POST /api/live/backfill
```

### Python SDK (Recommended for ML)

```python
from the_trinity import DatasetClient, QualityFilter

client = DatasetClient(base_url="https://api.example.com")

# Create dataset
ds = client.create_dataset(
    location="Jakarta",
    date_start="2024-01-01",
    date_end="2024-12-31",
    tiers=["GOLD"],
    name="Flood ML Training",
    quality_settings={
        "min_quality_score": 70,
        "min_cloud_cover": 15
    }
)

# Poll for completion
while ds.status in ["QUEUED", "PREPARING", "DOWNLOADING", "PROCESSING"]:
    ds.refresh()
    print(f"Progress: {ds.progress_percent}%")

# Download & iterate
for product in ds.get_products(tier="GOLD", band="VV"):
    if product.quality_flag == "PASS":
        data, metadata = product.load_as_array()  # Returns numpy float32
        # Train model...
```

### GeoDataFrame Integration (GeoPandas)

```python
import geopandas as gpd
from rasterio.open import open as rio_open

# List products as GeoDataFrame
gdf = client.to_geodataframe(dataset_id)
# gdf.columns: ['product_id', 'band_name', 'quality_score', 'geometry', 'file_path', ...]

# Filter by quality
high_quality = gdf[gdf['quality_score'] >= 70]

# Spatial filter
aoi = gpd.read_file('aoi.geojson')  # User's area of interest
intersects = gpd.sjoin(gdf, aoi, how='inner')

# Load COG with rasterio
for idx, row in intersects.iterrows():
    with rio_open(row['file_path']) as src:
        vv = src.read(1)  # Band 1 (VV)
        # Rasterio auto-handles COG overviews, no tile download overhead
```

### xarray/Dask for Time-Series Analysis

```python
import xarray as xr

# Create multi-temporal stack
ds = client.to_xarray_dataset(
    dataset_id,
    band='VV',
    chunks={'time': 5, 'y': 512, 'x': 512}  # Dask lazy loading
)

# ds.VV: (time=120, y=5500, x=5800) → dask array
# Compute anomalies without loading all into RAM
anomaly = (ds.VV - ds.VV.mean('time')) / ds.VV.std('time')
anomaly.to_netcdf('flood_anomaly.nc')
```

---

## 5. Data Lineage & Reproducibility

### Scene Lineage Chain

Every GOLD product documents full transformation:

```
RAW_VV (product_id=100, checksum=abc123...)
  ↓ [CROP] job_id=10, params: {bbox: [...], resampling: bilinear}
BRONZE_VV (product_id=101, checksum=def456...)
  ↓ [LEE_FILTER] job_id=11, params: {window_size: 7, looks: 1}
SILVER_VV (product_id=102, checksum=ghi789...)
  ↓ [COG_EXPORT] job_id=12, params: {compression: LZW, blocksize: 512}
GOLD_VV (product_id=103, checksum=jkl012...)
```

### Inspect Lineage

```python
# Get full chain for a GOLD product
lineage = client.get_lineage_chain(product_id=103, direction='ancestors')

for step in lineage:
    print(f"{step['transformation_type']}: {step['input_checksum'][:12]} → {step['output_checksum'][:12]}")
    print(f"  Job {step['job_id']}, Params: {step['transformation_params']}")
```

### Reproduce a Scene

Given lineage, re-run identical processing:

```python
# Extract parameters from lineage
lineage = client.get_lineage_chain(product_id=103)

# Re-run only SILVER tier with same params
for step in lineage:
    if step['transformation_type'] == 'LEE_FILTER':
        window_size = step['transformation_params']['window_size']
        looks = step['transformation_params']['looks']
        # Reapply: silver = apply_lee_filter(bronze, window_size, looks)
```

---

## 6. Live Ingestion & Continuous Learning

### Automated Daily Collection

For models requiring up-to-date training signals:

1. **Enable Live Mode** → Create "live" dataset (one per system)
2. **Scheduler checks** → Daily 02:00 Asia/Jakarta
3. **Multi-source ingest**:
   - **Sentinel-1**: Query last 7 days, download new scenes
   - **MODIS Flood**: Daily NRT product (250m resolution)
   - **GPM**: Daily rainfall aggregation (24h, 72h, 7-day)
4. **Auto-QA**: Scenes tagged PASS/FAIL, alerts on quality issues
5. **Auto-cleanup**: Old data deleted per retention policy

### Backfill Historical Data

```python
# Ingest past data for model re-training
client.trigger_backfill(
    date_start="2023-01-01",
    date_end="2023-12-31",
    sources=['SENTINEL1', 'MODIS', 'GPM']
)
```

---

## 7. Storage Optimization & Retention Policy

### Tier Auto-Deletion

Based on user's `required_tiers` selection:

| Scenario | RAW | BRONZE | SILVER | GOLD | PREVIEW |
|----------|-----|--------|--------|------|---------|
| User requests only GOLD | ✗ deleted | ✗ deleted | ✗ deleted | ✓ kept | ✓ kept |
| User requests only FUSION | ✗ deleted | ✗ deleted | ✗ deleted | ✗ deleted | ✓ kept |
| User requests SILVER + GOLD | ✗ deleted | ✗ deleted | ✓ kept | ✓ kept | ✓ kept |
| User requests all (research) | ✓ kept | ✓ kept | ✓ kept | ✓ kept | ✓ kept |

PREVIEW is never auto-deleted: it is not in `TIER_ORDER`, so
`compute_tiers_to_delete` never sees it. That is deliberate — on a FUSION-only
dataset `gold/` is gone, so the PNGs cannot be rebuilt and are the only visual
record left. Delete them explicitly with
`POST /api/storage/cleanup {"tier": "preview"}` if the disk cost matters.

**Cleanup timing**: after the whole per-scene pipeline finishes (GOLD_EXPORT →
PREVIEW → FUSION), so every stage still sees the tiers it reads.

### Cost Example (1 year, 365 scenes)

| Retention Profile | Storage | Annual Cost @ $0.023/GB/month |
|-------------------|---------|-------------------------------|
| GOLD only | 15 GB | $4.14 |
| SILVER + GOLD | 32 GB | $8.83 |
| All tiers | 71 GB | $19.53 |

---

## 8. Quality Assurance & Alerting

### Automated QA Pipeline

Every scene processed through:

1. **Radiometric check**: Backscatter mean within calibration spec (±2 dB tolerance)
2. **Geometric check**: Scene bounds overlap Jabodetabek, pixel registration verified
3. **Speckle assessment**: Std/Mean ratio quantifies noise (target < 0.3)
4. **Cloud impact**: Sentinel-1 immune, MODIS/optical checked separately
5. **Nodata audit**: Flag if > 30% pixels invalid

### Alert Events

| Event Type | Trigger | Action |
|-----------|---------|--------|
| DATA_ARRIVAL | Scene processed to GOLD | Log entry, webhook (optional) |
| QUALITY_WARNING | QA score 50–60 (borderline PASS) | Alert to dashboard, log |
| QUALITY_FAIL | QA score < 50 | Alert + flag product, don't auto-train |
| PIPELINE_ERROR | Job timeout or crash | Alert ops, retry job |
| THRESHOLD_BREACH | Anomalous backscatter (flood detection) | Alert + send to inference pipeline |

### Consume Alerts (Webhook)

```json
POST your_webhook_url

{
  "alert_id": "alert_12345",
  "event_type": "QUALITY_WARNING",
  "severity": "WARNING",
  "scene_id": 9876,
  "title": "Scene 9876 QA score = 52.1 (borderline)",
  "message": "Nodata 28%, speckle_index 0.39 (close to threshold)",
  "metadata": {
    "quality_score": 52.1,
    "product_id": 102345,
    "band": "VV",
    "triggered_at": "2024-01-15T08:30:00Z"
  }
}
```

---

## 9. Scalability & Performance Targets

### Throughput

- **Single machine**: 3–5 scenes/day (32 GB RAM, 8-core CPU, SATA SSD)
- **Cluster (3 machines)**: 10–15 scenes/day (shared PostgreSQL database)
- **Bottleneck**: Network (Copernicus CDSE download) or disk I/O (SILVER → GOLD COG export)

### Latency

| Stage | Time | Variability |
|-------|------|-------------|
| Download | 5–15 min | Network speed |
| CROP | 1–2 min | Geometry complexity |
| LEE_FILTER | 3–8 min | CPU cores, RAM |
| COG_EXPORT | 2–5 min | Compression, tile count |
| QA Analytics | 30–60 sec | Data size |
| **Total per scene** | **12–30 min** | Network-dependent |

### Database Capacity

- **Single PostgreSQL**: ≤ 50,000 scenes (before query slowdown)
- **With TimescaleDB**: ≥ 500,000 scenes (time-series optimized)
- **Recommended**: Migrate to TimescaleDB at 20k scenes

---

## 10. Integration Examples for ML/DL

### Flood Detection Classifier (PyTorch)

```python
import torch
from the_trinity import DatasetClient

client = DatasetClient()
dataset = client.get_dataset(dataset_id)

# Load GOLD products into DataLoader
class SentinelDataset(torch.utils.data.Dataset):
    def __init__(self, products, transform=None):
        self.products = products
        self.transform = transform
    
    def __getitem__(self, idx):
        product = self.products[idx]
        vv, vh = product.load_bands(['VV', 'VH'])  # numpy arrays
        
        # Stack & normalize
        x = np.stack([vv, vh], axis=0).astype('float32')
        x = (x + 35) / 40  # Normalize to [0, 1] given range [-35, +5] dB
        
        if self.transform:
            x = self.transform(x)
        
        # Label: 1 if any quality alert OR manual label, 0 otherwise
        y = 1 if product.quality_flag == "FAIL" else 0
        return torch.from_numpy(x), y

products = dataset.get_products(tier='GOLD', quality_flag='PASS')
dataset_torch = SentinelDataset(products)
loader = torch.utils.data.DataLoader(dataset_torch, batch_size=16)

# Train model...
```

### Real-Time Inference (FastAPI)

```python
from fastapi import FastAPI
from the_trinity import DatasetClient
import rasterio
import numpy as np

app = FastAPI()
client = DatasetClient()
model = load_model('flood_classifier.pt')  # Trained offline
live_dataset = client.get_dataset(dataset_id=1)  # Live ingestion dataset

@app.get("/predict-latest")
async def predict_latest():
    products = live_dataset.get_products(
        tier='GOLD',
        order_by='acquisition_datetime desc',
        limit=1
    )
    if not products:
        return {"error": "No recent data"}
    
    product = products[0]
    vv, vh = product.load_bands(['VV', 'VH'])
    x = np.stack([vv, vh], axis=0)[np.newaxis, :, :, :]  # Batch dim
    
    with torch.no_grad():
        flood_prob = model(torch.from_numpy(x)).sigmoid().item()
    
    return {
        "scene_id": product.scene_id,
        "acquisition": product.acquisition_datetime,
        "flood_probability": flood_prob,
        "recommended_action": "EVACUATE" if flood_prob > 0.7 else "MONITOR"
    }
```

---

## 11. Compliance & Governance

### Data Provenance

Every GOLD file includes provenance metadata:

```bash
gdalinfo S1A_20240115_VV_cog.tif | grep -A 10 'Band 1'
# OUTPUT:
#   Band 1 Block=512x512 Type=Float32
#   Overviews: 2752x2900, 1376x1450, 688x725, 344x363
#   Metadata:
#     PROCESSING_DATE=2024-01-15T23:15:00Z
#     SOURCE_PRODUCT_ID=S1A_IW_GRDH_1SDV_20240115T225041_20240115T225106_052186_064F3A_B5C2
#     LINEAGE_ID=S1A-CROP-LEE-COG-12
#     DATA_HASH=abc123def456...
```

### Quality Audit Trail

Access full processing log for any scene:

```python
lineage = client.get_lineage_chain(product_id=103)
lineage_to_csv(lineage, 'scene_103_audit.csv')
# Columns: lineage_id, stage, job_id, input_hash, output_hash, duration_sec, status, error_msg
```

### Reproducibility Guarantee

Given same input scene + same processing params → **byte-identical output**
(due to deterministic COG export, LZW compression, same GdalInfo version)

---

## 12. Success Metrics & KPIs

| KPI | Target | Current |
|-----|--------|---------|
| Data availability | 95% of scenes GOLD ready within 24h | 94% |
| QA pass rate | ≥ 90% of ingested scenes | 88% |
| Latency (download to GOLD) | ≤ 30 min | avg 22 min |
| API uptime | 99.5% | 99.7% |
| Storage cost per scene (GOLD) | ≤ $0.01 | $0.009 |
| Model retraining cadence | Weekly (optional daily) | manual today |

---

## 13. Roadmap (Q1-Q4 2024)

- **Q1**: MVP (Sentinel-1 only, static dataset creation)
- **Q2**: Live ingestion (daily auto-refresh), MODIS + GPM integration
- **Q3**: Multi-modal fusion (HDF5 stacks for DL), Python SDK
- **Q4**: Inference API + webhook alerting, TimescaleDB scale-out

---

## 14. Appendices

### A. Supported AOIs

Currently: **Jabodetabek (106.4°–107.2°E, 5.9°–6.7°S)**

Future: Will support dynamic bbox input, separate configs per region.

### B. Dependencies for ML

```
numpy>=1.21
rasterio>=1.3
geopandas>=0.13
xarray>=2023.01
dask>=2023.01
torch>=2.0  # Optional, for inference
fastapi>=0.100  # Optional, for API serving
```

### C. Glossary

- **COG**: Cloud-Optimized GeoTIFF → tiled, compressed, with overviews
- **SAR**: Synthetic Aperture Radar → active microwave sensor (Sentinel-1)
- **Backscatter**: Radar energy reflected back to sensor (measured in dB)
- **Sigma-0 (σ⁰)**: Radar cross-section per unit area (standard calibration metric)
- **IW mode**: Interferometric Wide swath (Sentinel-1 default, 250 km footprint)
- **Speckle**: Noise inherent to SAR (reduced by filtering, increased by averaging)

---

**Document Version**: 1.0  
**Last Updated**: January 2024  
**Author**: Julius Marselinus (The Trinity Core Team)  
**Contact**: support@example.com
