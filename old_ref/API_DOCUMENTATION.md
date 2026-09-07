# API DOCUMENTATION
## Sentinel-1 Flood Detection Data Pipeline

**Base URL:** `http://localhost:8000`  
**Interactive Docs:** `/docs` (Swagger UI) · `/redoc` (ReDoc)  
**OpenAPI Spec:** `/openapi.json`

---

## Authentication

Currently no authentication required (research/academic deployment). API key support is scaffolded via `api_key_id` in `api_access_logs` for future production use.

---

## Endpoints

### Health

#### `GET /api/health`

Returns database connectivity and connection pool statistics.

**Response 200:**
```json
{
  "status": "ok",
  "db_connected": true,
  "pool_size": 5,
  "checked_out": 1,
  "api_version": "1.0.0",
  "timestamp": "2024-01-15T22:50:41Z"
}
```

---

### Scenes

#### `GET /api/scenes`

List Sentinel-1 scenes with optional filters.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `region_id` | int | — | Filter by AOI region |
| `orbit_direction` | string | — | `ASCENDING` or `DESCENDING` |
| `date_from` | datetime | — | Acquisition from (ISO 8601 UTC) |
| `date_to` | datetime | — | Acquisition to (ISO 8601 UTC) |
| `only_gold` | bool | `false` | Only scenes with GOLD product |
| `limit` | int | `20` | Max results (1–200) |
| `offset` | int | `0` | Pagination offset |

**Response 200:**
```json
{
  "total": 47,
  "limit": 20,
  "offset": 0,
  "items": [
    {
      "scene_id": 1,
      "scene_uuid": "a1b2c3d4-...",
      "product_identifier": "S1A_IW_GRDH_1SDV_20240115T225041_...",
      "platform": "SENTINEL-1",
      "instrument_mode": "IW",
      "polarization_vv": true,
      "polarization_vh": true,
      "acquisition_datetime": "2024-01-15T22:50:41Z",
      "orbit_direction": "ASCENDING",
      "orbit_number": 52186,
      "relative_orbit": 98,
      "cloud_cover_percent": 12.5,
      "resolution_m": 10,
      "region_id": 1,
      "is_available": true,
      "created_at": "2024-01-16T00:10:00Z"
    }
  ]
}
```

---

#### `GET /api/scenes/{scene_id}`

Full metadata for a single scene.

**Response 200:** Same as list item plus:
```json
{
  "raw_file_path": "/data/raw/S1A_IW_GRDH_20240115.zip",
  "raw_file_size_mb": 847.3,
  "download_url": "https://scihub.copernicus.eu/...",
  "checksum_md5": "d41d8cd98f00b204...",
  "incidence_angle_near": 30.8,
  "incidence_angle_far": 46.2,
  "updated_at": "2024-01-16T01:00:00Z"
}
```

**Response 404:** `{"detail": "Scene 99 not found"}`

---

#### `GET /api/scenes/{scene_id}/status`

ETL pipeline execution status per stage.

**Response 200:**
```json
{
  "scene_id": 1,
  "overall_status": "COMPLETE",
  "stages": [
    {
      "job_id": 1,
      "stage_name": "DOWNLOAD",
      "stage_order": 1,
      "attempt_number": 1,
      "status": "SUCCESS",
      "queued_at": "2024-01-15T23:00:00Z",
      "started_at": "2024-01-15T23:00:05Z",
      "completed_at": "2024-01-15T23:08:30Z",
      "error_message": null
    }
  ]
}
```

`overall_status` values: `COMPLETE` · `IN_PROGRESS` · `FAILED` · `NOT_STARTED`

---

### Products

#### `GET /api/products`

List output data products.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scene_id` | int | — | Filter by parent scene |
| `tier` | string | — | `RAW` · `BRONZE` · `SILVER` · `GOLD` |
| `band_name` | string | — | `VV` · `VH` · `VV_VH` |
| `latest_only` | bool | `true` | Only `is_latest=TRUE` products |
| `valid_only` | bool | `true` | Only `is_valid=TRUE` products |
| `limit` | int | `20` | 1–200 |
| `offset` | int | `0` | Pagination offset |

**Response 200:**
```json
{
  "total": 8,
  "limit": 20,
  "offset": 0,
  "items": [
    {
      "product_id": 7,
      "product_uuid": "b2c3d4e5-...",
      "scene_id": 1,
      "job_id": 4,
      "product_tier": "GOLD",
      "product_type": "COG",
      "band_name": "VV",
      "file_name": "S1A_20240115_VV_cog.tif",
      "file_path": "/processed/gold/1/S1A_20240115_VV_cog.tif",
      "file_size_mb": 41.2,
      "file_format": "COG",
      "data_hash_sha256": "e3b0c44298fc1c149...",
      "crs": "EPSG:4326",
      "pixel_size_m": 10.0,
      "rows": 5500,
      "cols": 5800,
      "band_count": 1,
      "storage_location": "LOCAL",
      "is_valid": true,
      "is_latest": true,
      "created_at": "2024-01-16T00:45:00Z"
    }
  ]
}
```

---

#### `GET /api/products/{product_id}/download`

Stream the product file for download.

**Response 200:** Binary file stream (`Content-Type: image/tiff`)  
**Response 404:** File not found on disk (may be stored remotely)

---

#### `GET /api/products/{product_id}/verify`

Recompute SHA-256 and compare against stored hash.

**Response 200:**
```json
{
  "product_id": 7,
  "file_path": "/processed/gold/1/S1A_20240115_VV_cog.tif",
  "stored_hash": "e3b0c44298fc1c149...",
  "computed_hash": "e3b0c44298fc1c149...",
  "integrity_ok": true,
  "file_size_mb": 41.2
}
```

---

### Quality

#### `GET /api/quality/{scene_id}`

Radiometric quality metrics for all bands of a scene.

**Response 200:**
```json
{
  "scene_id": 1,
  "overall_quality": "PASS",
  "bands": [
    {
      "metric_id": 1,
      "scene_id": 1,
      "product_id": 7,
      "band_name": "VV",
      "assessed_at": "2024-01-16T01:00:00Z",
      "total_pixels": 31900000,
      "valid_pixels": 31580000,
      "nodata_pixels": 320000,
      "nodata_percent": 1.00,
      "backscatter_mean_db": -12.37,
      "backscatter_std_db": 2.81,
      "backscatter_min_db": -28.44,
      "backscatter_max_db": 1.92,
      "cloud_threshold_percent": 20.0,
      "radiometric_consistency": true,
      "speckle_index": 0.227,
      "quality_score": 82.4,
      "quality_flag": "PASS",
      "notes": "Good quality acquisition.",
      "created_at": "2024-01-16T01:00:00Z"
    }
  ]
}
```

`overall_quality`: `PASS` (all bands pass) · `FAIL` (any band fails) · `WARNING`

---

#### `GET /api/quality/summary/stats`

Aggregated quality statistics across scenes.

**Query Parameters:** `n_days` (int, default 30), `region_id` (int, optional)

**Response 200:**
```json
{
  "period_days": 30,
  "region_id": null,
  "flags": {
    "PASS":    {"count": 42, "avg_score": 81.3},
    "FAIL":    {"count": 3,  "avg_score": 38.7},
    "WARNING": {"count": 5,  "avg_score": 57.2}
  }
}
```

---

### Lineage

#### `GET /api/metadata/lineage/{product_id}`

Full provenance chain for a data product.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `direction` | string | `ancestors` | `ancestors` = trace to source · `descendants` = trace forward |

**Response 200:**
```json
{
  "product_id": 7,
  "direction": "ancestors",
  "total_steps": 3,
  "chain": [
    {
      "lineage_id": 1,
      "parent_product_id": 1,
      "child_product_id": 3,
      "transformation_type": "CROP",
      "stage_id": 2,
      "job_id": 2,
      "transformation_params": {"bbox": "JABODETABEK", "resampling": "bilinear"},
      "input_checksum": "abc123...",
      "output_checksum": "def456...",
      "created_at": "2024-01-16T00:20:00Z"
    },
    {
      "lineage_id": 2,
      "parent_product_id": 3,
      "child_product_id": 5,
      "transformation_type": "LEE_FILTER",
      "transformation_params": {"window_size": 7, "looks": 1, "sigma": 0.9}
    },
    {
      "lineage_id": 3,
      "parent_product_id": 5,
      "child_product_id": 7,
      "transformation_type": "COG_EXPORT",
      "transformation_params": {"compression": "LZW", "blocksize": 512}
    }
  ]
}
```

---

## Error Codes

| HTTP Status | Meaning |
|-------------|---------|
| `200` | Success |
| `400` | Bad request (invalid parameter value) |
| `404` | Resource not found |
| `422` | Validation error (invalid parameter type/format) |
| `500` | Internal server error |

All error responses follow:
```json
{"detail": "Human-readable error description"}
```

---

## Pagination

All list endpoints support `limit` (max 200) and `offset` pagination. Response always includes `total`, `limit`, and `offset` for client-side page calculation:

```python
# Example: fetch all scenes in pages of 50
offset = 0
limit  = 50
while True:
    resp = requests.get(f"/api/scenes?limit={limit}&offset={offset}")
    data = resp.json()
    process(data["items"])
    if offset + limit >= data["total"]:
        break
    offset += limit
```
