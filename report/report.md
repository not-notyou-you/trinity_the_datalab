# Trinity DataLab: Dataset Report Generation Specification

## Overview
The Dataset Report feature generates comprehensive PDF reports that document the complete lifecycle, configuration, quality, and characteristics of datasets within Trinity DataLab. Each report is tailored to the specific satellite sources (Sentinel-1, MODIS, GPM) and processing configurations used in the dataset.

## Report Architecture

### Report Structure (10 Sections)
The PDF report consists of the following sections, each serving a specific analytical and documentation purpose:

1. **Cover Page & Executive Summary** (1 page)
2. **Configuration & Data Ingestion Summary** (1-2 pages)
3. **Processing Level Comparison** (1-2 pages)
4. **Fusion Strategy & Temporal Alignment** (1 page)
5. **Overall Trends & Patterns** (2-3 pages)
6. **Sentinel-1 Deep Dive** (3-4 pages)
7. **MODIS Deep Dive** (2-3 pages)
8. **GPM Deep Dive** (1-2 pages)
9. **Data Quality & Warnings** (1 page)
10. **JSON Summary & Machine-Readable Export** (appendix)

---

## Section Details

### Section 1: Cover Page & Executive Summary

**Content:**
- Dataset name (title)
- Creation date
- User/organization
- Brief summary (2-3 sentences) of dataset purpose and key metrics
- Quick statistics:
  - Total records ingested
  - Date range covered
  - Processing levels included
  - Spatial coverage (bounding box or region)
  - Temporal completeness (%)

**Example:**
```
TRINITY DATALAB REPORT
Dataset: "Monsoon Region Soil Moisture Analysis"
Generated: 2026-09-25

Executive Summary
This dataset combines Sentinel-1 SAR backscatter, MODIS Land Surface Temperature, 
and GPM precipitation estimates over the South Asian monsoon region (15°N–35°N, 
60°E–100°E) from 2023-01-01 to 2026-09-25. The dataset serves as the foundation 
for regional soil moisture modeling with 73,450 records across three processing levels.

Key Metrics:
• Total Records: 73,450
• Date Range: 2023-01-01 to 2026-09-25 (1,363 days)
• Spatial Coverage: 15°N–35°N, 60°E–100°E
• Processing Levels: RAW, L1A, L2A
• Temporal Completeness: 87.3%
```

---

### Section 2: Configuration & Data Ingestion Summary

**Content:**
- Dataset metadata (name, purpose, user, dates created/updated)
- Data sources activated and their configurations:
  - **Sentinel-1:**
    - Processing levels selected (RAW/PROCESSED)
    - Orbit modes (ascending/descending)
    - Polarization bands (VV/VH)
  - **MODIS:**
    - Processing levels selected
    - Product types (e.g., MOD11A2 for LST)
    - Quality filters applied
  - **GPM:**
    - Processing levels selected
    - Algorithm version
    - Precipitation rate thresholds
- Spatial bounds (lat/lon box or named region)
- Temporal range and ingestion timeline
- Total files ingested per source

**Table Example:**
```
| Source | Processing Levels | Records Ingested | Date Range | Status |
|--------|-------------------|------------------|------------|--------|
| Sentinel-1 | RAW, L1A | 24,560 | 2023-01-01 to 2026-09-25 | ✓ Complete |
| MODIS | RAW, L2A | 31,200 | 2023-01-01 to 2026-09-25 | ✓ Complete |
| GPM | RAW, L1B | 17,690 | 2023-01-01 to 2026-09-25 | ✓ Complete |
| **Total** | — | **73,450** | — | — |
```

---

### Section 3: Processing Level Comparison

**Content:**
- Ablation study comparing processing levels for each satellite source
- For each source, show:
  - Data completeness (%) at each level
  - Mean file size per record
  - Processing time per level
  - Data loss/filtering between levels
  - Quality improvements achieved

**Visualization:**
- Bar chart: Record count by processing level and source
- Line chart: Data completeness (%) across processing levels
- Table: Processing statistics (time, size, loss)

**Example Content:**
```
### Sentinel-1 Processing Ablation

Processing progression from RAW → L1A → L2A shows increasing data refinement:

**RAW → L1A Transition:**
- Records preserved: 24,560 → 24,348 (98.9%)
- Mean file size: 2.1 MB → 1.8 MB (14.3% reduction via compression)
- Processing quality: Removed 212 corrupted frames and 1 geolocation error
- Median processing time: 45 seconds per scene

**L1A → L2A Transition:**
- Records preserved: 24,348 → 24,092 (98.9%)
- Mean file size: 1.8 MB → 0.9 MB (50% reduction via speckle filtering)
- Processing enhancements: Applied refined calibration, orthorectified to 10m grid
- Median processing time: 120 seconds per scene

**Quality Improvements:**
- Radiometric accuracy: +2.3 dB signal-to-noise ratio improvement in L2A
- Geometric accuracy: ±15m in L1A → ±5m in L2A
- Speckle reduction: Improved edge preservation via dual-pol filtering
```

**Chart 1: Record Completeness by Level**
```
Sentinel-1 Data Completeness
100% |████████████
 98% |████████████  ████████████
 96% |████████████  ████████████  ████████████
     |    RAW          L1A           L2A
```

---

### Section 4: Fusion Strategy & Temporal Alignment

**Content:**
- Fusion methodology (if multiple sources are combined)
- Temporal alignment strategy:
  - Interpolation method (linear, cubic spline, nearest-neighbor)
  - Maximum temporal offset tolerance
  - Gap-filling approach for missing data
- Spatial resampling approach
- Fusion weights or priorities assigned to each source
- Examples of aligned records (1-2 sample timestamps with aligned data from all sources)

**Example:**
```
### Temporal Alignment Strategy

With three independent satellite systems acquiring data on different schedules, 
temporal alignment is critical. The following strategy ensures data consistency:

**Alignment Timeline (Sample Date: 2026-05-15):**

| Time (UTC) | Sentinel-1 | MODIS | GPM | Strategy |
|-----------|-----------|-------|-----|----------|
| 06:00 | ✓ Pass | — | ✓ Observation | S1 at 6:00, MODIS gap-filled via L1A (5-day composite), GPM at 6:00 |
| 12:00 | — | ✓ Observation | ✓ Observation | MODIS at 12:00, S1 gap-filled via interpolation, GPM at 12:00 |
| 18:00 | ✓ Pass | — | — | S1 at 18:00, MODIS/GPM gap-filled |

**Alignment Method:**
- Temporal window: ±2 hours for multi-source records
- Interpolation: Cubic spline for MODIS between 5-day observations
- Gap-filling: Forward-fill up to 10 days; beyond that, exclude record
- Confidence flag: Added to each fused record indicating source availability
```

---

### Section 5: Overall Trends & Patterns

**Content:**
- Time-series trends across the entire dataset
- Seasonal patterns identified
- Anomalies or data quality issues
- Key findings summarized from all sources

**Visualizations (Matplotlib/Plotly):**
1. **Time-series plot:** One metric per source (e.g., mean VV backscatter for S1, mean LST for MODIS, mean precipitation for GPM) over full temporal range
2. **Seasonal heatmap:** Monthly average values for each source
3. **Completeness timeline:** Percentage of records available per week
4. **Anomaly scatter:** Flagged outliers or quality warnings over time

**Example Content:**
```
### Temporal Trends (2023-01-01 to 2026-09-25)

**Sentinel-1 VV Backscatter Trend:**
Mean backscatter shows seasonal modulation with peak values during monsoon 
season (June-September) and minima during winter (December-February). 
Average values: winter -12.3 dB, summer -8.7 dB.

**MODIS Land Surface Temperature:**
Clear annual cycle with summer maxima (45–52°C) and winter minima (8–15°C). 
No significant long-term warming trend detected (slope: +0.02°C/year).

**GPM Precipitation:**
Heavy monsoon precipitation evident June-September (monthly mean: 200–300 mm). 
Dry season precipitation minimal (December-May: 10–30 mm/month).

**Data Completeness:**
Overall dataset completeness: 87.3% (temporal coverage without gaps exceeding 10 days).
Sentinel-1: 95.2%, MODIS: 89.1%, GPM: 77.4%
```

---

### Section 6: Sentinel-1 Deep Dive

**Content:**
- Detailed analysis of Sentinel-1 SAR data
- Orbit coverage: ascending vs. descending passes
- Polarization analysis (VV, VH, VV/VH ratio trends)
- Incidence angle statistics
- Scene previews with timeline visualization
- Data quality metrics specific to SAR (speckle, phase coherence)
- Notable acquisitions or gaps

**Scene Preview Timeline Visualization:**
A visual timeline showing acquisition density and coverage over the study period.

**Example:**
```
### Sentinel-1 Scene Preview Timeline

Timeline of Sentinel-1 acquisitions (2023-01-01 to 2026-09-25):

2023 |▓▓▓▓▓▓▓▓▓▓▓▓ Monsoon Dense ▓▓▓▓▓▓▓▓▓▓▓▓|
2024 |▓▓▓▓▓▓░░░░░░░░▓▓▓▓▓▓▓▓▓░░░░░░|
2025 |▓▓▓▓▓▓▓▓▓▓▓░░░░░▓▓▓▓▓▓▓▓▓▓▓▓|
2026 |▓▓▓▓▓▓▓▓░░░░░░░░░▓▓▓▓▓▓▓▓▓|

Legend: ▓ = Acquisition, ░ = Gap (>3 days)

**Acquisition Statistics:**
- Ascending passes: 12,340 scenes
- Descending passes: 12,220 scenes
- Mean VV backscatter: -10.5 dB (std: 2.3 dB)
- Mean VV/VH ratio: 2.8 dB
- Incidence angle range: 29.0° to 46.0°
- Phase coherence (L1A): mean 0.64, indicating good interferometric quality
```

**Sentinel-1 Data Quality:**
- Thermal noise floor: -22 dB (nominal)
- Radiometric stability: σ = 0.5 dB (excellent)
- Geolocation accuracy: ±5 m (post-orthorectification)
- Notable data artifacts: 3 scenes with minor phase wrapping in mountainous terrain

---

### Section 7: MODIS Deep Dive

**Content:**
- Analysis of MODIS products used (MOD11, MOD09, etc.)
- Collection version and processing baseline
- Cloud cover statistics and impact on data completeness
- Land surface temperature (LST) range and variability
- Vegetation indices (if applicable)
- Data quality flags and QC issues
- Comparison to validation datasets (if available)

**Example:**
```
### MODIS Land Surface Temperature Analysis

**Product:** MOD11A2 (8-day LST composites, 1 km resolution)
Collection: 6.1, Processing baseline: Jan 2023 onwards

**Temperature Statistics:**
- Mean annual LST: 28.3°C
- Summer maximum: 52.1°C (peak in May, 35.2°N, 78.5°E)
- Winter minimum: 8.7°C (peak in January, 18.3°N, 72.1°E)
- Spatial std dev: 4.2°C (high variability due to topography and land use)

**Data Quality:**
- Cloud-free observations: 67.2% (limited by monsoon cloud cover June-September)
- QC Flag distribution:
  - Good quality (0): 61.4%
  - Acceptable quality (1): 19.8%
  - Marginal quality (2): 13.2%
  - Poor/missing (3): 5.6%

**Seasonal Patterns:**
- Clear diurnal and seasonal cycles
- Monsoon season: Cooler due to cloud cover, mean 24.5°C
- Post-monsoon (Oct-Nov): Rapid heating, mean 35.7°C
- Winter (Dec-Feb): Coolest, mean 16.2°C
- Pre-monsoon (Mar-May): Hottest, mean 42.1°C
```

---

### Section 8: GPM Deep Dive

**Content:**
- Analysis of Global Precipitation Measurement (GPM) data
- Algorithm version and data latency
- Precipitation statistics (mean, median, max, distribution)
- Detection frequency and coverage
- Spatial variability of precipitation
- Seasonal precipitation patterns
- Extreme events detected

**Example:**
```
### GPM Precipitation Analysis

**Product:** IMERG Final Run (0.1° × 0.1°, 30-min resolution)
Version: v06B, Latency: ~3.5 months post-observation

**Precipitation Statistics (Full Dataset Period):**
- Mean daily precipitation: 2.3 mm/day
- Median daily precipitation: 0 mm/day (60% of observations zero)
- 95th percentile: 15.8 mm/day
- Maximum observed: 87.3 mm/day (2026-07-18, monsoonal event)
- Precipitation days (>0.1 mm): 46.2% of all observations

**Seasonal Breakdown:**
| Season | Mean (mm/day) | Max (mm/day) | Frequency |
|--------|---------------|--------------|-----------|
| Winter (Dec-Feb) | 0.4 | 12.1 | 8.3% |
| Pre-monsoon (Mar-May) | 1.8 | 34.5 | 22.1% |
| Monsoon (Jun-Sep) | 8.2 | 87.3 | 64.3% |
| Post-monsoon (Oct-Nov) | 2.1 | 28.7 | 24.8% |

**Extreme Precipitation Events:**
- Top 5 24-hour totals detected:
  1. 2026-07-18: 87.3 mm (Southwest monsoon peak)
  2. 2025-08-22: 76.4 mm (Monsoonal event)
  3. 2024-09-15: 71.2 mm
  4. 2023-07-10: 65.8 mm
  5. 2024-06-30: 61.9 mm
```

---

### Section 9: Data Quality & Warnings

**Content:**
- Summary of data quality issues identified during report generation
- QC flag statistics for each source
- Missing data and gap analysis
- Geolocation/geometric errors
- Radiometric anomalies
- Recommendations for data use

**Example:**
```
### Data Quality Assessment

**Overall Data Health: 89/100 (Excellent)**

**Sentinel-1 Quality Issues:**
- 3 scenes with geolocation errors >10 m (flagged in records)
- 12 scenes with thermal noise >-20 dB (minimal impact)
- Phase coherence degradation in 5 alpine areas (documented)
- **Recommendation:** Use with caution in mountainous regions; consider disabling high-incidence-angle passes

**MODIS Quality Issues:**
- Cloud contamination: 32.8% of observations affected
- 124 scenes with QC flag = 3 (poor quality; recommended for exclusion)
- LST uncertainty: ±1.5 K typical, ±2.5 K in mountainous areas
- **Recommendation:** Apply cloud masking; use only QC flags 0-1 for analysis requiring high confidence

**GPM Quality Issues:**
- 8 days with zero precipitation observations (instrument malfunction 2024-03-11 to 2024-03-18)
- Gauge adjustment available for 76.3% of grid cells
- Uncertainty: ±10% mean error (documented by IMERG team)
- **Recommendation:** Use caution during dry season (<0.5 mm/day); combine with ground stations if available

**Data Completeness by Month:**
(Line chart showing % of expected records received, highlighting gaps)
```

---

### Section 10: JSON Summary & Machine-Readable Export

**Content:**
- Complete dataset metadata in JSON format
- Structured configuration of all active sources
- Record counts and statistics
- Processing lineage
- Data quality metrics (machine-readable)
- Links to raw data and associated files

**Example JSON Structure:**
```json
{
  "dataset": {
    "id": "uuid-12345",
    "name": "Monsoon Region Soil Moisture Analysis",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2026-09-25T08:45:00Z",
    "owner": "user@example.com",
    "description": "Comprehensive dataset combining SAR, thermal, and precipitation data",
    "spatial_bounds": {
      "north": 35.0,
      "south": 15.0,
      "east": 100.0,
      "west": 60.0,
      "crs": "EPSG:4326"
    },
    "temporal_range": {
      "start": "2023-01-01",
      "end": "2026-09-25",
      "completeness_percent": 87.3
    }
  },
  "sources": {
    "sentinel_1": {
      "enabled": true,
      "processing_levels": ["RAW", "L1A", "L2A"],
      "records_ingested": 24560,
      "quality_metrics": {
        "mean_vv_backscatter_db": -10.5,
        "std_vv_backscatter_db": 2.3,
        "geolocation_accuracy_m": 5.0,
        "completeness_percent": 95.2
      }
    },
    "modis": {
      "enabled": true,
      "processing_levels": ["RAW", "L2A"],
      "records_ingested": 31200,
      "quality_metrics": {
        "mean_lst_celsius": 28.3,
        "cloud_free_percent": 67.2,
        "qc_good_percent": 61.4,
        "completeness_percent": 89.1
      }
    },
    "gpm": {
      "enabled": true,
      "processing_levels": ["RAW", "L1B"],
      "records_ingested": 17690,
      "quality_metrics": {
        "mean_precip_mm_day": 2.3,
        "max_precip_mm_day": 87.3,
        "completeness_percent": 77.4
      }
    }
  },
  "fusion_strategy": {
    "method": "temporal_alignment_with_interpolation",
    "temporal_window_hours": 2,
    "interpolation_method": "cubic_spline",
    "gap_fill_max_days": 10
  },
  "report_metadata": {
    "generated_at": "2026-09-25T12:30:00Z",
    "report_version": "1.0",
    "schema_version": "trinity_datalab_v1"
  }
}
```

---

## Implementation Notes

### Chart Generation
- **Tool:** Matplotlib or Plotly for interactive charts
- **Format:** PNG embedded in PDF via ReportLab or Weasyprint
- **Sizes:** 
  - Full-width charts: 6.5 inches (accounting for margins)
  - Side-by-side charts: 3.25 inches each
  - Font size: 10pt for labels, 12pt for titles
- **Color scheme:** Colorblind-friendly palette (avoid red-green)

### PDF Creation
- **Tool:** ReportLab (for programmatic table/chart layout) or Weasyprint (for HTML-based templating)
- **Page format:** A4 (8.27 × 11.69 inches)
- **Margins:** 0.75 inches
- **Header/Footer:** Page number, dataset name, generation date
- **Table of Contents:** Auto-generated by ReportLab

### Data Requirements
- **Database queries:** All summarized metrics must be computed from stored records
- **Chart data:** Aggregate statistics (mean, std, percentile values) pre-computed in backend
- **JSON export:** Serialized directly from database schema without formatting loss

### Async Report Generation Flow
```
1. User clicks "Generate Report" button (UI)
2. Backend enqueues async task: POST /api/datasets/generate-report
3. Task ID returned to frontend immediately
4. Celery/APScheduler worker picks up task
5. Worker queries database for dataset & all records
6. Worker computes statistics & generates charts
7. Worker assembles PDF using ReportLab/Weasyprint
8. Worker stores PDF in cloud storage (S3/GCS)
9. User polls GET /api/datasets/{id}/report/status/{job_id}
10. When complete, user downloads PDF via GET /api/datasets/{id}/report/download
```

### Error Handling
- Insufficient records: Display warning, generate abbreviated report
- Database query timeout: Queue for retry with extended timeout
- Chart generation failure: Omit chart, display text summary instead
- PDF assembly failure: Log error, return user-friendly message

### Phase 2 Enhancements (Post-MVP)
- Interactive Plotly charts in HTML report version
- 3D visualization of spatial patterns (if data permits)
- Custom report sections (user-selectable)
- Scheduled report generation and email delivery
- Multi-dataset comparison reports
