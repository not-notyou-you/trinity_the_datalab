# PROMPT REVISI: Perbaikan Report Generation - Detailing & Content Expansion

## Analisis Problem

Report saat ini adalah **skeleton/bare-bones** dengan struktur lengkap tapi konten minimal:
- ✅ Semua 10 sections ada
- ✅ Table of contents ok
- ✅ Basic charts ada
- ❌ Hanya 13 pages (harusnya 20-30)
- ❌ Konten setiap section 50-70% dari specification
- ❌ Tidak ada narrative/penjelasan detail
- ❌ Tabel-tabel terlalu sederhana
- ❌ Case studies hilang
- ❌ Data aggregation queries terlalu basic

---

## SECTION 2: Configuration & Data Ingestion Summary

**CURRENT:** ~1 page (minimal)  
**EXPECTED:** 2-3 pages (detail)

### Yang Kurang:

1. **Detailed Source Specifications** (baru ditambah)
   ```
   Untuk setiap source (S1, MODIS, GPM), buat subsection:
   
   SENTINEL-1 CONFIGURATION DETAIL
   Enabled: Yes
   Processing Levels: RAW, L1A, L2A
   Orbit Mode: Both Ascending & Descending
   Polarization Bands: VV, VH (dual-pol)
   Incidence Angle Range: 28.0° – 46.0°
   Spatial Resolution: 10m (L2A), 20m (L1A)
   Temporal Frequency: ~6 days
   Records in Dataset: [dari database]
   Ascending Passes: [query COUNT]
   Descending Passes: [query COUNT]
   
   → Query: count(product) where source='sentinel1' AND direction='ascending'
   ```

2. **Spatial Bounds Visualization** (sekarang hanya raw text)
   - Tambah ASCII art visualization seperti di spec:
   ```
       ┌────────────────────────────────────────┐
       │       SOUTH ASIAN MONSOON REGION       │
       │                                        │
       │     35°N ───────────────────────────   │
       │            ┌─────────────────────┐     │
       │     30°N   │  [Study Region]     │     │
       │            │                     │     │
       │     25°N   │  ★ Bayah (Center)  │     │
       │            │                     │     │
       │     20°N   │                     │     │
       │            │                     │     │
       │     15°N ──└─────────────────────┘──── │
       │                                        │
       │     60°E  70°E  80°E  90°E  100°E     │
       └────────────────────────────────────────┘
   ```

3. **Temporal Coverage Timeline** (ASCII art baru)
   - Format seperti spec:
   ```
   Timeline Visualization:
   
   2025: ▓▓▓▓▓▓▓▓░░░░░░░░░▓▓▓▓▓▓▓▓
   2026: ▓▓▓▓▓▓▓▓░░░░░░░░░▓▓▓▓▓▓▓▓
   
   Legend: ▓ = Observation, ░ = Gap
   ```
   - Query: group by month, count records, detect >10-day gaps

4. **Data Validation Summary Table** (baru)
   ```
   Validation Checks Performed:
   ✓ Geolocation bounds check: PASSED
   ✓ Temporal continuity: PASSED
   ✓ Metadata completeness: PASSED
   ✓ Data type validation: PASSED
   ✓ Radiometric range check: PASSED
   
   Quality Flag Distribution:
   - Good: X%
   - Acceptable: X%
   - Marginal: X%
   - Poor: X%
   ```

---

## SECTION 3: Processing Level Comparison & Ablation Study

**CURRENT:** ~1 page + 1 chart  
**EXPECTED:** 2-3 pages dengan detail ablation untuk tiap source

### Yang Kurang:

1. **Processing Level Flow Diagram** (untuk S1, MODIS, GPM)
   - Buat ASCII art untuk tiap source
   - Example S1:
   ```
   RAW DATA (L0)
       ↓ [Radiometric Calibration + Geometric Correction]
       ↓ [Processing Time: X sec/scene avg]
       ↓ [Data Loss: X% (corrupted frames)]
       ↓
   L1A DATA (Calibrated)
       ↓ [Speckle Filtering + Orthorectification]
       ↓ [Processing Time: X sec/scene avg]
       ↓ [Data Loss: X% (edge artifacts)]
       ↓
   L2A DATA (Analysis-Ready)
   ```

2. **Detailed Ablation Tables** untuk TIAP source
   ```
   SENTINEL-1 PROCESSING ABLATION
   
   ┌─────────────────────────┬─────────┬──────────┬──────────┐
   │ Processing Level        │ Records │ File Sz  │ Loss %   │
   ├─────────────────────────┼─────────┼──────────┼──────────┤
   │ RAW (L0)               │ [DB]    │ [calc]   │ —        │
   │ → L1A (Calibrated)     │ [DB]    │ [calc]   │ [%]      │
   │ → L2A (Analysis-Ready) │ [DB]    │ [calc]   │ [%]      │
   └─────────────────────────┴─────────┴──────────┴──────────┘
   
   Queries needed:
   - count(product) per processing_level
   - AVG(file_size_bytes) per processing_level
   - Calculate % loss between levels
   - Extract quality scores from metadata
   ```

3. **Quality Improvements Narrative**
   - Write text hasil perhitungan:
   ```
   Quality Improvements (L0 → L2A):
   • Radiometric accuracy: ±X dB → ±Y dB (+Z% improvement)
   • Geometric accuracy: ±X m → ±Y m (+Z% improvement)
   • SNR improvement: +X dB gained
   • Phase coherence: X → Y (Z% improvement)
   ```

4. **Combined Processing Effectiveness Chart** (baru)
   - Buat comparison chart untuk S1, MODIS, GPM side-by-side
   - Bar chart: File Size Reduction, Accuracy Improvement, Data Retention %

---

## SECTION 4: Fusion Strategy & Temporal Alignment

**CURRENT:** ~0.5 page (bullet points saja)  
**EXPECTED:** 1-2 pages dengan case studies & algorithm

### Yang Kurang:

1. **Fusion Algorithm Pseudocode** (baru)
   ```python
   def fuse_records(date, s1_data, modis_data, gpm_data):
       """
       Fuse three independent satellite observations.
       
       Logic:
         1. Search ±2 hours for all available observations
         2. If all three found → confidence = 'complete'
         3. If 2 of 3 found → confidence = 'partial'
         4. If only 1 found → confidence = 'single'
         5. If none within ±2 hours:
            - Search ±24 hours
            - If found → apply interpolation
            - If not → gap-fill backward/forward up to 10 days
         6. Return fused record with confidence flag
       """
   ```

2. **Case Study Examples** (TIGA case study detail)
   
   **Case Study 1: Optimal Alignment (All 3 Sources)**
   ```
   DATE: [dari database date dengan semua 3 sources]
   
   Source    │ Observation Time (UTC) │ Offset │ Data Quality
   ──────────┼───────────────────────┼────────┼──────────────
   Sentinel-1│ HH:MM:SS              │ ±X min │ ✓ OK
   MODIS     │ HH:MM:SS              │ ±X min │ ✓ OK
   GPM       │ HH:MM:SS              │ ±X min │ ✓ OK
   
   Fused Record JSON:
   {
     "timestamp": "...",
     "confidence": "complete",
     "sources": {
       "sentinel_1": {...},
       "modis": {...},
       "gpm": {...}
     }
   }
   ```

   **Case Study 2: Partial Alignment (2 of 3)**
   ```
   Gap-Fill Strategy for Missing Source:
   1. Search ±24 hours: Found observation at HH:MM:SS
   2. Time gap: X hours (exceeds ±2hr)
   3. Action: Apply cubic spline interpolation
   4. Interpolated value: X ± Y (with uncertainty)
   ```

   **Case Study 3: Single Source + Gap-Fill**
   ```
   Show example dengan forward-fill dan NULL values
   Include gap_fill_days, confidence flags, null_reasons
   ```

3. **Temporal Alignment Statistics Table** (query results)
   ```
   Records with all 3 sources: X (Y%)
   Records with 2 sources: X (Y%)
   Records with 1 source: X (Y%)
   
   Mean Temporal Offsets:
   - S1 vs MODIS: ±X min (σ=X min)
   - S1 vs GPM: ±X min (σ=X min)
   - MODIS vs GPM: ±X min (σ=X min)
   
   Gap-Fill Summary:
   - Records requiring interpolation: X (Y%)
   - Mean interpolation gap: X hours
   - Maximum gap filled: X hours
   ```

---

## SECTION 5: Overall Trends & Patterns

**CURRENT:** 1 chart saja  
**EXPECTED:** 2-3 pages dengan multiple charts & narrative

### Yang Kurang:

1. **THREE Time-Series Charts** (bukan 1)
   - **Chart 1:** Sentinel-1 VV backscatter trend
   - **Chart 2:** MODIS LST trend  
   - **Chart 3:** GPM precipitation trend (bar chart)
   - Semua dengan seasonal bands/annotations

2. **Narrative Analysis untuk tiap source**
   ```
   SENTINEL-1 BACKSCATTER TRENDS
   
   Seasonal Pattern Detected:
   • Winter minima (Dec-Feb): mean -X dB (σ=X dB)
   • Pre-monsoon (Mar-May): mean -X dB (σ=X dB)
   • Monsoon maxima (Jun-Sep): mean -X dB (σ=X dB)
   • Post-monsoon (Oct-Nov): mean -X dB (σ=X dB)
   
   Annual Cycle Amplitude: X dB
   Long-term Trend: ±X dB/year
   
   Physical Interpretation:
   [Write narrative hasil analisis data]
   ```

3. **Seasonal Heatmap** (temperature/precipitation per bulan)

4. **Key Findings Section** (bullets)
   ```
   1. STRONG MONSOON SIGNAL - [explanation]
   2. NO SIGNIFICANT LONG-TERM TRENDS - [explanation]
   3. HIGH SPATIAL HETEROGENEITY - [explanation]
   4. DATA COMPLETENESS ADEQUATE - [explanation]
   5. FUSION SUCCESS RATE - [explanation]
   ```

---

## SECTION 6: Sentinel-1 Deep Dive

**CURRENT:** ~1 page (basic stats + 1 image)  
**EXPECTED:** 3-4 pages (detailed analysis)

### Yang Kurang:

1. **VV/VH Polarization Comparison Table**
   ```
   VV BACKSCATTER Characteristics:
   • Mean: [dari DB] dB
   • Range: [min] to [max] dB
   • Sensitivity: [narrative]
   
   VH BACKSCATTER Characteristics:
   • Mean: [dari DB] dB
   • Range: [min] to [max] dB
   • Sensitivity: [narrative]
   
   VV/VH Ratio (Decomposition Index):
   • Mean: [calculated] dB
   • Interpretation: [narrative]
   • Seasonal variation: [narrative dengan data]
   
   SEASONAL PATTERN TABLE (Monthly):
   Month    │ Mean VV (dB) │ Std Dev │ Interpretation
   ─────────┼──────────────┼─────────┼────────────────
   January  │ -X.X         │ X.X     │ [description]
   ...
   ```

2. **Orbit Analysis Statistics** (baru)
   ```
   Ascending Passes:
   • Count: [query] scenes
   • Typical time: ~XX:XX UTC
   • Geometry: Right-looking SAR
   • Incidence angle: X° to Y°
   
   Descending Passes:
   • Count: [query] scenes
   • Typical time: ~XX:XX UTC
   • Geometry: Right-looking SAR
   • Incidence angle: X° to Y°
   ```

3. **Scene Acquisition Timeline** (ASCII art)
   ```
           JAN  FEB  MAR  APR  MAY  JUN  JUL  AUG  SEP  OCT  NOV  DEC
   2025:   ▓▓▓  ▓▓░  ▓▓▓  ▓▓░  ▓▓▓  ▓▓░  ▓▓▓  ▓▓░  ▓▓▓  ▓▓░  ▓▓▓  ▓▓░
   2026:   ▓▓▓  ▓▓░  ▓▓▓  ▓▓░  ▓▓▓  ▓▓░  ▓▓▓  ▓▓░  ▓▓
   
   Legend: ▓ = 4-8 scenes, ░ = 1-3 scenes
   ```

4. **Quality Metrics Tables** (3 subsections)
   - **Radiometric Quality:** thermal noise, stability, accuracy
   - **Geometric Quality:** geolocation, orthorectification accuracy
   - **Coherence & Interferometry:** mean coherence, temporal coherence by season

5. **Data Artifacts & Issues** (detailed)
   ```
   THERMAL NOISE ARTIFACTS
   Issue: Elevated in 3 scenes
   Scene IDs: [list]
   Impact: [description]
   Severity: LOW
   Resolution: [how handled]
   
   GEOLOCATION OUTLIERS
   Identified: [X] scenes with >10m error
   Example: [one detailed example]
   Severity: [LOW/MEDIUM]
   Status: [REMEDIATED/FLAGGED]
   ```

---

## SECTION 7-8: MODIS & GPM Deep Dive

**CURRENT:** ~1 page each (basic stats + 1 image)  
**EXPECTED:** 2-3 pages each (detailed)

### MODIS Needs:

1. **Product Characteristics Section** (baru)
   ```
   Product: MOD11A2.061
   Satellite: Terra
   Overpass Time: ~10:30 AM local
   Wavelengths: Band 31 (11.0 μm), Band 32 (12.0 μm)
   Accuracy: ±1.5 K typical, ±2.5 K mountainous
   Coverage: Day + Night
   ```

2. **Monthly Temperature Statistics Table** (baru)
   ```
   Month    │ Mean LST (°C) │ Std Dev │ Max │ Min
   ─────────┼───────────────┼─────────┼─────┼─────
   January  │ [dari query]   │ [calc]  │ [X] │ [X]
   February │ [dari query]   │ [calc]  │ [X] │ [X]
   ...
   ```

3. **Cloud Cover Analysis Table** (baru)
   ```
   Cloud-Free Observations: X%
   
   Seasonal Cloud Cover:
   Season          │ Cloud-Free % │ Days Affected
   ────────────────┼──────────────┼──────────────
   Winter (Dec-Feb)│ X%           │ X days/month
   Pre-monsoon     │ X%           │ X days/month
   Monsoon (Jun-Sep)│ X%          │ X days/month
   Post-monsoon    │ X%           │ X days/month
   ```

4. **Validation Results** (baru)
   ```
   Validation Sites: X stations
   MODIS vs Ground Truth Accuracy:
   
   Site 1 (Lowland):
   • RMSE: ±X°C
   • Bias: +X°C
   • R²: X
   
   Site 2 (Foothill):
   ...
   ```

### GPM Needs:

1. **Product Characteristics** (baru)
   ```
   Product: IMERG Final Run v06B
   Temporal Resolution: 30-minute
   Spatial Resolution: 0.1° × 0.1°
   Latency: ~3.5 months
   Uncertainty: ±10% mean error
   ```

2. **Seasonal Precipitation Breakdown Table** (detail)
   ```
   Season │ Mean (mm/day) │ Max (mm/day) │ Days >0.1mm │ % of Year
   ───────┼───────────────┼──────────────┼─────────────┼──────────
   Winter │ [query AVG]   │ [query MAX]  │ [query cnt] │ X%
   Pre-m  │ [query AVG]   │ [query MAX]  │ [query cnt] │ X%
   Monsoon│ [query AVG]   │ [query MAX]  │ [query cnt] │ X%
   Post-m │ [query AVG]   │ [query MAX]  │ [query cnt] │ X%
   ```

3. **Extreme Events Table** (Top 5)
   ```
   Rank │ Date │ Peak (mm/day) │ Duration │ Region │ Cause
   ─────┼──────┼───────────────┼──────────┼────────┼──────
    1   │ [X]  │ [X]           │ [X hours]│ [X]    │ Monsoon
    2   │ [X]  │ [X]           │ [X hours]│ [X]    │ Monsoon
   ...
   ```

---

## SECTION 9: Data Quality & Warnings

**CURRENT:** Basic scorecard + 1 chart  
**EXPECTED:** More detailed with warnings per source

### Yang Kurang:

1. **More detailed Data Health Scorecard** (dengan visual bars)
   ```
   Overall Data Health: XX/100 ████████░
   Completeness Score: XX/100 ███████░░
   Geometric Quality: XX/100 ████████░
   Radiometric Quality: XX/100 █████████
   Fusion Success: XX/100 ████████░
   ```

2. **Source-Specific Issues Details** (bukan summary saja)
   ```
   SENTINEL-1 QUALITY ISSUES
   
   ✓ EXCELLENT - No critical issues
   
   Minor Issues:
   1. Thermal noise elevated (3 scenes)
      • Severity: LOW
      • Scenes: [IDs]
      • Recommended action: Flag for sensitivity analyses
      • Status: FLAGGED IN METADATA
   
   2. Geolocation errors >10m (now corrected)
      • Severity: LOW (already corrected)
      • Original error: ±X m max
      • Current status: ±X m
      
   3. Phase wrapping in mountains
      • Severity: MEDIUM
      • Affected area: X% of domain
      • Recommended action: [specific action]
      • Workaround: [solution]
   
   Recommendation: ✓ APPROVED FOR USE
   ```

3. **Quality Recommendations Section** (baru)
   ```
   RECOMMENDED DATA USAGE GUIDELINES
   
   ✓ SUITABLE FOR:
   • Temporal trend analysis
   • Seasonal characterization
   • Machine learning training
   • [list specific use cases]
   
   ⚠ USE WITH CAUTION:
   • Real-time applications (GPM latency)
   • Mountain-region analysis
   • [list conditional uses]
   
   ✗ NOT SUITABLE FOR:
   • Climate trend detection (<4 years)
   • Operational nowcasting
   • [list not suitable uses]
   ```

---

## QUERY OPTIMIZATION

Claude Code perlu aggregate data lebih kompleks:

### Current (Too Simple):
```python
count(records)
AVG(file_size)
MAX/MIN values
```

### Needed (Complex Aggregation):
```python
# Temporal statistics by month/season
SELECT DATE_TRUNC('month', timestamp) as month,
       source,
       COUNT(*) as record_count,
       AVG(value) as mean_value,
       STDDEV(value) as std_value,
       MAX(value) as max_value,
       MIN(value) as min_value
FROM data_products
GROUP BY 1, 2
ORDER BY 1, 2

# Processing level comparison
SELECT processing_level,
       COUNT(*) as records,
       AVG(file_size_bytes) as avg_size,
       (prev_count - curr_count) / prev_count * 100 as loss_pct
FROM data_products
GROUP BY processing_level

# Completeness analysis
SELECT source,
       COUNT(DISTINCT DATE(timestamp)) as unique_days,
       MAX(timestamp) - MIN(timestamp) as date_range_days,
       COUNT(DISTINCT DATE(timestamp)) * 100.0 / 
         EXTRACT(DAY FROM MAX(timestamp) - MIN(timestamp)) as completeness_pct
FROM data_products
GROUP BY source

# Quality flag distribution
SELECT source,
       quality_flag,
       COUNT(*) as count,
       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY source) as pct
FROM data_products
GROUP BY source, quality_flag
```

---

## Implementation Checklist

### MUST DO:
- [ ] Expand Section 2: Add source specs, spatial/temporal diagrams, validation table
- [ ] Expand Section 3: Add flow diagrams, detailed ablation tables for all 3 sources
- [ ] Expand Section 4: Add pseudocode, 3 case studies, alignment statistics table
- [ ] Expand Section 5: Add 3 time-series charts, monthly tables, key findings
- [ ] Expand Section 6: Add polarization table, orbit analysis, timeline, quality metrics
- [ ] Expand Section 7-8: Add product specs, seasonal tables, validation (MODIS) / extremes (GPM)
- [ ] Enhance Section 9: Detailed issues per source, recommendations
- [ ] Optimize queries: Use GROUP BY, aggregation, statistical functions
- [ ] Add narrative: Write interpretive text alongside data
- [ ] Verify page counts: Section 2 ≥2pg, Sect 3 ≥2pg, Sect 4 ≥1pg, Sect 5 ≥2pg, Sect 6 ≥3pg, Sect 7-8 ≥2pg each

### SUCCESS CRITERIA:
- ✅ Report 25-35 pages (not 13)
- ✅ Each section has narrative + tables + charts
- ✅ Monthly/seasonal breakdowns for all sources
- ✅ Multiple detailed tables with database query results
- ✅ Case studies with real data examples
- ✅ ASCII art visualizations where specified
- ✅ No empty sections (all 10 sections fully populated)
- ✅ Content matches report-detailed.md specification exactly
