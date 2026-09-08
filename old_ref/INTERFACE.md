# Interface Documentation
## The Trinity Web Dashboard & Control Panel

---

## Overview

The Trinity web interface provides three main sections for users to create, manage, and monitor satellite data ingestion pipelines:

1. **Buat Dataset** (Create Dataset) — Define new data collection jobs
2. **Dataset Saya** (My Datasets) — Monitor running or completed jobs
3. **Live** — Enable/disable continuous data collection and view live ingestion status

The interface is built with vanilla JavaScript, modern CSS (CSS Grid, Flexbox), and Leaflet map integration. All data flows through REST API endpoints (`/api/...`).

---

## Color Scheme & Design System

### Theme Variables (CSS)

```css
--ink: #0A0E1A              /* Deep background */
--panel: #121A2B            /* Card background */
--text: #E8ECF5             /* Primary text (light) */
--text-dim: #7C89A6         /* Secondary/muted text */
--cyan: #35D0C0             /* Highlight, success */
--amber: #F0A63C            /* Warning, pending */
--coral: #EF6461            /* Error, danger */
--violet: #8B7FE8           /* Secondary accent */
--bronze: #C97C4B           /* RAW tier color */
--silver: #9FB0C9           /* SILVER tier color */
```

### Badge/Chip Status Colors

| Status | Color | Background |
|--------|-------|------------|
| COMPLETED | Cyan | rgba(53, 208, 192, 0.1) |
| PROCESSING | Amber | rgba(240, 166, 60, 0.1) |
| FAILED | Coral | rgba(239, 100, 97, 0.1) |
| PAUSED | Amber | rgba(240, 166, 60, 0.1) |
| QUEUED | Silver | rgba(159, 176, 201, 0.1) |

---

## Section 1: Buat Dataset (Create Dataset)

### Purpose
Users define a new satellite data acquisition job by specifying location, date range, data tiers, and quality settings.

### Layout

**Two-column grid** (responsive: stacks on mobile):
- **Left panel** (60%): Form controls
- **Right panel** (40%): Tier preview + tips

### Left Panel: Form Fields

#### 1. Lokasi (Location)

**Component**: 
- Pre-built **region cards** (3-column grid, clickable)
- **Text input** below cards for custom location entry
- **Map preview** showing selected area

**Region Cards**:
```
┌──────────────────────┐
│  ✓ (selected check)  │
│  Jabodetabek         │
│  6,392 km²           │
└──────────────────────┘
```

**Behavior**:
- Click region card → highlight (border cyan, background dim cyan), populate text input
- Type text → Nominatim geocoding via location_resolver (backend matches known regions or geocodes)
- On match: auto-select region card, show map
- Empty → clear selection, hide map

**Map Preview**:
- Leaflet instance (CartoDB Dark tiles)
- Shows bounding box as cyan rectangle
- Zoom to bounds when region selected
- 200px height, rounded corners

#### 2. Tanggal (Date Range)

**Fields**: 
- `fDateStart` (date input, type="date")
- `fDateEnd` (date input)

**Validation**:
- Both required
- End date must be ≥ Start date
- Client-side only (server validates too)

#### 3. Pengaturan Lanjutan (Advanced Settings)

**Collapsible `<details>` section** with 3 optional fields:

- **Maks. tutupan awan (%)**: `0–100`, skip if empty (backend uses default `cloud_threshold_percent: 20`)
- **Skor kualitas minimum**: `0–100`, skip if empty (backend uses `min_quality_score: 60`)
- **Resolusi (m)**: `>0`, skip if empty (backend uses native 10m)

#### 4. Tier Pilihan (Data Tiers to Save)

**Component**: 
- 5 checkboxes in a row (flex wrap):
  - RAW, BRONZE, SILVER, GOLD (individual)
  - Semua (Select All toggle)
  
**Styling**:
- Rounded pill-shaped border
- Unchecked: light gray border + text
- Checked: cyan border + text
- Hover: slightly darker background

**Behavior**:
- At least 1 tier must be selected (form validation)
- Clicking "Semua" → toggle all 4 tiers in unison
- Unchecking individual tier → auto-uncheck "Semua"

#### 4b. Opsi Pipeline — Buat Preview

Directly below the tier pills, above the name field:

```
┌──────────────────────────────────────────────────────────┐
│ [x] ▣  Buat Preview (Grayscale + Berwarna)               │
│        Menghasilkan PNG dari tier GOLD untuk publikasi    │
│        dan pembacaan ilmiah. Matikan untuk mempercepat.   │
└──────────────────────────────────────────────────────────┘
```

- Element: `<label class="option-row" for="fGeneratePreview">` with
  `<input type="checkbox" id="fGeneratePreview" checked>` and `ICONS.image`.
- **Checked by default** — previews are cheap and useful for research, so this
  is opt-out rather than opt-in.
- Submitted as a top-level `generate_preview` boolean on `POST /api/datasets`,
  *not* inside `quality_settings` (that bag is for data-quality thresholds).
- Deliberately **not** a sixth `.tier-pill`: PREVIEW is not a lineage tier that
  can be requested via `required_tiers`, so rendering it alongside RAW..FUSION
  would imply it obeys the same retention rules. `.option-row` is a separate
  control style with room for the one-line consequence ("slower vs. figures for
  publication") to be read *before* ticking rather than after.
- `.option-row:has(input:checked)` tints the whole card cyan, so the active
  state is legible from the card rather than just the 13px checkbox. Browsers
  without `:has()` still get a fully working control, just without the tint.

#### 5. Metadata (Name & Description)

- **Nama dataset** (required text): "dataset nov ke feb", "Flood Detection 2024", etc.
- **Deskripsi** (optional textarea): "Study focused on urban flooding in Central Jakarta", etc.

#### 6. Submit Button

**Button**: 
- Text: "Buat Dataset"
- Class: `btn btn-primary` (cyan background, dark text)
- On click:
  1. Validate all required fields
  2. Show error toast if missing (red chip)
  3. Disable button, change text to "Membuat..."
  4. POST `/api/datasets` with payload:
     ```json
     {
       "location": "Jakarta",
       "date_start": "2024-01-01",
       "date_end": "2024-12-31",
       "tiers": ["BRONZE", "SILVER", "GOLD"],
       "name": "January–December 2024 Study",
       "description": "...",
       "quality_settings": {
         "min_cloud_cover": 20,
         "min_quality_score": 60
       }
     }
     ```
  4. On success: show green toast "Dataset dibuat (status: QUEUED)", clear form, switch to "Dataset Saya" tab
  5. On error: show red toast with error detail, re-enable button

### Right Panel: Tier Preview

#### Ring Visualization

**SVG concentric circles** showing tier selection:

```
        GOLD (innermost, 10 m²)
        ↓
    SILVER (10 m²)
    ↓
BRONZE (10 m²)
↓
RAW (outermost, 400-800 MB)
```

- **Radius**: Proportional to data size (inner = smaller)
- **Color**: TIER_COLORS (RAW: purple, BRONZE: tan, SILVER: gray, GOLD: cyan)
- **Fill**: Gradient based on checkbox state (checked = 100% opacity, unchecked = 20% opacity)
- **Overviews**: Faint background circles, opaque filled portions for selected tiers

Updates **in real-time** as user checks/unchecks tier boxes.

#### Tier List

**Below ring**: Flat list of selected tiers as inline badges:
```
[RAW] [BRONZE] [SILVER] [GOLD]
```

If no tiers selected: `[Belum ada tier dipilih]` (muted gray)

#### Explanatory Text

Gray text box (font-size 13px):

> "Setelah dataset dibuat, unduhan dijalankan sambil scene sebelumnya diproses. Tier yang tidak dipilih otomatis dihapus dari penyimpanan setelah tiap scene selesai."

Translation: "After dataset creation, downloads run while previous scenes are processed. Unselected tiers are automatically deleted from storage after each scene completes."

---

## Section 2: Dataset Saya (My Datasets)

### Purpose
Display all user datasets (created, running, completed, failed) with progress tracking, scene-level details, and action buttons.

### Top Bar

**Flex row** with:
- Title: "Dataset Saya" (left)
- "Segarkan" (Refresh) button (right, ghost style)

Clicking Refresh → calls `loadDatasets()` → polls all dataset metadata + progress.

### Card List

#### For Each Dataset: Card Component

**Structure**:
```
┌─────────────────────────────────────┐
│  ┌────────────────────────────────┐ │
│  │ [Ring]  Name          [BADGE]  │ │
│  │         Location • Date1 - Date2│
│  │         [TIER] [TIER] [TIER]   │ │
│  └────────────────────────────────┘ │
│                                     │
│  Stats Row (4 columns):             │
│  ┌────┬────┬────┬────┐             │
│  │ N  │ M  │ X  │ GB │             │
│  │scn │ ok │ err│ tot│             │
│  └────┴────┴────┴────┘             │
│                                     │
│  [Pause] [Jeda] [Unduh] [Hapus] [Detail]
│                                     │
│  ┌─ Scene Table (if Detail expanded)
│  │ Product ID | Stage | Status | Error
│  │ S1A_...    | CROP  | ✓ OK   | -
│  │ S1A_...    | LEE   | ⏸ PAUSED | -
│  │ ...
│  └─
└─────────────────────────────────────┘
```

#### Card Header (Flex)

**Left**: Tier ring + metadata
- **Ring SVG**: Same as "Buat Dataset" preview, but smaller (96×96px)
- **Info block**:
  - **Title row**: Dataset name + badge (PROCESSING/QUEUED/COMPLETED/FAILED/PAUSED)
  - **Meta row**: Location • date_start – date_end (gray text)
  - **Tier chips**: Inline badges showing required_tiers (e.g., [BRONZE] [GOLD])

**Badge Status Classes**:
- `.badge.ok` (cyan) → COMPLETED
- `.badge.active` (silver) → PROCESSING
- `.badge.warn` (amber) → PAUSED, QUEUED
- `.badge.danger` (coral) → FAILED

#### Stats Row (2×2 Grid)

Centered stats in box backgrounds:

| Stat | Label |
|------|-------|
| total_scenes | "scene" |
| completed_scenes | "selesai" |
| failed_scenes | "gagal" |
| humanBytes(total_size_bytes) | "ukuran" |

Example:
```
┌─────┬─────┬─────┬──────────┐
│ 127 │ 98  │ 3   │ 8.2 GB   │
│scen │ ok  │ err │ storage  │
└─────┴─────┴─────┴──────────┘
```

#### Action Buttons (Flex Row, Wrap)

**Conditional rendering** based on `dataset.status`:

- **QUEUED, PREPARING, DOWNLOADING, PROCESSING** (any active):
  - `[Jeda]` button (amber) → `pause_dataset` API call
  
- **PAUSED**:
  - `[Lanjutkan]` button (accent cyan) → `resume_dataset` API call
  
- **Any status with total_size_bytes > 0**:
  - `[Unduh]` link (ghost style) → navigates to `/api/datasets/{id}/download`
  
- **All statuses**:
  - `[Hapus]` button (danger red) → `openDeleteModal(id)`
  - `[Detail]` button (ghost) → toggle scene table visibility below
  - `[Struktur]` button (ghost, only when `total_size_bytes > 0`) → toggle the
    Struktur panel (storage per tier, quality per source, and the PREVIEW
    gallery) below

#### Delete Confirmation Modal

**On "Hapus" click**:

```
┌──────────────────────────────────┐
│ Hapus dataset ini?               │
│                                  │
│ Semua file yang tersimpan untuk  │
│ dataset ini akan dihapus total   │
│ dan tidak bisa dikembalikan.     │
│                                  │
│ ☐ Paksa hentikan proses yang     │
│   sedang berjalan                │
│                                  │
│              [Batal] [Ya, Hapus] │
└──────────────────────────────────┘
```

- Checkbox: `deleteForce` → if checked, send `force=true` to DELETE endpoint
- Modal overlay with semi-transparent background (dark)
- Center-aligned, fixed width (360px max)
- Escape key closes modal

**On confirm**:
- DELETE `/api/datasets/{id}?force={force}`
- Show toast: "Penghapusan dimulai" (green)
- Refresh dataset list

#### Scene Detail Table (Expandable)

**On "Detail" button click**, show table below card:

```
┌────────────────────────────────────────────────────┐
│ Product ID    │ Tahap      │ Status   │ Catatan   │
├────────────────────────────────────────────────────┤
│ S1A_IW_GRDH.. │ CROP       │ ✓ COMPLETED │ -      │
│ S1A_IW_GRDH.. │ LEE_FILTER │ ⏸ PAUSED │ -        │
│ S1A_IW_GRDH.. │ COG_EXPORT │ ✗ FAILED   │ timeout│
└────────────────────────────────────────────────────┘
```

**Columns**:
- `product_identifier` (monospace, truncated)
- `current_stage` (stage name)
- `stage_status` (badge: OK, PENDING, PAUSED, FAILED)
- `last_error` (monospace, gray, small font)

**Polling**: Dataset list refreshes every 3 seconds if any dataset is `ACTIVE_STATUSES` (QUEUED, PREPARING, DOWNLOADING, PROCESSING).

#### Struktur Panel (Expandable)

**On "Struktur" button click**, renders below the card, in order:

1. **Storage per tier & source** — one bar per tier, segmented by source.
   Iterates `STORAGE_TIER_ORDER` (`RAW, BRONZE, SILVER, GOLD, PREVIEW, FUSION`),
   which is deliberately *not* `TIER_ORDER`: the latter is the lineage chain the
   user can request and the progress ring draws, while PREVIEW is a derived tier
   that never appears in `required_tiers` but still occupies disk. `fusion` and
   `preview` have no per-source split and draw as one neutral hatched bar.
2. **Kualitas per source**
3. **File list** for whichever tier's `[Berkas]` link was clicked
4. **PREVIEW gallery** (below)

#### PREVIEW Gallery

Source: `GET /api/datasets/{id}/preview`, which reads the sidecar JSON that
`module10_generate_preview.py` wrote next to the PNGs — so the colormap and
interpretation text in the UI is the same text the renderer emitted, not a
second copy that can drift.

```
┌───────────────────────────────────────────────────────────────────┐
│ ▣ PREVIEW                                                 9.0 MB  │
│ 12 Jul 2026            [ Grayscale (2) ] [ Berwarna (3) ]         │
│ Publikasi dan presentasi — warna yang bisa dibaca lintas tanggal. │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                │
│ │              │ │              │ │              │                │
│ │   (image)    │ │   (image)    │ │   (image)    │                │
│ │              │ │              │ │              │                │
│ ├──────────────┤ ├──────────────┤ ├──────────────┤                │
│ │Sentinel-1 VV │ │Sentinel-1 VH │ │S1 Komposit   │                │
│ │[viridis]     │ │[viridis]     │ │[false-color] │                │
│ │[-11.1 – 0.8] │ │[-17.0 – -7.1]│ │              │                │
│ │Backscatter…  │ │Backscatter…  │ │False color…  │                │
│ └──────────────┘ └──────────────┘ └──────────────┘                │
│ 5 lapisan tidak dirender: modis_ndvi, modis_ndwi, gpm_rain_24h…   │
└───────────────────────────────────────────────────────────────────┘
```

**Controls**

- **Date tabs** (`.preview-date`) — one per acquisition date that has previews.
  Hidden when there is only one date; a plain label is shown instead.
- **Kind tabs** (`.preview-kind`, `role="tablist"`) — `Grayscale` / `Berwarna`,
  each with a count badge. Default is `Berwarna`.
- Both selections live in `state.previewScene[id]` / `state.previewKind[id]`, so
  they survive the 3-second polling re-render.

**Cards** (`.preview-grid` → `auto-fill, minmax(190px, 1fr)`)

- `<img loading="lazy" decoding="async">` — one dataset can hold dozens of dates
  × 8 PNGs, and the panel is often opened just to read the storage numbers.
- Thumbnails sit on a **checkerboard** background: preview PNGs are transparent
  where the source was NoData, and on a flat backdrop an empty area is
  indistinguishable from dark data.
- Caption: label, colormap chip, value-range chip (monospace), and the
  interpretation text clamped to 3 lines (full text in `title`).
- Layers that could not be rendered are named explicitly in amber. A gallery
  that silently shows 3 of 8 images reads as "that's all there is" rather than
  "something failed".

**Empty state**: `renderPreviewEmpty(id)` explains *why* the gallery is empty,
because the three reasons need different sentences — "no previews yet" would
leave a user waiting for something that is never coming:

| Condition | Message |
|-----------|---------|
| `dataset.generate_preview === false` | Preview disabled for this dataset; tick "Buat Preview" when creating one, or re-render via the CLI |
| `required_tiers` reaches neither GOLD nor FUSION | Previews render from GOLD; this dataset stops earlier |
| Otherwise | Not generated yet — created automatically after GOLD; older datasets can be re-rendered via the CLI |

This is why `generate_preview` is exposed on `DatasetItem` (the list payload)
and not only on `DatasetDetail`: the card list never fetches detail.

**Icons**: `ICONS.image` (inline SVG, `stroke: currentColor`). No emoji, per the
design system.

---

## Section 3: Live (Continuous Data Ingestion)

### Purpose
Enable/manage 24/7 automated satellite data collection for flood monitoring and model re-training.

### Layout

**Two-column grid** (responsive: stacks on mobile):
- **Left panel** (50%): Live status + controls
- **Right panel** (50%): Recent scenes table

### Left Panel: Live Status & Controls

#### Live Toggle Switch

**Component**: Custom HTML checkbox styled as toggle switch

```
Status:          [🔘      ]  ← Draggable/clickable
Live monitoring  

Ukuran saat ini:  4.2 GB
Terakhir dicek:   Jan 15, 02:00
```

**Behavior**:
- Click to toggle `POST /api/live/toggle?enabled=true/false`
- On toggle: Update UI instantly (optimistic), confirm from API
- Error → revert toggle, show error toast

#### Live Metadata Rows

**Flex rows** showing live status:

```
Ukuran saat ini    → 4.2 GB (from dataset.total_size_bytes)
Terakhir dicek     → Jan 15, 02:00 (from dataset.live_last_checked_at)
```

Formatted with `new Date(iso).toLocaleString('id-ID')`

#### Action Buttons

**Row 1**:
- `[Unduh Terbaru]` link (accent) → `/api/datasets/{live_dataset_id}/download`
- `[Kosongkan]` button (danger) → openClearModal()

**Clear Confirmation Modal**:

```
┌──────────────────────────────────┐
│ Kosongkan dataset live?          │
│                                  │
│ Semua data yang sudah terkumpul  │
│ di dataset live akan dihapus.    │
│ Pemantauan tetap berjalan.       │
│                                  │
│              [Batal] [Ya, Kosong]│
└──────────────────────────────────┘
```

#### Live Source Status

**Section header**: "Sumber Data" (Data Sources)

**For each source** (SENTINEL1, MODIS, GPM):

```
┌────────────────────────────────────┐
│ • SENTINEL1 | cek: Jan 15, 02:00  │
│            | ambil: Jan 14, 23:00 │
│                                    │
│ • MODIS     | cek: Jan 15, 02:00  │
│            | ambil: Jan 14, 01:00 │
│                                    │
│ ○ GPM       | cek: -              │
│            | ambil: disabled      │
└────────────────────────────────────┘
```

**Status dot**:
- Filled circle (cyan) = enabled, recently checked
- Circle outline (gray) = enabled but never checked
- Crossed circle = disabled

**Columns**:
- Source name (SENTINEL1, MODIS, GPM)
- Last check time (from live_sources.last_check)
- Last ingest time (from live_sources.last_ingest)

#### Backfill Form

**Section header**: "Isi Data Lampau (Backfill)"

**Form fields**:
- Date range: `bfStart`, `bfEnd` (date inputs)
- Button: `[Mulai Backfill]` (primary cyan)

**Behavior**:
- Both dates required
- POST `/api/live/backfill` with `{date_start, date_end}`
- On success: toast "Backfill dimulai (job 123)", clear form
- On error: error toast

---

### Right Panel: Recent Scenes

**Section header**: "Scene Terbaru" (Latest Scenes)

**Table**:

```
┌─────────────────────────────────────┐
│ Tanggal       │ Tier   │ Ukuran    │
├─────────────────────────────────────┤
│ 15 Jan 2024   │ [GOLD] │ 82.3 MB  │
│ 14 Jan 2024   │ [GOLD] │ 81.9 MB  │
│ 13 Jan 2024   │ [GOLD] │ 83.1 MB  │
│ 12 Jan 2024   │ [MODIS]│ 15.2 MB  │
│ 11 Jan 2024   │ [GPM]  │ 3.8 MB   │
└─────────────────────────────────────┘
```

**Columns**:
- `scene_date`: Formatted as `toLocaleString('id-ID')`
- `tier`: Chip badge (GOLD, MODIS, GPM colors)
- `size_mb`: Human-readable size

**Polling**: Refreshes every 5 seconds (from `loadLive()` interval).

---

## Top Navigation Bar

### Header Structure

```
┌─────────────────────────────────────────────────┐
│ [ICON] The Trinity    |  Status  | Time   │
│ Konsol Data Banjir          | [●] Online       │
└─────────────────────────────────────────────────┘
```

**Left**: Brand logo (SVG) + text
**Center**: Tab navigation
**Right**: API status + last update time

### Tab Navigation

**Three tabs**, left-aligned below header:

```
[Buat Dataset]  [Dataset Saya]  [Live]
```

- Active tab: Cyan background, dark text, rounded top corners
- Inactive: Ghost style (transparent, dim text)
- On click: Switch to corresponding view, auto-load data if needed

**Behavior**:
- Click "Dataset Saya" → `switchTab('datasets')` → call `loadDatasets()` + start polling
- Click "Live" → `switchTab('live')` → call `loadLive()` + start polling
- Polling stops when switching away from tab

### Status Indicator (Top Right)

```
[●] Online          (green dot, success)
[●] Degraded        (amber dot, warning)
[●] Offline         (red dot, error)
```

Fetches `/api/health` every 15 seconds, updates dot + label.

---

## Toast Notifications

**Position**: Fixed stack in bottom-right corner

**Types**:
- **Success** (green/cyan): "Dataset dibuat (status: QUEUED)"
- **Error** (red): "Permintaan gagal (400)"
- **Info** (gray): "Memuat..."

**Behavior**:
- Auto-dismiss after 4.2 seconds
- Stack vertically with 10px gap
- Slide-in animation (smooth)
- Click to dismiss manually (optional)

**Example**:
```
┌─────────────────────────────────────┐
│ ✓ Dataset dibuat (status: QUEUED)   │
└─────────────────────────────────────┘
```

---

## Monitoring Dashboard (Bonus: Sentinel1Dashboard.html)

A **read-only monitoring view** for operations teams (separate from data-creation interface).

### Layout

**Stats row** (5 columns, responsive):
- Total scenes ingested
- GOLD products ready
- QA PASS rate
- QA FAIL / Alerts
- Total storage used

**Gallery** (responsive grid):
- Latest GOLD COG thumbnails (Band VV by default, toggle VH)
- Click to view full scene metadata
- Quality badge (PASS/FAIL/WARNING)

**Storage usage** (4-column grid):
- RAW: X GB (red bar)
- BRONZE: Y GB (amber bar)
- SILVER: Z GB (blue bar)
- GOLD: W GB (gold bar)

**Scheduler status** (panel):
- Is scheduler running?
- Next check time
- Manual trigger button: "Jalankan pipeline sekarang"
- Log panel: Last 10 events (scrollable)

### Refresh Cadence

- Stats: Refresh every 60 seconds
- Gallery: Refresh every 60 seconds
- Storage: Refresh every 60 seconds
- API health: Refresh every 15 seconds
- Log: Real-time append (if WebSocket available, else polling)

---

## Responsive Design

### Breakpoints

| Viewport | Behavior |
|----------|----------|
| ≥ 1200px | 2-col layout (form + preview) |
| 900–1200px | Stack form + preview, full width |
| 600–900px | Single column, smaller fonts |
| < 600px | Mobile optimized, touch-friendly buttons |

### Mobile Considerations

- Form fields: full width, larger touch targets (44px min)
- Region cards: 2-column grid (instead of 3)
- Storage grid: 2 rows of 2 cards (instead of 1×4)
- Buttons: Stack vertically if needed
- Modal: Match viewport with safe margins

---

## Accessibility Features

- **Semantic HTML**: `<form>`, `<label>`, `<input>`, etc.
- **Color contrast**: WCAG AA compliant (text on background)
- **Keyboard navigation**: Tab through form fields, Enter to submit
- **Focus indicators**: `:focus-visible` outline (2px cyan)
- **Error announcements**: Toast notifications for screen readers
- **Date inputs**: Native browser date picker (accessible)

---

## API Integration Summary

| User Action | Endpoint | Method | Payload |
|------------|----------|--------|---------|
| Create dataset | `/api/datasets` | POST | {location, date_start, date_end, tiers, name, description, quality_settings} |
| List datasets | `/api/datasets` | GET | (query params: limit, offset) |
| Get dataset status | `/api/datasets/{id}/status` | GET | - |
| Pause dataset | `/api/datasets/{id}/pause` | POST | {reason?} |
| Resume dataset | `/api/datasets/{id}/resume` | POST | - |
| Delete dataset | `/api/datasets/{id}` | DELETE | ?force=true/false |
| Download dataset | `/api/datasets/{id}/download` | GET | (stream .zip) |
| Get live status | `/api/live` | GET | - |
| Toggle live | `/api/live/toggle` | POST | {enabled: true/false} |
| Clear live | `/api/live/clear` | POST | - |
| Backfill live | `/api/live/backfill` | POST | {date_start, date_end} |
| Health check | `/api/health` | GET | - |

---

## Error Handling & User Feedback

### Validation Errors

**Client-side** (instant feedback):
- Required fields: red border on input, focus-trap
- Date range: visual alert if end < start
- Tier selection: button disabled until ≥1 selected

**Server-side** (HTTP error response):
- 400 Bad Request: invalid location, date range, etc.
- Toast: "Lengkapi lokasi, tanggal, dan nama dataset"
- Re-enable form for retry

### Network Errors

**Timeout or network failure**:
- Toast: "Tidak bisa menghubungi server"
- Retry button (auto-retry for polling, manual for form submit)
- Status indicator shows "Offline"

### Graceful Degradation

- If map library (Leaflet) fails to load: show fallback message "Map tidak tersedia"
- If polling fails: toast "Gagal memuat data terbaru", continue showing stale data
- If API returns 500: toast "Server error, coba lagi", don't leave user blocked

---

## Theming & Customization

### Dark Mode (Default, No Light Mode)

All colors optimized for dark theme. Light backgrounds reserved for actionable elements (buttons, inputs).

### Font Stack

```css
--font-sans: 'IBM Plex Sans', -apple-system, 'Segoe UI', sans-serif
--font-mono: 'IBM Plex Mono', ui-monospace, 'SFMono-Regular', Consolas, monospace
```

Loaded from Google Fonts CDN.

### Custom CSS Properties

All colors as CSS vars for easy theme swapping (if needed in future).

---

## File Structure

```
web/
├── index.html              (Main dashboard + form)
├── app.js                  (All view logic: create, datasets, struktur, preview gallery, live)
├── icons.js                (Icon SVG library: trash, plus, search, pin, image)
├── style.css               (Full stylesheet, incl. .preview-* gallery rules)
└── logo.webp
```

---

## Known Limitations & Future Enhancements

### Current Limitations

1. **Map preview**: Leaflet only supports single bbox, not complex polygons
2. **Live sources**: UI shows status but cannot configure per-source params
3. **Bulk operations**: No multi-select dataset delete
4. **Scene filtering**: Cannot filter by orbit direction, cloud cover in list view

### Future Enhancements

- Real-time WebSocket updates (instead of polling)
- Dark/light theme toggle
- Export dataset metadata as CSV/GeoJSON
- Interactive quality metrics charts (QA score trend)
- Download individual scenes (not just full dataset)
- Scene comparison tool (diff two scenes)
- Admin panel for tier retention policies
- Lightbox / full-size view for preview images (currently capped at 1024 px wide)
- Side-by-side grayscale vs colored comparison instead of tab switching
- Preview date scrubber for datasets with many acquisition dates

---

**Interface Version**: 1.0  
**Last Updated**: January 2024  
**Tested Browsers**: Chrome 120+, Firefox 121+, Safari 17+  
**Mobile Tested**: iOS Safari 17+, Chrome Android 120+
