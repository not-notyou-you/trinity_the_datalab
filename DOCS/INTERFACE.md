# Interface Design

## Technology

Vanilla HTML5 + CSS3 + JavaScript. Leaflet.js for maps. No build step, no framework. Served as static files by FastAPI. Fonts: IBM Plex Mono (headings, labels, monospace data) + IBM Plex Sans (body text).

## Visual Design System

### Color Palette

```css
--ink: #0A0E1A;          /* deepest background */
--panel: #121A2B;         /* panel fill (not used directly — glass-bg replaces it) */
--panel-alt: #182238;     /* input fields, alternate surfaces */
--hairline: #232F49;      /* borders, dividers */
--text: #E7ECF5;          /* primary text */
--text-dim: #00F0FF;      /* labels, secondary text (cyan, not gray) */
--cyan: #00F0FF;          /* accent: active states, status, links */
--amber: #F0A63C;         /* warning badges */
--coral: #EF6461;         /* danger/error badges, delete buttons */
```

### Glassmorphism Surface (Every Panel, Card, Modal, Toast, Navbar)

Every container uses this stack — no solid backgrounds anywhere:

```css
background: rgba(10,14,26,0.16);           /* ~16% opacity black */
backdrop-filter: blur(22px) saturate(140%); /* blur what's behind */
border: 2px solid transparent;              /* invisible — gradient goes in ::before */
overflow: hidden;                           /* clip blur to rounded corners */
box-shadow:
  inset -4px -4px 10px rgba(0,0,0,0.35),   /* inner depth: bottom-right dark */
  inset 4px 4px 10px rgba(255,255,255,0.05),/* inner depth: top-left light */
  0 8px 32px rgba(0,0,0,0.28);             /* outer shadow */
```

### Gradient Border (::before Pseudo-Element)

Every glass surface has a gradient border rendered via a masked `::before`:

```css
.glass::before {
  content: '';
  position: absolute;
  inset: 0;
  border-radius: inherit;
  padding: 2px;                              /* border thickness */
  background: linear-gradient(135deg,
    rgba(0,240,255,0.9) 0%,                  /* cyan at top-left */
    rgba(10,14,26,0.9) 65%                   /* fades to near-black at bottom-right */
  );
  -webkit-mask: linear-gradient(#fff 0 0) content-box,
                linear-gradient(#fff 0 0);
  -webkit-mask-composite: xor;
  mask-composite: exclude;                   /* punches hole → only border visible */
  pointer-events: none;
}
```

### Hover Glow

Cards, panels, and sections gain a cyan glow on hover:

```css
.card:hover {
  background: rgba(10,14,26,0.08);          /* slightly more transparent */
  box-shadow:
    inset -4px -4px 10px rgba(0,0,0,0.35),
    inset 4px 4px 10px rgba(255,255,255,0.05),
    0 8px 32px rgba(0,0,0,0.28),
    0 0 46px rgba(0,240,255,0.4);           /* ← cyan glow ring */
}
```

### Border Radius

- Panels, side sections: `18px`
- Cards, modals, inputs, map gap: `10px` (`--radius`)
- Pills, badges, chips: `999px` (capsule)

### Background

The entire app background is a **full-viewport Leaflet map** (`position: fixed; inset: 0; z-index: -2`). On top sits a radial-gradient overlay for ambient color. Outside the "Buat Dataset" tab, the overlay adds a dark linear gradient so cards remain readable over bright map tiles.

## Floating Navbar (Expanding Pill)

The navbar is a **compact circular pill** (56px wide, showing only the logo) that expands when the user hovers anywhere in the top 1/6 of the viewport:

```
COLLAPSED (default):
┌──────┐
│ LOGO │  ← 56px pill, border-radius: 999px
└──────┘

EXPANDED (on hover / focus / click):
┌──────────────────────────────────────────────────────────────────┐
│ LOGO  THE TRINITY          Buat Dataset │ Dataset Saya │ Live  ● │
│        SENTINEL/MODIS/GPM                                        │
└──────────────────────────────────────────────────────────────────┘
```

- Trigger zone: `position: fixed; top: 0; width: 100vw; height: 16.667vh`
- Transition: `max-width 0.4s cubic-bezier(.4,0,.2,1)` — brand text, tabs, and status indicator fade in with `opacity` transition + `transition-delay: 0.15–0.2s`
- Active tab: `color: var(--ink); background: var(--cyan)` (black text on cyan fill)
- Status dot: 8px circle — `.ok` = cyan with glow, `.degraded` = amber, `.down` = coral

## Tab Structure

```
┌─────────────────┬──────────────────┬──────────────┐
│  Buat Dataset   │  Dataset Saya    │  Live        │
│  (Create)       │  (My Datasets)   │  (Ingestion) │
└─────────────────┴──────────────────┴──────────────┘
```

## Tab 1: Buat Dataset (Create Dataset)

### Layout: Three-Column Grid

```
┌────────────┐  ┌──────────────────┐  ┌────────────┐
│  Pilih     │  │                  │  │ Konfigurasi│
│  Lokasi    │  │   Peta Wilayah   │  │            │
│            │  │   (map gap —     │  │ Dates      │
│ [+Tambah]  │  │    transparent   │  │ Satellites │
│ [🔍Cari]   │  │    hole showing  │  │ Processing │
│            │  │    Leaflet map)  │  │ Fusion     │
│ Card list  │  │                  │  │ Preview    │
│ of saved   │  │   Selection box  │  │ Name       │
│ regions    │  │   (cyan border)  │  │            │
│            │  │                  │  │ [Buat]     │
└────────────┘  └──────────────────┘  └────────────┘
  ~268px          flexible width        ~268px
```

Both side panels are glass surfaces (18px radius) with internal scrolling (`.panel-scroll`). The map gap is fully transparent — it's a hole in the layout revealing the background Leaflet map, with a gradient border `::before` and an SVG mask (`#mapMask`, `fill-rule: evenodd`) blocking pointer events outside the gap so the map can only be dragged inside the window.

**Selected region** shown as a cyan-bordered rectangle on the map, repositioned on every map move via `latLngToContainerPoint`.

### Pre-Wizard: Clone Last Configuration

Above the 4-step wizard, a "Pakai Config Sebelumnya" button (gear icon + text) appears only if user has previously created a dataset:

```
┌────────────────────────────────────────┐
│ [⚙ Pakai Config Sebelumnya]            │  ← Click to populate
│                                        │
│ Konfigurasi Terakhir:                  │
│ • Jabodetabek                          │
│ • S1[RAW+PROC] · MODIS[PROC] · GPM[RAW]│
│ • Strategi: HYBRID                     │
└────────────────────────────────────────┘
```

On click:
1. Fetch `GET /api/datasets/last-config`
2. Populate form fields (region, source checkboxes, fusion strategy, preview options)
3. Focus on first field for editing
4. User can edit before creating (dates, region, anything)

If no prior datasets exist, button hidden. If API fails, button stays visible but shows inline error.

### User Journey (4-Step Wizard in Right Panel)

**Step 1 — Region & Date**
- Location: preset region cards (from `regions_of_interest` DB table) + free-text geocoding via Nominatim + Leaflet map
- Region card: glass surface, selected state = `rgba(0,240,255,0.20)` bg + cyan left-edge bar (`::after`) + glow shadow
- Date range: two `input[type=date]` fields

**Step 2 — Satellite & Processing Selection** (combined — each satellite has its own processing config)

Top-level master toggle:
```
Sumber Data Satelit:                              [☑ Pilih Semua]
```
"Pilih Semua" checks all 3 sources + all their processing levels.

Each satellite is an expandable `.option-row` card. Enabling a source reveals its per-satellite processing checkboxes:

```
┌─────────────────────────────────────────────────────────────┐
│ ☑ Sentinel-1 SAR (ESA)                                     │
│   radar, all-weather, ~10m, revisit ~7-8 hari               │
│                                                             │
│   Tingkat Pemrosesan:                        [☑ Semua]      │
│     ☑ RAW  — kalibrasi + crop (tanpa Lee filter, tanpa QA)  │
│     ☑ PROCESSED — + Lee filter 7×7 + QA analytics + COG     │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ ☐ MODIS Optical (NASA)                        [disabled]    │
│   flood/vegetation, 250m, daily                             │
│                                                             │
│   Tingkat Pemrosesan:                        [☐ Semua]      │
│     ☐ RAW  — flood map saja (tanpa indeks turunan)          │
│     ☐ PROCESSED — + hitung NDVI + NDWI dari reflectance     │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ ☐ GPM IMERG Rainfall (NASA/JAXA)              [disabled]    │
│   precipitation, ~10km, daily                               │
│                                                             │
│   Tingkat Pemrosesan:                        [☐ Semua]      │
│     ☐ RAW  — curah hujan harian (hari itu saja)             │
│     ☐ PROCESSED — + akumulasi 24h / 72h / 7 hari            │
└─────────────────────────────────────────────────────────────┘
```

Key interactions:
- Unchecking a source collapses its processing options and grays them out
- Each source has its own "Semua" mini-toggle for its processing levels
- The top-level "Pilih Semua" checks all sources + all processing levels
- At least 1 source with at least 1 processing level is required to proceed

**Step 3 — Fusion & Preview**

Sub-step 3a — Fusion Strategy (shown only if >1 source enabled):
```
Strategi Fusi:
  ○ CO-OCCURRENCE — hanya tanggal yang semua sumber punya data
  ○ FULL_COVERAGE — setiap hari, ±1-2 hari offset OK
  ○ HYBRID — auxiliary harian, S1 jadi jangkar
```

Sub-step 3b — Preview Options (optional, `.option-row` cards):
```
  ☐ GRAYSCALE — percentile 2-98 stretch
  ☐ COLORED — per-source colormap
  ☐ COMPOSITE — false color RGB (khusus S1)
```

The `.option-row` component: `border: 1px solid var(--glass-border); border-radius: 12px; padding: 11px 13px`. When checked: `border-color: rgba(0,240,255,0.45); background: rgba(0,240,255,0.07)` (uses `:has(input:checked)`).

**Step 4 — Review & Create**
- Dataset name input, optional description textarea
- "Buat Dataset" button (`.btn-primary`: cyan bg, black text, full-width)

### Validation Rules
- At least 1 source must be enabled with at least 1 processing level checked
- Each enabled source must have at least 1 processing level checked
- Fusion strategy required if >1 source enabled, hidden if only 1
- Date range valid (start ≤ end)
- Region must be selected from list

## Tab 2: Dataset Saya (My Datasets)

### Dataset Card Layout

```
┌──────────────────────────────────────────────────────────────┐
│  Dataset Name                              PROCESSING        │
│  Region · Date Range                                         │
│  ┌──────────┬──────────┬──────────┐                          │
│  │ S1[R+P]  │ MODIS[P] │ GPM[R]   │  ← per-source processing│
│  └──────────┴──────────┴──────────┘                          │
│                                                              │
│  Strategi Fusi: HYBRID                                       │
│  S1: 5 scene · MODIS: 30 scene · GPM: 30 scene              │
│  Storage: 2.4 GB (S1: 1.2G | MODIS: 0.8G | GPM: 0.4G)      │
│                                                              │
│  Live Logs (terbaru)                                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ 14:02:15  S1A_IW...  LEE_FILTER  COMPLETED          │   │
│  │ 14:01:58  S1A_IW...  CROP        COMPLETED          │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  [Jeda] [Lanjutkan] [Coba lagi] [Batalkan] [Unduh] [Hapus] │
│  [Detail] [Struktur]                                        │
└──────────────────────────────────────────────────────────────┘
```

**Key changes from Prototype**:
- ❌ Removed tier chips (RAW, BRONZE, SILVER, GOLD, FUSION)
- ✅ Source + processing level chips: `S1[R+P]`, `MODIS[P]`, `GPM[R]`
  - `R` = RAW configured, `P` = PROCESSED configured, `[R+P]` = both selected
  - Only show sources actually configured in dataset
- ❌ Removed ring progress with tier colors
- ✅ Replaced with per-source scene count (e.g., "S1: 5 scene · MODIS: 30 scene")
- ✅ Per-source storage breakdown in parentheses (S1: 1.2G | MODIS: 0.8G | GPM: 0.4G)
- ✅ Fusion strategy label
- ✅ Live logs still show per-stage progress

**Card styling**: `.dataset-card` (glass surface, 18px radius, cyan border gradient on hover)

### Expandable Panels

**Detail panel**:
- Scene list per source (S1 scenes, MODIS granules, GPM daily)
- Per-scene: product_identifier, current stage, status, last error
- Scene status colors: COMPLETED = cyan, RUNNING = amber, FAILED = coral

**Struktur panel**:
- Storage tree (collapsible per source, then per processing level, then per tier):
  ```
  SENTINEL-1
    ├─ RAW: 150 MB (BRONZE ██ + PREVIEW █)
    └─ PROCESSED: 600 MB (BRONZE ██ + SILVER ██ + GOLD ██ + PREVIEW █)
  MODIS
    └─ PROCESSED: 250 MB (BRONZE █ + SILVER ██ + GOLD ██ + PREVIEW █)
  GPM
    └─ RAW: 60 MB (BRONZE █ + PREVIEW ░)
  ```
- No horizontal bar chart; use nested list with indentation + byte counts
- Quality metrics per source (only for S1 PROCESSED): nodata%, speckle, score
- File browser: browse by source/processing_level/tier/date
- Preview gallery: separate tabs per source+processing_level combo

### Preview Gallery (Inside Struktur Panel)

Structure:
```
SENTINEL-1 RAW          [Date selector: 2024-01-15 ↕]
Grayscale / Colored
[Image grid 190px min]

SENTINEL-1 PROCESSED   [Date selector: 2024-01-15 ↕]
Grayscale / Colored
[Image grid 190px min]

MODIS PROCESSED        [Date selector: 2024-01-15 ↕]
Grayscale / Colored
[Image grid 190px min]
```

Each image card: thumbnail + label (band name), colormap tag, value range, interpretation note (3 lines max). Checkerboard for NoData.

## Tab 3: Live (Live Ingestion)

- Master toggle (`.switch` — capsule slider, cyan when ON)
- Per-source enable rows (dot indicator + source name + processing level + last check/ingest timestamps)
  - E.g., "Sentinel-1 [RAW+PROCESSED] · Last check: 2 hours ago · Last ingest: 1 hour ago"
- Total storage size, overall last checked timestamp
- Backfill form (date range + submit)
- Recent ingested scenes table:
  | Date | Source | Processing | Stage | Status | Size |
  |---|---|---|---|---|---|
  | 2024-09-08 | Sentinel-1 | PROCESSED | GOLD_EXPORT | COMPLETED | 125 MB |
  | 2024-09-08 | MODIS | PROCESSED | LEE_FILTER | COMPLETED | 48 MB |
  | 2024-09-07 | Sentinel-1 | RAW | CROP | COMPLETED | 65 MB |

## Modals

Glass surfaces (`border-radius: 10px; padding: 24px; width: 360px`), backdrop `rgba(6,9,16,0.55)` + `blur(4px)`. Used for:
- Add location (2-tab: search via Nominatim / manual coordinates)
- Delete location (soft-delete confirmation)
- Delete dataset (with force-stop checkbox)
- Cancel dataset
- Clear live data

All close on Escape key.

## Toasts

Bottom-right stack. Glass surface with gradient border. Success = cyan tint, Error = coral tint. Auto-dismiss after 4.2s with `translateY` entrance animation.

## UI Language

Primary: **Indonesian (Bahasa Indonesia)**. All labels, tooltips, status messages.

Status labels: Antrian (Queued), Sedang Diproses (Processing), Selesai (Completed), Gagal (Failed), Dijeda (Paused), Dibatalkan (Cancelled).

## Responsive Behavior

- **≤900px**: Single-column stack, map gap hidden, navbar trigger zone shrinks to 92px fixed height
- **≤1180px**: Narrower side panels (232px), shorter map window
- **≤560px**: Preview grid columns narrow to `minmax(140px, 1fr)`, option-row padding tightens
- `prefers-reduced-motion`: All animations and transitions disabled