# Brand Guidelines — THE TRINITY

Working reference for naming, color, and tone across the app, docs, and any
future collateral. Keep it consistent; keep it simple.

## Name & Slogan

- **Brand name:** THE TRINITY
- **Slogan:** "Sentinel Watches, MODIS Sees, GPM Measures"
- **Concept:** three independent satellite sources fused into one flood
  risk / early-warning signal:
  - **Sentinel-1 SAR** — *Watches* continuously (all-weather, day/night radar)
  - **MODIS** (optical) — *Sees* visual surface detail
  - **GPM** (rainfall) — *Measures* precipitation

Do not rename the underlying sensors. "Sentinel-1", "MODIS", and "GPM" are
real satellite/instrument names and stay exactly as they are in code,
schemas, and technical docs. "THE TRINITY" refers only to this project/
platform, i.e. the fusion of the three.

## Color Palette

| Role | Name | Hex |
|---|---|---|
| Primary / background | Deep Navy | `#0A0E1A` |
| Sentinel-1 / "Watches" | Deep Red | `#C41E3A` |
| MODIS / "Sees" | Ocean Blue | `#0077BE` |
| GPM / "Measures" | Forest Green | `#2D6A4F` |

Usage notes:
- Deep Navy is the default dark background/base — UI chrome, headers, cards.
- The three accent colors map 1:1 to the three sensors and should stay
  paired that way anywhere sensors are distinguished visually (legends,
  status chips, per-source charts) — don't reassign them arbitrarily.
- Use the accents for emphasis/status, not large fill areas; Deep Navy (or a
  neutral derived from it) should dominate.

## Logo & Typography

- Logo mark: keep the existing satellite/orbit motif (`web/logo.webp`); no
  redesign required for this rebrand, just re-labeled.
- Primary typeface: IBM Plex Sans (UI text), IBM Plex Mono (labels, data,
  technical/monospace contexts) — already loaded in `web/index.html`, no
  change needed.
- Brand name is set in full caps in UI chrome ("THE TRINITY"); prose/docs use
  title case ("The Trinity") or the full name depending on context — see
  Voice below.

## Voice & Tone

- Technical, direct, no marketing fluff — this is a research/engineering
  tool, not a consumer product. Write like the existing docs (PRD, README):
  plain statements of what the system does, not what it "empowers" you to do.
- When introducing the brand in docs, lead with the slogan and one sentence
  on the three-sensor concept, then get back to the technical content.
- Never imply the three sensors are one merged instrument — always describe
  them as three distinct sources that are fused/combined.

## Naming Conventions

| Context | Form |
|---|---|
| UI brand mark / navbar | `THE TRINITY` |
| Prose / headings | `The Trinity` |
| package/module/slug names | `the-trinity` or `the_trinity` |
| Env vars / constants | `THE_TRINITY` |

Do **not** touch: `Sentinel1`, `SentinelScene`, `sentinel1_*` tables/columns,
or any other identifier that names the Sentinel-1 sensor itself rather than
the project brand.
