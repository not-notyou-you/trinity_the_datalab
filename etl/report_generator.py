# etl/report_generator.py
"""Pembuat laporan PDF + JSON per dataset (report/report-detailed.md, 10 section).

Modul ini sengaja diisolasi dari pipeline ETL: dia hanya MEMBACA dataset yang
sudah ada (DB + file di disk) dan tidak pernah menulis balik ke schema/produk
data. Kegagalan generate laporan tidak boleh mengganggu ETL atau API lain --
pemanggil (api/routes/report.py) menangkap ReportGenerationError dan
mengembalikannya sebagai error HTTP biasa, bukan crash proses.

Semua angka berasal dari etl/report_stats.py: query agregat ke DB (GROUP BY
bulan/musim/tier/stage, STDDEV, persentil) dan statistik piksel dari raster
COG MODIS/GPM milik dataset. Spesifikasi mencontohkan beberapa besaran yang
TIDAK diukur oleh pipeline ini (validasi ground-truth per stasiun, akurasi
geolokasi, phase coherence, LST -- produk MODIS di sini adalah MCDWD flood +
indeks NDVI/NDWI, bukan MOD11A2). Modul ini secara sengaja TIDAK memalsukan
angka tersebut: bagian terkait diisi dengan proksi yang benar-benar terukur
(mis. uji konsistensi silang antar-sensor) dan diberi catatan eksplisit.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median, pstdev

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    CondPageBreak,
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

from etl import folder_manager as fm
from etl import report_forecast as rf
from etl import report_stats as rs
from etl.dataset_manager import DatasetManager
from etl.database_client import DatabaseClient

logger = logging.getLogger(__name__)

REPORT_VERSION = "2.0"

# Kategorikal, urutan tetap (colorblind-friendly). Entitas -> warna tetap.
_CHART_COLORS = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
_SOURCE_COLORS = {"SENTINEL1": "#4C72B0", "MODIS": "#DD8452", "GPM": "#55A868", "FUSION": "#8172B3"}
_INK = "#333333"
_GRID = "#DDDDDD"

_SOURCE_PREVIEW_TOKENS = {"SENTINEL1": "_s1_", "MODIS": "_modis_", "GPM": "_gpm_"}
_SOURCE_LABEL = {"SENTINEL1": "Sentinel-1", "MODIS": "MODIS", "GPM": "GPM"}

_TEXT_W = A4[0] - 1.5 * inch


class ReportGenerationError(Exception):
    """Data dataset tidak cukup untuk membuat laporan, atau assembly PDF gagal."""


# -- fonts --------------------------------------------------------------------

_FONTS = {"body": "Helvetica", "bold": "Helvetica-Bold", "italic": "Helvetica-Oblique", "mono": "Courier"}


def _register_fonts() -> None:
    """DejaVu (dibundel matplotlib) dipakai supaya simbol ✓ ⚠ ✗ ▓ ░ █ ★ ° ±
    dan box-drawing di diagram ASCII ter-render -- font Type1 bawaan PDF
    (Helvetica/Courier) tidak punya glyph tersebut."""
    if _FONTS["body"] == "DejaVuSans":
        return
    ttf = Path(matplotlib.get_data_path()) / "fonts" / "ttf"
    try:
        for name, fn in [
            ("DejaVuSans", "DejaVuSans.ttf"), ("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"),
            ("DejaVuSans-Oblique", "DejaVuSans-Oblique.ttf"), ("DejaVuSans-BoldOblique", "DejaVuSans-BoldOblique.ttf"),
            ("DejaVuSansMono", "DejaVuSansMono.ttf"), ("DejaVuSansMono-Bold", "DejaVuSansMono-Bold.ttf"),
        ]:
            pdfmetrics.registerFont(TTFont(name, str(ttf / fn)))
        pdfmetrics.registerFontFamily(
            "DejaVuSans", normal="DejaVuSans", bold="DejaVuSans-Bold",
            italic="DejaVuSans-Oblique", boldItalic="DejaVuSans-BoldOblique",
        )
        _FONTS.update(body="DejaVuSans", bold="DejaVuSans-Bold", italic="DejaVuSans-Oblique", mono="DejaVuSansMono")
    except Exception:  # font hilang -> tetap jalan dengan font PDF standar
        logger.warning("[report] font DejaVu tidak tersedia; memakai Helvetica/Courier")


@dataclass
class _Styles:
    title: ParagraphStyle
    section: ParagraphStyle
    sub: ParagraphStyle
    sub2: ParagraphStyle
    cover_subtitle: ParagraphStyle
    body: ParagraphStyle
    bullet: ParagraphStyle
    bold: ParagraphStyle
    note: ParagraphStyle
    mono: ParagraphStyle
    pre: ParagraphStyle
    caption: ParagraphStyle
    cell: ParagraphStyle


def _styles() -> _Styles:
    # report.md "PDF Format Standards": Title 24pt, H1 16pt, H2 14pt, body 11pt,
    # caption/footer 9pt, spasi 1.5.
    b, bd, it, mo = _FONTS["body"], _FONTS["bold"], _FONTS["italic"], _FONTS["mono"]
    body = ParagraphStyle("Body11", fontName=b, fontSize=10.5, leading=15.5, spaceAfter=4)
    return _Styles(
        title=ParagraphStyle("TitleXL", fontName=bd, fontSize=24, leading=30),
        section=ParagraphStyle("SectionHeading", fontName=bd, fontSize=16, leading=20, spaceBefore=6, spaceAfter=10,
                               textColor=colors.HexColor("#1F3A5F")),
        sub=ParagraphStyle("SubHeading", fontName=bd, fontSize=13, leading=17, spaceBefore=10, spaceAfter=6,
                           textColor=colors.HexColor("#1F3A5F")),
        sub2=ParagraphStyle("SubSub", fontName=bd, fontSize=11, leading=15, spaceBefore=6, spaceAfter=3),
        cover_subtitle=ParagraphStyle("CoverSubtitle", fontName=bd, fontSize=16, leading=22),
        body=body,
        bullet=ParagraphStyle("Bullet", parent=body, leftIndent=14, spaceAfter=2),
        bold=ParagraphStyle("Bold", parent=body, fontName=bd),
        note=ParagraphStyle("Note", parent=body, fontName=it, fontSize=9, leading=13, textColor=colors.HexColor("#555555")),
        mono=ParagraphStyle("Mono", parent=body, fontName=mo, fontSize=6.5, leading=7.8, spaceAfter=0),
        pre=ParagraphStyle("Pre", fontName=mo, fontSize=7.5, leading=9.5, backColor=colors.HexColor("#F6F7F9"),
                           borderPadding=6, spaceBefore=4, spaceAfter=10),
        caption=ParagraphStyle("Caption", parent=body, fontName=it, fontSize=9, leading=12,
                               textColor=colors.HexColor("#555555"), spaceAfter=8),
        cell=ParagraphStyle("Cell", fontName=b, fontSize=7.8, leading=9.5),
    )


class _ReportDocTemplate(SimpleDocTemplate):
    """SimpleDocTemplate yang mencatat TOCEntry + bookmark PDF setiap kali
    Paragraph berstyle SectionHeading/SubHeading lewat -- inilah mekanisme
    "Table of Contents (Auto-Generated)" di report.md. Butuh multiBuild()
    (dua pass) supaya nomor halaman di TOC benar."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._toc_seq = 0
        self.section_pages: list[tuple[str, int]] = []

    def afterFlowable(self, flowable):
        if not isinstance(flowable, Paragraph):
            return
        style_name = getattr(flowable.style, "name", "")
        if style_name not in ("SectionHeading", "SubHeading"):
            return
        text = flowable.getPlainText()
        level = 0 if style_name == "SectionHeading" else 1
        self.notify("TOCEntry", (level, text, self.page))
        if level == 0:
            self.section_pages.append((text, self.page))
        key = f"toc-{self._toc_seq}"
        self._toc_seq += 1
        self.canv.bookmarkPage(key)
        self.canv.addOutlineEntry(text, key, level=level, closed=False)

    def handle_documentBegin(self):
        # multiBuild menjalankan beberapa pass; catatan halaman hanya dari pass terakhir.
        self.section_pages = []
        self._toc_seq = 0
        super().handle_documentBegin()


@dataclass
class _ReportContext:
    dataset: dict
    breakdown: dict
    quality: list[dict]
    slug: str
    root: Path
    stats: rs.ReportStats | None = None
    charts: list[str] = field(default_factory=list)
    issues: list[dict] = field(default_factory=list)
    scores: dict = field(default_factory=dict)
    forecasts: list = field(default_factory=list)


class ReportGenerator:
    """`ReportGenerator(dataset_id, db).generate()` -> path ke PDF yang dibuat.
    File JSON (Section 10) ditulis di sebelahnya dengan stem yang sama."""

    def __init__(self, dataset_id: int, db: DatabaseClient):
        self.dataset_id = dataset_id
        self.db = db

    def generate(self, force: bool = False) -> Path:
        info = DatasetManager(self.db).get_dataset(self.dataset_id)
        if info is None:
            raise ReportGenerationError(f"Dataset {self.dataset_id} not found")

        root = fm.get_dataset_root(self.dataset_id, info["name"])
        reports_dir = root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        cached = None if force else self._find_cached(reports_dir, info)
        if cached is not None:
            return cached

        try:
            _register_fonts()
            ctx = _ReportContext(
                dataset=info,
                breakdown=fm.storage_breakdown(self.dataset_id, info["name"]),
                quality=self._quality_dicts(),
                slug=fm.slugify(info["name"]),
                root=root,
            )
            ctx.stats = rs.collect(self.db, self.dataset_id, info)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            out_path = reports_dir / f"report_{timestamp}.pdf"
            summary = self._build_pdf(ctx, out_path)
            payload = _json_summary(ctx)
            payload["report_metadata"]["implementation"] = summary
            out_path.with_suffix(".json").write_text(
                json.dumps(payload, indent=2, default=str, ensure_ascii=False), encoding="utf-8",
            )
        except ReportGenerationError:
            raise
        except Exception as exc:  # isolasi: kegagalan chart/PDF tidak boleh crash caller
            logger.exception("[report] gagal membuat laporan dataset %s", self.dataset_id)
            raise ReportGenerationError(f"Failed to generate the report: {exc}") from exc

        return out_path

    # -- caching ------------------------------------------------------------

    def _find_cached(self, reports_dir: Path, info: dict) -> Path | None:
        """Laporan lama dipakai ulang kalau tidak ada perubahan dataset sejak
        dibuat (report.md, "Cache chart PNGs, regen only on dataset update").
        Kita cache seluruh PDF, bukan cuma chart-nya -- lebih sederhana dan
        sama efeknya untuk kasus penggunaan ini (klik ulang tombol tanpa
        ada job baru selesai)."""
        existing = sorted(reports_dir.glob("report_*.pdf"))
        if not existing:
            return None
        latest = existing[-1]
        updated_at = info.get("updated_at")
        if updated_at is None:
            return latest
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        mtime = datetime.fromtimestamp(latest.stat().st_mtime, tz=timezone.utc)
        return latest if mtime >= updated_at else None

    # -- data gathering -------------------------------------------------------

    def _quality_dicts(self) -> list[dict]:
        from api.routes.quality import compute_quality_by_source

        items = compute_quality_by_source(self.db, self.dataset_id)
        return [item.model_dump() for item in items]

    def _coverage_audit_issues(self, dataset_name: str) -> list[dict]:
        """Surface `audit_dataset_coverage` (valid_fraction sentinel1/VV per
        stack fusion) ke Section 9.2, supaya cakupan rendah -- baik yang
        berulang legitimately (tier revisit orbit S1) maupun yang terisolasi
        dan patut dicurigai sebagai bug mosaik -- termonitor otomatis lewat
        laporan, tanpa perlu investigasi manual per dataset seperti sebelumnya.
        Murah (cuma baca attrs HDF5); kegagalannya tidak boleh menggagalkan
        laporan sama sekali."""
        from etl.module9_fusion import audit_dataset_coverage

        try:
            audit = audit_dataset_coverage(self.db, self.dataset_id, dataset_name)
        except Exception:  # noqa: BLE001
            logger.exception("[report] audit cakupan dilewati untuk dataset %s", self.dataset_id)
            return []
        issues = []
        recurring = audit.get("recurring_low") or []
        if recurring:
            issues.append({
                "source": "FUSION", "severity": "LOW",
                "title": f"{len(recurring)} fusion stacks with a lower but recurring sentinel1/VV valid_fraction "
                         "(orbit-revisit tier)",
                "detail": ", ".join(f"{e['file']} ({e['valid_fraction']:.3f})" for e in recurring[:8]),
                "action": "Most likely a genuine partial S1 swath for this date (the AOI is "
                          "straddled by >1 relative orbit); no need to re-fuse unless checked manually.",
                "status": "INFO",
            })
        dropped = audit.get("dropped") or []
        if dropped:
            issues.append({
                "source": "FUSION", "severity": "HIGH",
                "title": f"{len(dropped)} fusion stacks with a sentinel1/VV valid_fraction "
                         "far lower and ISOLATED (not a recurring tier)",
                "detail": ", ".join(f"{e['file']} ({e['valid_fraction']:.3f})" for e in dropped[:8]),
                "action": "Check etl.refusion.scene_results_for_date for this date -- "
                          "the mosaic may have lost an S1 frame that actually exists on disk/DB.",
                "status": "OPEN",
            })
        return issues


    def _preview_images_for_source(self, ctx: _ReportContext, source: str) -> list[tuple[Path, str]]:
        """Preview berwarna milik satu source (Section 6/7/8): SEMUA band source
        itu pada satu tanggal representatif dari folder `colored/`, ditambah
        overlay `composite/` ({band}_on_s1) kalau ada. `grayscale/` hanya
        dipakai kalau tidak ada preview berwarna sama sekali.

        module10_generate_preview.py menamai {tanggal}_{s1|modis|gpm}_{band}.png;
        tidak ada tabel preview di DB, jadi daftarnya dibaca dari disk."""
        token = _SOURCE_PREVIEW_TOKENS.get(source)
        if token is None:
            return []
        root = fm.get_dataset_root(self.dataset_id, ctx.dataset["name"]) / "preview"
        for level in ("PROCESSED", "RAW"):
            for kinds in (("colored", "composite"), ("grayscale",)):
                by_date: dict[str, list[tuple[Path, str]]] = {}
                for kind in kinds:
                    kind_dir = root / level / kind
                    if not kind_dir.is_dir():
                        continue
                    for p in sorted(kind_dir.glob("*.png")):
                        stem = p.stem.lower()
                        if token not in stem:
                            continue
                        day, _, rest = stem.partition("_")
                        by_date.setdefault(day, []).append((p, _preview_label(rest, kind)))
                if by_date:
                    day = max(by_date, key=lambda d: (len(by_date[d]), _preview_date_score(ctx.stats, source, d)))
                    return by_date[day]
        return []

    # -- charts -----------------------------------------------------------

    def _save(self, ctx: _ReportContext, fig, name: str) -> Path:
        path = ctx.root / "reports" / "_charts" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.tight_layout()
        fig.savefig(path, dpi=150)
        plt.close(fig)
        ctx.charts.append(name)
        return path

    def _chart_storage_by_tier(self, ctx: _ReportContext, out_dir: Path) -> Path | None:
        tiers = {
            name: t["size_bytes"] / (1024 * 1024)
            for name, t in ctx.breakdown["tiers"].items()
            if t["size_bytes"] > 0
        }
        if not tiers:
            return None
        fig, ax = _fig()
        ax.bar(list(tiers.keys()), list(tiers.values()), color=_CHART_COLORS[0], width=0.6)
        ax.set_ylabel("Size (MB)")
        ax.set_title("Disk Usage per Tier", loc="left")
        plt.xticks(rotation=30, ha="right", fontsize=8)
        return self._save(ctx, fig, "chart_storage.png")

    def _chart_ablation(self, ctx: _ReportContext, out_dir: Path) -> Path | None:
        """RAW vs PROCESSED per source di disk: `aligned` ditulis ke laci RAW/,
        `cog` ke PROCESSED/ (etl/folder_manager.py LEVEL_BY_TIER)."""
        raw_sources = ctx.breakdown["tiers"].get("aligned", {}).get("sources", {})
        cog_sources = ctx.breakdown["tiers"].get("cog", {}).get("sources", {})
        sources = sorted(set(raw_sources) & set(cog_sources))
        if not sources:
            return None
        raw_counts = [raw_sources[s]["file_count"] for s in sources]
        cog_counts = [cog_sources[s]["file_count"] for s in sources]
        x = range(len(sources))
        width = 0.35
        fig, ax = _fig()
        ax.bar([i - width / 2 for i in x], raw_counts, width, label="RAW", color=_CHART_COLORS[0])
        ax.bar([i + width / 2 for i in x], cog_counts, width, label="COG (processed)", color=_CHART_COLORS[1])
        ax.set_xticks(list(x))
        ax.set_xticklabels(sources)
        ax.set_ylabel("File count")
        ax.set_title("File di Disk: RAW vs COG per Source", loc="left")
        ax.legend(frameon=False, fontsize=8, ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.12))
        return self._save(ctx, fig, "chart_ablation.png")

    def _chart_effectiveness(self, ctx: _ReportContext) -> Path | None:
        """Section 3.4: per source, retensi tanggal & reduksi ukuran dari tier
        pertama ke COG -- dua ukuran persen dengan skala sama (0-100)."""
        st = ctx.stats
        rows = []
        for src in ("SENTINEL1", "MODIS", "GPM"):
            first, last = _tier_endpoints(st, src)
            if not first or not last:
                continue
            retention = last["dates"] / first["dates"] * 100 if first["dates"] else 0
            reduction = (1 - last["avg_mb"] / first["avg_mb"]) * 100 if first["avg_mb"] else 0
            rows.append((src, retention, reduction))
        if not rows:
            return None
        fig, ax = _fig()
        x = range(len(rows))
        w = 0.36
        ax.bar([i - w / 2 for i in x], [r[1] for r in rows], w, label="Date retention (%)", color=_CHART_COLORS[0])
        ax.bar([i + w / 2 for i in x], [r[2] for r in rows], w, label="Average file-size reduction (%)",
               color=_CHART_COLORS[1])
        for i, r in enumerate(rows):
            ax.text(i - w / 2, r[1] + 1.5, f"{r[1]:.0f}%", ha="center", fontsize=7, color=_INK)
            ax.text(i + w / 2, max(r[2], 0) + 1.5, f"{r[2]:.0f}%", ha="center", fontsize=7, color=_INK)
        ax.set_xticks(list(x))
        ax.set_xticklabels([_SOURCE_LABEL[r[0]] for r in rows])
        ax.set_ylim(0, 110)
        ax.set_ylabel("%")
        ax.set_title("Processing Effectiveness: First Tier → COG", loc="left")
        ax.legend(frameon=False, fontsize=8, ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.1))
        return self._save(ctx, fig, "chart_processing_effectiveness.png")

    def _chart_series(self, ctx: _ReportContext, name: str, title: str, ylabel: str,
                      series: dict[str, list[tuple[date, float]]], kind: str = "line",
                      forecasts: dict | None = None) -> Path | None:
        series = {k: sorted(v) for k, v in series.items() if len(v) >= 2}
        if not series:
            return None
        forecasts = forecasts or {}
        fig, ax = _fig()
        all_days = [d for pts in series.values() for d, _ in pts]
        _shade_seasons(ax, min(all_days), max(all_days))
        for i, (label, pts) in enumerate(series.items()):
            color = _CHART_COLORS[i % len(_CHART_COLORS)]
            xs, ys = [p[0] for p in pts], [p[1] for p in pts]
            if kind == "bar":
                ax.bar(xs, ys, width=0.8, color=color, label=label)
            else:
                ax.plot(xs, ys, color=color, linewidth=1.4, marker="o", markersize=2.5, label=label)
            fc = forecasts.get(label)
            if fc is not None and fc.days:
                _draw_forecast(ax, fc, color, xs[-1], ys[-1])
        if forecasts:
            _mark_forecast_start(ax, ctx.stats.period_end)
        ax.set_ylabel(ylabel)
        ax.set_title(title, loc="left")
        if len(series) > 1 or forecasts:
            ax.legend(frameon=False, fontsize=7, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.2))
        fig.autofmt_xdate()
        return self._save(ctx, fig, name)

    def _chart_forecast_panels(self, ctx: _ReportContext) -> Path | None:
        """Small multiples: tiap variabel satu panel (skala-y sendiri, tanpa
        dual axis) -- jendela historis 2x horizon + prakiraan dan pita 80/95%."""
        fcs = ctx.forecasts
        if not fcs:
            return None
        n = len(fcs)
        cols = 2
        rows = (n + 1) // 2
        fig, axes = plt.subplots(rows, cols, figsize=(6.8, 1.9 * rows + 0.3), squeeze=False)
        color = {"SENTINEL1": _SOURCE_COLORS["SENTINEL1"], "MODIS": _SOURCE_COLORS["MODIS"], "GPM": _SOURCE_COLORS["GPM"]}
        for ax, fc in zip(axes.flat, fcs):
            start = ctx.stats.period_end - timedelta(days=2 * fc.horizon_days)
            pts = [(d, v) for d, v in fc.obs if d >= start]
            c = color[fc.source]
            if pts:
                ax.plot([d for d, _ in pts], [v for _, v in pts], color=c, linewidth=1.1, marker="o", markersize=1.8)
            _draw_forecast(ax, fc, c, pts[-1][0] if pts else None, pts[-1][1] if pts else None, legend=False)
            _mark_forecast_start(ax, ctx.stats.period_end, label=False)
            ax.set_title(f"{fc.label} ({fc.unit})" if fc.unit else fc.label, loc="left", fontsize=8)
            ax.tick_params(labelsize=6.5)
            ax.spines[["top", "right"]].set_visible(False)
            ax.grid(axis="y", color=_GRID, linewidth=0.5)
            ax.xaxis.set_major_locator(matplotlib.dates.MonthLocator())
            ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b"))
            ax.text(0.99, 0.97, f"{fc.model} · confidence {fc.confidence.lower()}", transform=ax.transAxes,
                    ha="right", va="top", fontsize=6, color="#666666")
        for ax in list(axes.flat)[n:]:
            ax.axis("off")
        fig.suptitle("Forecast per Variable: dashed line = forecast, band = 80% / 95% interval",
                     x=0.01, ha="left", fontsize=9)
        return self._save(ctx, fig, "chart_forecast_panels.png")

    def _chart_rain_heatmap(self, ctx: _ReportContext) -> Path | None:
        """Heatmap curah hujan harian: baris = bulan, kolom = tanggal (sequential, satu hue)."""
        import numpy as np
        obs = ctx.stats.raster_band("GPM", "RAIN_24H")
        if len(obs) < 2:
            return None
        months = sorted({(o.day.year, o.day.month) for o in obs})
        grid = np.full((len(months), 31), np.nan)
        for o in obs:
            if o.mean is not None:
                grid[months.index((o.day.year, o.day.month)), o.day.day - 1] = o.mean
        fig, ax = plt.subplots(figsize=(6.8, 0.45 * len(months) + 1.3))
        im = ax.imshow(grid, aspect="auto", cmap="Blues", vmin=0)
        ax.set_yticks(range(len(months)))
        ax.set_yticklabels([f"{rs.MONTH_NAMES[m][:3]} {y}" for y, m in months], fontsize=8)
        ax.set_xticks(range(0, 31, 5))
        ax.set_xticklabels([str(d) for d in range(1, 32, 5)], fontsize=8)
        ax.set_xlabel("Date")
        ax.set_title("GPM Daily Rainfall Heatmap (mm/day, AOI mean)", loc="left", fontsize=10)
        fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02).ax.tick_params(labelsize=7)
        return self._save(ctx, fig, "chart_gpm_heatmap.png")

    def _chart_seasonal_heatmap(self, ctx: _ReportContext, table: list[dict]) -> Path | None:
        """Heatmap musiman: z-score bulanan tiap variabel (diverging, netral di 0)."""
        import numpy as np
        variables = [("vv", "S1 VV (dB)"), ("vh", "S1 VH (dB)"), ("ndvi", "NDVI"), ("ndwi", "NDWI"),
                     ("flood", "Flood (%)"), ("rain", "Rain (mm/day)")]
        variables = [(k, lbl) for k, lbl in variables if sum(r.get(k) is not None for r in table) >= 2]
        if not variables or len(table) < 2:
            return None
        z = np.full((len(variables), len(table)), np.nan)
        raw = [[r.get(k) for r in table] for k, _ in variables]
        for i, vals in enumerate(raw):
            present = [v for v in vals if v is not None]
            mu, sd = mean(present), pstdev(present) or 1.0
            for j, v in enumerate(vals):
                if v is not None:
                    z[i, j] = (v - mu) / sd
        fig, ax = plt.subplots(figsize=(6.8, 0.42 * len(variables) + 1.4))
        im = ax.imshow(z, aspect="auto", cmap="RdBu_r", vmin=-2, vmax=2)
        for i, vals in enumerate(raw):
            for j, v in enumerate(vals):
                if v is not None:
                    ax.text(j, i, f"{v:.2f}" if abs(v) < 10 else f"{v:.1f}", ha="center", va="center", fontsize=6.5,
                            color="white" if abs(z[i, j]) > 1.2 else _INK)
        ax.set_yticks(range(len(variables)))
        ax.set_yticklabels([lbl for _, lbl in variables], fontsize=8)
        ax.set_xticks(range(len(table)))
        ax.set_xticklabels([f"{rs.MONTH_NAMES[r['month']][:3]}\n{r['year']}" for r in table], fontsize=7)
        ax.set_title("Seasonal Pattern: monthly z-score per variable (numbers = raw values)", loc="left", fontsize=10)
        fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="z").ax.tick_params(labelsize=7)
        return self._save(ctx, fig, "chart_seasonal_heatmap.png")

    def _chart_completeness(self, ctx: _ReportContext) -> Path | None:
        st = ctx.stats
        months = _period_months(st)
        if not months:
            return None
        fig, ax = _fig()
        x = range(len(months))
        srcs = [s for s in ("SENTINEL1", "MODIS", "GPM") if st.obs_dates.get(s)]
        if not srcs:
            plt.close(fig)
            return None
        w = 0.8 / len(srcs)
        for i, src in enumerate(srcs):
            days = st.obs_dates[src]
            vals = []
            for y, m in months:
                n_days = _days_in_period_month(st, y, m)
                n_obs = sum(1 for d in days if d.year == y and d.month == m)
                vals.append(n_obs / n_days * 100 if n_days else 0)
            ax.bar([k + (i - (len(srcs) - 1) / 2) * w for k in x], vals, w, label=_SOURCE_LABEL[src],
                   color=_SOURCE_COLORS[src])
        ax.set_xticks(list(x))
        ax.set_xticklabels([f"{rs.MONTH_NAMES[m][:3]} {y}" for y, m in months], fontsize=8)
        ax.set_ylabel("% of days with observations")
        ax.set_ylim(0, 105)
        ax.set_title("Monthly Temporal Completeness per Source", loc="left")
        ax.legend(frameon=False, fontsize=8)
        return self._save(ctx, fig, "chart_completeness.png")

    def _chart_s1_box(self, ctx: _ReportContext) -> Path | None:
        st = ctx.stats
        months = sorted({(m["date"].year, m["date"].month) for m in st.s1_metrics if m["date"] and m["mean_db"] is not None})
        if len(months) < 2:
            return None
        fig, ax = _fig()
        for i, band in enumerate(("VV", "VH")):
            data = [[m["mean_db"] for m in st.s1_metrics if m["band"] == band and _db(m)
                     and m["date"] and (m["date"].year, m["date"].month) == ym] for ym in months]
            pos = [k + (i - 0.5) * 0.36 for k in range(len(months))]
            bp = ax.boxplot([d or [float("nan")] for d in data], positions=pos, widths=0.3, patch_artist=True,
                            showfliers=True, flierprops={"markersize": 3})
            for patch in bp["boxes"]:
                patch.set_facecolor(_CHART_COLORS[i])
                patch.set_alpha(0.75)
            ax.plot([], [], color=_CHART_COLORS[i], linewidth=6, label=band)
        ax.set_xticks(range(len(months)))
        ax.set_xticklabels([f"{rs.MONTH_NAMES[m][:3]} {y}" for y, m in months], fontsize=8)
        ax.set_ylabel("Backscatter mean per scene (dB)")
        ax.set_title("Sentinel-1 Backscatter Distribution per Month", loc="left")
        ax.legend(frameon=False, fontsize=8)
        return self._save(ctx, fig, "chart_s1_monthly_box.png")

    def _chart_s1_quality(self, ctx: _ReportContext) -> Path | None:
        scores = [m["score"] for m in ctx.stats.s1_metrics if m["score"] is not None]
        if len(scores) < 2:
            return None
        fig, ax = _fig()
        ax.hist(scores, bins=20, color=_CHART_COLORS[0], edgecolor="white")
        ax.axvline(mean(scores), color=_INK, linewidth=1, linestyle="--")
        ax.text(mean(scores), ax.get_ylim()[1] * 0.92, f"  mean {mean(scores):.1f}", fontsize=8, color=_INK)
        ax.set_xlabel("quality_score (0-100)")
        ax.set_ylabel("Product count")
        ax.set_title("Sentinel-1 Radiometric Quality Score Distribution", loc="left")
        return self._save(ctx, fig, "chart_s1_quality_hist.png")

    def _chart_flood_classes(self, ctx: _ReportContext) -> Path | None:
        obs = [o for o in ctx.stats.raster_band("MODIS", "FLOOD") if o.classes]
        if len(obs) < 2:
            return None
        months = sorted({(o.day.year, o.day.month) for o in obs})
        fig, ax = _fig()
        bottom = [0.0] * len(months)
        for i, (cls, label) in enumerate(rs.FLOOD_CLASSES.items()):
            vals = []
            for ym in months:
                sel = [o.classes.get(cls, 0) for o in obs if (o.day.year, o.day.month) == ym]
                vals.append(mean(sel) * 100 if sel else 0)
            ax.bar(range(len(months)), vals, 0.6, bottom=bottom, label=label, color=_CHART_COLORS[i],
                   edgecolor="white", linewidth=1)
            bottom = [b + v for b, v in zip(bottom, vals)]
        ax.set_xticks(range(len(months)))
        ax.set_xticklabels([f"{rs.MONTH_NAMES[m][:3]} {y}" for y, m in months], fontsize=8)
        ax.set_ylabel("% valid pixels")
        ax.set_title("MODIS Flood (MCDWD) Class Composition per Month", loc="left")
        ax.legend(frameon=False, fontsize=7, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.12))
        return self._save(ctx, fig, "chart_modis_flood_classes.png")

    def _chart_rain_distribution(self, ctx: _ReportContext) -> Path | None:
        vals = [o.mean for o in ctx.stats.raster_band("GPM", "RAIN_24H") if o.mean is not None]
        if len(vals) < 3:
            return None
        fig, ax = _fig()
        ax.hist(vals, bins=25, color=_SOURCE_COLORS["GPM"], edgecolor="white")
        for q, lbl in ((0.5, "p50"), (0.95, "p95")):
            v = _percentile(vals, q)
            ax.axvline(v, color=_INK, linewidth=1, linestyle="--")
            ax.text(v, ax.get_ylim()[1] * 0.9, f" {lbl}={v:.1f}", fontsize=8, color=_INK)
        ax.set_xlabel("Daily rainfall (mm/day, AOI mean)")
        ax.set_ylabel("Number of days")
        ax.set_title("GPM Daily Rainfall Distribution", loc="left")
        return self._save(ctx, fig, "chart_gpm_distribution.png")

    def _chart_scorecard(self, ctx: _ReportContext) -> Path | None:
        items = [(k, v) for k, v in ctx.scores.items() if v is not None]
        if not items:
            return None
        fig, ax = plt.subplots(figsize=(6.8, 0.4 * len(items) + 1))
        ax.barh([k for k, _ in items][::-1], [v for _, v in items][::-1], color=_CHART_COLORS[0], height=0.55)
        for i, (_, v) in enumerate(items[::-1]):
            ax.text(v + 1, i, f"{v:.0f}", va="center", fontsize=8, color=_INK)
        ax.set_xlim(0, 105)
        ax.set_xlabel("Score (0-100)")
        ax.set_title("Data Health Scorecard", loc="left")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="x", color=_GRID, linewidth=0.6)
        ax.set_axisbelow(True)
        return self._save(ctx, fig, "chart_scorecard.png")

    # -- PDF assembly -------------------------------------------------------

    def _build_pdf(self, ctx: _ReportContext, out_path: Path) -> dict:
        S = _styles()
        ds = ctx.dataset
        st = ctx.stats
        generated_at = datetime.now(timezone.utc)

        doc = _ReportDocTemplate(
            str(out_path),
            pagesize=A4,
            leftMargin=0.75 * inch, rightMargin=0.75 * inch,
            topMargin=0.9 * inch, bottomMargin=0.8 * inch,
            title=f"Trinity DataLab Report - {ds['name']}",
            author="Trinity DataLab Report Engine",
        )
        page_decorator = _make_page_decorator(ds["name"], generated_at, _FONTS["body"])

        chart_dir = ctx.root / "reports" / "_charts"
        chart_dir.mkdir(parents=True, exist_ok=True)
        ctx.issues = _collect_issues(ctx)
        ctx.issues.extend(self._coverage_audit_issues(ds["name"]))
        ctx.scores = _compute_scores(ctx)
        try:
            ctx.forecasts = rf.build_forecasts(st)
        except Exception:  # prakiraan opsional: kegagalannya tidak boleh menggagalkan laporan
            logger.exception("[report] prakiraan gagal untuk dataset %s", self.dataset_id)
            ctx.forecasts = []

        story: list = []
        self._section_cover(story, ctx, S, generated_at)
        self._section_config(story, ctx, S)
        self._section_ablation(story, ctx, S, chart_dir)
        self._section_fusion(story, ctx, S)
        self._section_trends(story, ctx, S)
        if "SENTINEL1" in ds.get("sources", {}):
            self._section_s1(story, ctx, S)
        if "MODIS" in ds.get("sources", {}):
            self._section_modis(story, ctx, S)
        if "GPM" in ds.get("sources", {}):
            self._section_gpm(story, ctx, S)
        self._section_quality(story, ctx, S, chart_dir)
        self._section_outlook(story, ctx, S)
        self._section_json(story, ctx, S)

        try:
            doc.multiBuild(story, onFirstPage=page_decorator, onLaterPages=page_decorator)
        except Exception as exc:
            raise ReportGenerationError(f"Failed to build the PDF: {exc}") from exc

        total_pages = doc.page
        pages = {}
        for i, (title, start) in enumerate(doc.section_pages):
            end = doc.section_pages[i + 1][1] if i + 1 < len(doc.section_pages) else total_pages + 1
            pages[title] = max(end - start, 1)
        return {
            "total_pages": total_pages,
            "pages_per_section": pages,
            "queries": st.queries,
            "charts": ctx.charts,
        }

    # -- Section 1 ------------------------------------------------------------

    def _section_cover(self, story, ctx, S, generated_at) -> None:
        ds, st = ctx.dataset, ctx.stats
        sources = sorted(ds.get("sources", {}).keys())
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph("TRINITY DATALAB REPORT", S.title))
        story.append(Spacer(1, 0.1 * inch))
        story.append(Paragraph(f"Dataset: &ldquo;{_e(ds['name'])}&rdquo;", S.cover_subtitle))
        story.append(Spacer(1, 0.15 * inch))
        story.append(_kv_table([
            ("Generated", f"{generated_at:%Y-%m-%d %H:%M UTC}"),
            ("Dataset ID", str(ds["dataset_uuid"])),
            ("Location", ds.get("location_label") or "-"),
            ("Period", f"{ds.get('date_start')} to {ds.get('date_end')} ({st.period_days} days)"),
            ("Sources", ", ".join(_SOURCE_LABEL.get(s, s) for s in sources) or "-"),
            ("Report Version", REPORT_VERSION),
        ], S))
        story.append(Spacer(1, 0.3 * inch))

        story.append(Paragraph("1. Cover Page &amp; Executive Summary", S.section))
        story.append(Paragraph("Executive Summary", S.sub))
        for para in _executive_summary(ctx):
            story.append(Paragraph(para, S.body))
        story.append(Paragraph("Key Metrics", S.sub2))
        s1_n = len(st.obs_dates.get("SENTINEL1", []))
        rows = [["Metric", "Value", "Notes"]]
        rows += [
            ["Total scenes (pipeline)", str(ds.get("total_scenes", 0)),
             f"{ds.get('completed_scenes', 0)} completed, {ds.get('failed_scenes', 0)} failed"],
            ["S1 acquisition dates", str(s1_n), f"average revisit {_n(st.revisit_days('SENTINEL1'), 1, ' days')}"],
            ["MODIS / GPM observation days", f"{len(st.obs_dates.get('MODIS', []))} / {len(st.obs_dates.get('GPM', []))}",
             f"of {st.period_days} period days"],
            ["Fusion stacks", str(len(st.fusion)), f"strategy {ds.get('fusion_strategy') or '-'}"],
            ["Total data size", _fmt_bytes(ds.get("total_size_bytes", 0)), "all tiers on disk"],
            ["Overall Data Health", _n(ctx.scores.get("Overall Data Health"), 0, "/100"),
             _health_label(ctx.scores.get("Overall Data Health") or 0)],
        ]
        story.append(_table(rows, [2.2 * inch, 1.5 * inch, 3.0 * inch], S))
        story.append(Spacer(1, 0.1 * inch))
        top = [i for i in ctx.issues if i["severity"] in ("HIGH", "MEDIUM")][:3]
        if top:
            story.append(Paragraph("Key findings to watch", S.sub2))
            for i in top:
                story.append(Paragraph(f"⚠ <b>[{i['severity']}] {_SOURCE_LABEL.get(i['source'], i['source'])}</b>: "
                                       f"{_e(i['title'])}", S.bullet))
        story.append(PageBreak())

        toc_heading = ParagraphStyle("TOCPageHeading", parent=S.section)  # tidak dilacak afterFlowable
        story.append(Paragraph("Table of Contents", toc_heading))
        toc = TableOfContents()
        toc.levelStyles = [
            ParagraphStyle("TOCLevel0", fontName=_FONTS["bold"], fontSize=9.5, leading=12, spaceBefore=5),
            ParagraphStyle("TOCLevel1", fontName=_FONTS["body"], fontSize=8.2, leading=10.5, leftIndent=16),
        ]
        toc.tableStyle = TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ])
        story.append(toc)
        story.append(PageBreak())

    # -- Section 2 ------------------------------------------------------------

    def _section_config(self, story, ctx, S) -> None:
        ds, st = ctx.dataset, ctx.stats
        story.append(Paragraph("2. Configuration &amp; Data Ingestion Summary", S.section))
        story.append(Paragraph("2.1 Dataset Overview", S.sub))
        story.append(_kv_table([
            ("Name", ds["name"]),
            ("Description", ds.get("description") or "-"),
            ("Created", f"{ds['created_at']:%Y-%m-%d %H:%M UTC}"),
            ("Last updated", f"{ds['updated_at']:%Y-%m-%d %H:%M UTC}"),
            ("Status", ds["status"]),
            ("Dataset kind", ds.get("dataset_kind") or "STANDARD"),
            ("Fusion strategy", ds.get("fusion_strategy") or "-"),
            ("S1 pairing tolerance", f"±{ds.get('s1_match_tolerance_days')} days"),
            ("Fusion output only", "Yes" if ds.get("fusion_output_only") else "No"),
            ("Preview", ", ".join(ds.get("preview_options") or []) or "-"),
        ], S))

        story.append(Paragraph("2.2 Data Sources Configuration", S.sub))
        story.append(Paragraph(
            "Each source is configured with its own processing level. The &ldquo;in dataset&rdquo; values "
            "below are computed directly from the <i>data_products</i> and <i>satellite_scenes</i> tables and "
            "the COG rasters of this dataset.", S.body))
        for src, levels in sorted(ds.get("sources", {}).items()):
            story.append(Paragraph(f"{_SOURCE_LABEL.get(src, src).upper()} CONFIGURATION DETAIL", S.sub2))
            story.append(_kv_table(_source_spec_rows(ctx, src, levels), S))
            story.append(Spacer(1, 0.05 * inch))

        story.append(CondPageBreak(4 * inch))
        story.append(Paragraph("2.3 Spatial &amp; Temporal Coverage", S.sub))
        bounds = _bbox_bounds(ds.get("bbox_wkt"))
        if bounds:
            w, s, e, n = bounds
            area = _bbox_area_km2(bounds)
            story.append(Paragraph(
                f"The study area <b>{_e(ds.get('location_label') or 'AOI')}</b> is bounded by {abs(s):.3f}°{'S' if s < 0 else 'N'}"
                f"–{abs(n):.3f}°{'S' if n < 0 else 'N'} and {w:.3f}°E–{e:.3f}°E (EPSG:4326), "
                f"area ≈{area:,.0f} km² ({(e - w) * 111.32 * _cos_deg((s + n) / 2):.1f} km × {(n - s) * 110.57:.1f} km).",
                S.body))
            story.append(Preformatted(_ascii_bbox(bounds, ds.get("location_label") or "AOI"), S.pre))
        else:
            story.append(Paragraph(f"Spatial bounds (WKT): {_e(ds.get('bbox_wkt') or '-')}", S.body))

        story.append(Paragraph("Timeline Visualization (daily)", S.sub2))
        story.append(Paragraph(
            "Each row is one month; each character is one date. ▓ = observation present, ░ = none, "
            "blank = outside the dataset period. Gaps &gt;10 days are detected automatically.", S.body))
        for src in ("SENTINEL1", "MODIS", "GPM"):
            if src in ds.get("sources", {}):
                story.append(KeepTogether([
                    Paragraph(f"{_SOURCE_LABEL[src]}", S.bold),
                    Preformatted(_ascii_daily_timeline(st, src), S.pre),
                ]))
        gap_rows = [["Source", "Observation days", "Completeness", "Average revisit", "Gaps >10 days"]]
        for src in ("SENTINEL1", "MODIS", "GPM"):
            if src not in ds.get("sources", {}):
                continue
            gaps = st.gaps(src)
            gap_txt = "; ".join(f"{a:%d %b}–{b:%d %b} ({n} d)" for a, b, n in gaps[:3]) or "None"
            gap_rows.append([_SOURCE_LABEL[src], str(len(st.obs_dates.get(src, []))), _n(st.completeness(src), 1, "%"),
                             _n(st.revisit_days(src), 1, " days"), gap_txt])
        if len(gap_rows) > 1:
            story.append(_table(gap_rows, [1.0 * inch, 1.0 * inch, 0.9 * inch, 1.1 * inch, 2.7 * inch], S))
        chart = self._chart_completeness(ctx)
        if chart:
            story.append(_img(chart))
            story.append(Paragraph("Figure 2.1 — Percentage of days with observations per month.", S.caption))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("2.4 Ingestion Summary &amp; Validation", S.sub))
        ingest_rows = [["Source", "Levels", "Products (DB)", "Files on disk", "Disk size", "Status"]]
        total_files = total_products = 0
        for source, levels in sorted(ds.get("sources", {}).items()):
            per_source = ctx.breakdown["sources"].get(source.lower(), {})
            count = per_source.get("file_count", 0)
            n_products = sum(t["files"] for t in st.tiers.get(source, []))
            total_files += count
            total_products += n_products
            ingest_rows.append([
                _SOURCE_LABEL.get(source, source), ", ".join(levels) or "-", str(n_products), str(count),
                _fmt_bytes(per_source.get("size_bytes", 0)), "✓ Has data" if (count or n_products) else "✗ None yet",
            ])
        ingest_rows.append(["Total", "—", str(total_products), str(total_files),
                            _fmt_bytes(sum(v.get("size_bytes", 0) for v in ctx.breakdown["sources"].values())), "—"])
        if len(ingest_rows) > 2:
            t = _table(ingest_rows, [1.0 * inch, 1.1 * inch, 1.0 * inch, 1.0 * inch, 1.1 * inch, 1.1 * inch], S)
            t.setStyle(TableStyle([("FONTNAME", (0, -1), (-1, -1), _FONTS["bold"])]))
            story.append(t)
        else:
            story.append(Paragraph("No sources have been configured yet.", S.body))

        story.append(Paragraph("Validation Checks Performed", S.sub2))
        for ok, label, detail in _validation_checks(ctx):
            mark = "✓" if ok is True else ("⚠" if ok is None else "✗")
            status = "PASSED" if ok is True else ("WARNING" if ok is None else "FAILED")
            story.append(Paragraph(f"{mark} <b>{label}</b>: {status} — {_e(detail)}", S.bullet))

        story.append(Paragraph("Quality Flag Distribution", S.sub2))
        qrows = [["Source", "Basis", "GOOD/PASS", "ACCEPT.", "MARGINAL", "POOR/FAIL", "Total"]]
        for src in ("SENTINEL1", "MODIS", "GPM"):
            flags = st.quality_flags.get(src)
            if not flags:
                continue
            total = sum(flags.values()) or 1
            if src == "SENTINEL1":
                good, acc, mar, poor = flags.get("PASS", 0) + flags.get("GOOD", 0), flags.get("ACCEPTABLE", 0), \
                    flags.get("MARGINAL", 0), flags.get("FAIL", 0) + flags.get("POOR", 0)
                basis = "quality_flag"
            else:
                good, acc, mar, poor = flags["GOOD"], flags["ACCEPTABLE"], flags["MARGINAL"], flags["POOR"]
                basis = "valid pixels*"
            qrows.append([_SOURCE_LABEL[src], basis] + [f"{v} ({v / total * 100:.0f}%)" for v in (good, acc, mar, poor)]
                         + [str(total)])
        if len(qrows) > 1:
            story.append(KeepTogether([
                _table(qrows, [0.85 * inch, 1.2 * inch, 1.0 * inch, 0.95 * inch, 0.95 * inch, 1.0 * inch, 0.55 * inch], S),
                Paragraph(
                "* MODIS/GPM flags are derived from the fraction of valid pixels per raster: GOOD ≥ 90%, ACCEPTABLE ≥ 70%, "
                "MARGINAL ≥ 50%, POOR &lt; 50% (MCDWD marks cloud-covered/insufficient-data pixels as 255).",
                S.note)]))
        story.extend(_section_break())

    # -- Section 3 ------------------------------------------------------------

    def _section_ablation(self, story, ctx, S, chart_dir) -> None:
        st = ctx.stats
        story.append(Paragraph("3. Processing Level Comparison &amp; Ablation Study", S.section))
        story.append(Paragraph(
            "This section traces each product through the lineage tiers (RAW → ALIGNED → … → COG). "
            "<b>Records</b> = number of product files, <b>Dates</b> = unique dates represented, "
            "<b>Loss</b> = percentage of dates lost compared with the previous tier. Processing time is taken from "
            "<i>processing_jobs</i> (duration completed_at − started_at).", S.body))

        stage_by_name = {j["stage"]: j for j in st.stage_jobs}
        titles = {"SENTINEL1": "3.1 Sentinel-1 Processing Pipeline", "MODIS": "3.2 MODIS Processing Pipeline",
                  "GPM": "3.3 GPM Processing Pipeline"}
        any_src = False
        for src in ("SENTINEL1", "MODIS", "GPM"):
            tiers = st.tiers.get(src)
            if not tiers:
                continue
            any_src = True
            story.append(CondPageBreak(3.5 * inch))
            story.append(Paragraph(titles[src], S.sub))
            story.append(Preformatted(_ascii_flow(src, tiers, stage_by_name), S.pre))
            rows = [["Tier", "Level", "Records", "Dates", "Band", "Avg/file", "σ", "Total", "Loss"]]
            prev_dates = None
            for t in tiers:
                loss = "—" if prev_dates is None else (
                    f"{(prev_dates - t['dates']) / prev_dates * 100:.1f}%" if prev_dates else "—")
                rows.append([t["tier"], t["level"] or "-", str(t["files"]), str(t["dates"]), str(t["bands"]),
                             _fmt_mb(t["avg_mb"]), _fmt_mb(t["std_mb"]) if t["std_mb"] is not None else "—",
                             _fmt_mb(t["size_mb"]), loss])
                prev_dates = t["dates"]
            story.append(Paragraph(f"{_SOURCE_LABEL[src].upper()} PROCESSING ABLATION", S.sub2))
            story.append(_table(rows, [0.95 * inch, 0.8 * inch, 0.6 * inch, 0.6 * inch, 0.45 * inch, 0.85 * inch,
                                       0.7 * inch, 0.85 * inch, 0.5 * inch], S))
            for line in _ablation_narrative(src, tiers, stage_by_name, st):
                story.append(Paragraph(f"• {line}", S.bullet))

        if st.stage_jobs:
            story.append(CondPageBreak(2.5 * inch))
            story.append(Paragraph("Processing Time per Stage", S.sub2))
            rows = [["Stage", "Jobs", "Mean", "Median", "σ", "Max", "Failed"]]
            for j in st.stage_jobs:
                rows.append([j["stage"], str(j["jobs"]), _fmt_sec(j["avg_sec"]), _fmt_sec(j["median_sec"]),
                             _fmt_sec(j["std_sec"]), _fmt_sec(j["max_sec"]), str(j["failed"])])
            story.append(_table(rows, [1.5 * inch, 0.7 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch, 0.7 * inch], S))

        story.append(CondPageBreak(3.5 * inch))
        story.append(Paragraph("3.4 Processing Ablation Summary Chart", S.sub))
        eff = self._chart_effectiveness(ctx)
        abl = self._chart_ablation(ctx, chart_dir)
        if eff:
            story.append(_img(eff))
            story.append(Paragraph("Figure 3.1 — Date retention and average per-file size reduction from the first "
                                   "tier to COG, per source.", S.caption))
        if abl:
            story.append(_img(abl))
            story.append(Paragraph("Figure 3.2 — File counts in the RAW/ vs PROCESSED/ folders on disk.", S.caption))
        fused_levels = {}
        for f in st.fusion:
            fused_levels.setdefault(f["level"], []).append(f)
        if fused_levels:
            story.append(Paragraph("Ablation Fusion Stack: RAW vs PROCESSED", S.sub2))
            fused_tiers = {t["level"]: t for t in st.tiers.get("FUSION", [])}
            rows = [["Input level", "Stacks", "Complete (3 sources)", "Average size", "Total"]]
            for lvl, items in sorted(fused_levels.items()):
                t = fused_tiers.get(lvl, {})
                rows.append([lvl, str(len(items)), str(sum(1 for f in items if f["n_sources"] == 3)),
                             _fmt_mb(t.get("avg_mb")), _fmt_mb(t.get("size_mb"))])
            story.append(KeepTogether([_table(rows, [1.3 * inch, 0.9 * inch, 1.4 * inch, 1.4 * inch, 1.4 * inch], S), Paragraph(
                "A dataset that requests both the RAW and PROCESSED levels produces two stacks per date — this is the "
                "ablation pair that can be used directly to compare model performance with/without the Lee filter.",
                S.note)]))
        if not any_src and not eff and not abl:
            story.append(Paragraph("No products have been recorded yet to compare across levels.", S.body))
        story.extend(_section_break())

    # -- Section 4 ------------------------------------------------------------

    def _section_fusion(self, story, ctx, S) -> None:
        ds, st = ctx.dataset, ctx.stats
        if len(ds.get("sources", {})) <= 1:
            return
        tol = ds.get("s1_match_tolerance_days")
        story.append(Paragraph("4. Fusion Strategy &amp; Temporal Alignment", S.section))
        story.append(Paragraph("4.1 Fusion Methodology", S.sub))
        story.append(Paragraph(
            f"This dataset uses the <b>{_e(ds.get('fusion_strategy') or '-')}</b> strategy. The pipeline aligns "
            "sources at <b>daily</b> resolution: MODIS (daily MCDWD composite) and GPM (daily IMERG) are already "
            "per-date products, so alignment is done per calendar date, not per hour. Sentinel-1 "
            f"is the anchor; an S1 scene may be borrowed from another date within a ±{tol}-day tolerance "
            "(<i>s1_match_tolerance_days</i>) and the actual offset is recorded in <i>fusion_products.s1_offset_days</i>.",
            S.body))
        story.append(_table([
            ["Strategy", "MODIS/GPM dates downloaded", "One HDF5 stack per"],
            ["CO_OCCURRENCE", "S1 dates only", "S1 date"],
            ["FULL_COVERAGE", "Every day", "Every day (S1 borrowed ±tolerance)"],
            ["HYBRID", "Every day", "S1 date"],
        ], [1.5 * inch, 2.5 * inch, 2.7 * inch], S))
        story.append(Paragraph("Fusion Algorithm (pseudocode, following etl/fusion_strategies.py &amp; module9_fusion.py)", S.sub2))
        story.append(Preformatted(_FUSION_PSEUDOCODE.format(tol=tol, strategy=ds.get("fusion_strategy")), S.pre))

        story.append(CondPageBreak(4 * inch))
        story.append(Paragraph("4.2 Temporal Alignment Examples", S.sub))
        for block in _case_studies(ctx):
            story.append(Paragraph(block["title"], S.sub2))
            for para in block["paras"]:
                story.append(Paragraph(para, S.body))
            if block.get("rows"):
                story.append(_table(block["rows"], block["widths"], S))
            if block.get("pre"):
                story.append(Preformatted(block["pre"], S.pre))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("4.3 Temporal Alignment Statistics", S.sub))
        total = len(st.fusion)
        if not total:
            story.append(Paragraph("No fusion stacks have been recorded for this dataset yet.", S.body))
        else:
            by_n = {k: sum(1 for f in st.fusion if f["n_sources"] == k) for k in (3, 2, 1)}
            rows = [["Category", "Number of stacks", "%"]]
            for k, label in ((3, "Complete (3 sources)"), (2, "Partial (2 sources)"), (1, "Single (1 source)")):
                rows.append([label, str(by_n[k]), f"{by_n[k] / total * 100:.1f}%"])
            story.append(_table(rows, [2.5 * inch, 1.5 * inch, 1.5 * inch], S))
            rows = [["Offset from stack date", "n", "Mean", "σ", "Max"]]
            for key, label in (("s1_offset_days", "Sentinel-1"), ("modis_offset_days", "MODIS"),
                               ("gpm_offset_days", "GPM")):
                vals = [abs(f[key]) for f in st.fusion if f[key] is not None]
                rows.append([label, str(len(vals)), _n(mean(vals) if vals else None, 2, " days"),
                             _n(pstdev(vals) if len(vals) > 1 else None, 2, " days"),
                             _n(max(vals) if vals else None, 0, " days")])
            story.append(_table(rows, [2.5 * inch, 0.7 * inch, 1.2 * inch, 1.2 * inch, 1.1 * inch], S))
            s1_days = set(st.obs_dates.get("SENTINEL1", []))
            fused_days = {f["date"] for f in st.fusion}
            aux_only = set(st.obs_dates.get("MODIS", [])) | set(st.obs_dates.get("GPM", []))
            story.append(Paragraph("Gap-fill &amp; coverage summary", S.sub2))
            for line in [
                f"S1 dates that produced a stack: {len(s1_days & fused_days)} of {len(s1_days)} "
                f"({len(s1_days & fused_days) / len(s1_days) * 100:.0f}%)" if s1_days else "No S1 dates.",
                f"Stacks that borrowed S1 from another date (offset ≠ 0): "
                f"{sum(1 for f in st.fusion if (f['s1_offset_days'] or 0) != 0)}",
                f"Days with MODIS/GPM but no fusion stack: {len(aux_only - fused_days)} days "
                f"(a consequence of the {ds.get('fusion_strategy')} strategy, which assembles per S1 date)",
                "Value interpolation (spline/linear) is not performed by this pipeline — missing sources are "
                "written as NaN in the HDF5 stack, not filled with artificial values.",
            ]:
                story.append(Paragraph(f"• {_e(line)}", S.bullet))
            times = _s1_time_of_day(st)
            if times:
                story.append(Paragraph("Sentinel-1 acquisition time (UTC)", S.sub2))
                rows = [["Orbit", "Scenes", "Mean (UTC)", "Range", "Local (WITA, UTC+8)"]]
                for orbit, hrs in sorted(times.items()):
                    h = mean(hrs)
                    rows.append([orbit, str(len(hrs)), _hhmm(h), f"{_hhmm(min(hrs))}–{_hhmm(max(hrs))}", _hhmm((h + 8) % 24)])
                story.append(_table(rows, [1.3 * inch, 0.8 * inch, 1.5 * inch, 1.5 * inch, 1.6 * inch], S))
        story.extend(_section_break())

    # -- Section 5 ------------------------------------------------------------

    def _section_trends(self, story, ctx, S) -> None:
        st = ctx.stats
        story.append(Paragraph("5. Overall Trends &amp; Patterns", S.section))
        story.append(Paragraph(
            "The time series below use actual values: S1 backscatter from <i>quality_metrics</i> (per scene), "
            "and AOI means from the MODIS and GPM COG rasters (per day). Grey bands mark the seasons "
            "(DJF wet season, MAM transition I, JJA dry season, SON transition II). After the dotted line, "
            "the dashed line is a forecast one third of the period ahead, with 80%/95% interval bands — "
            "methodology and conclusions in Section 10.", S.body))
        story.append(Paragraph("5.1 Time-Series Analysis", S.sub))

        vv = [(m["date"], m["mean_db"]) for m in st.s1_metrics if m["band"] == "VV" and m["date"] and _ok(m) and m["mean_db"] is not None]
        vh = [(m["date"], m["mean_db"]) for m in st.s1_metrics if m["band"] == "VH" and m["date"] and _ok(m) and m["mean_db"] is not None]
        ndvi = [(o.day, o.mean) for o in st.raster_band("MODIS", "NDVI") if o.mean is not None]
        ndwi = [(o.day, o.mean) for o in st.raster_band("MODIS", "NDWI") if o.mean is not None]
        rain = [(o.day, o.mean) for o in st.raster_band("GPM", "RAIN_24H") if o.mean is not None]

        blocks = [
            ("Chart 1 — Sentinel-1 VV/VH Backscatter (PASS scenes, unit dB)", "chart_s1_trend.png", "Sentinel-1 Backscatter per Scene",
             "Backscatter mean (dB)", {"VV": vv, "VH": vh}, "line", "SENTINEL1"),
            ("Chart 2 — MODIS NDVI/NDWI", "chart_modis_trend.png", "MODIS: Daily AOI Mean",
             "Index (−1…1)", {"NDVI": ndvi, "NDWI": ndwi}, "line", "MODIS"),
            ("Chart 3 — GPM Daily Rainfall", "chart_gpm_trend.png", "GPM IMERG: Daily Rainfall",
             "mm/day (AOI mean)", {"RAIN_24H": rain}, "bar", "GPM"),
        ]
        n_charts = 0
        fc_by = {f.key: f for f in ctx.forecasts}
        fc_for = {"VV": fc_by.get("vv"), "VH": fc_by.get("vh"), "NDVI": fc_by.get("ndvi"),
                  "NDWI": fc_by.get("ndwi"), "RAIN_24H": fc_by.get("rain")}
        for heading, name, title, ylabel, series, kind, src in blocks:
            fcs = {k: fc_for[k] for k in series if fc_for.get(k) is not None}
            chart = self._chart_series(ctx, name, title, ylabel, series, kind, forecasts=fcs)
            story.append(CondPageBreak(4 * inch))
            story.append(Paragraph(heading, S.sub2))
            if chart:
                n_charts += 1
                story.append(_img(chart))
            else:
                story.append(Paragraph("Not enough data points (minimum 2) for this series.", S.note))
            for para in _trend_narrative(st, src):
                story.append(Paragraph(para, S.body))

        story.append(CondPageBreak(4 * inch))
        story.append(Paragraph("Monthly Summary (all sources)", S.sub2))
        table = _monthly_combined(st)
        if table:
            rows = [["Month", "S1", "VV dB", "VH dB", "NDVI", "NDWI", "Flood %", "Rain/day", "Total mm"]]
            for r in table:
                rows.append([f"{rs.MONTH_NAMES[r['month']][:3]} {r['year']}", str(r["s1_n"]), _n(r["vv"], 2), _n(r["vh"], 2),
                             _n(r["ndvi"], 3), _n(r["ndwi"], 3), _n(r["flood"], 2), _n(r["rain"], 2), _n(r["rain_sum"], 1)])
            story.append(_table(rows, [0.85 * inch, 0.6 * inch, 0.7 * inch, 0.7 * inch, 0.6 * inch, 0.65 * inch,
                                       0.7 * inch, 0.9 * inch, 0.7 * inch], S))
            heat = self._chart_seasonal_heatmap(ctx, table)
            if heat:
                story.append(_img(heat, h=2.9))
                story.append(Paragraph("Figure 5.4 — Seasonal heatmap: colour = monthly z-score of each variable "
                                       "(red above the period mean, blue below).", S.caption))
        rheat = self._chart_rain_heatmap(ctx)
        if rheat:
            story.append(CondPageBreak(3 * inch))
            story.append(_img(rheat, h=0.45 * len({(o.day.year, o.day.month) for o in st.raster_band('GPM', 'RAIN_24H')}) + 1.3))
            story.append(Paragraph("Figure 5.5 — Daily rainfall heatmap (month × date).", S.caption))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("5.2 Summary Findings", S.sub))
        for i, (title, text) in enumerate(_key_findings(ctx, table), 1):
            story.append(Paragraph(f"<b>{i}. {title}</b> — {text}", S.bullet))
        story.extend(_section_break())

    # -- Section 6 ------------------------------------------------------------

    def _section_s1(self, story, ctx, S) -> None:
        st = ctx.stats
        story.append(Paragraph("6. Sentinel-1 Deep Dive", S.section))
        if not st.s1_metrics and not st.s1_scenes:
            story.append(Paragraph("There are no Sentinel-1 scenes or metrics for this dataset yet.", S.body))
            story.extend(_section_break())
            return
        story.append(Paragraph("6.1 SAR Backscatter Analysis", S.sub))
        pol = {}
        for band in ("VV", "VH"):
            vals = [m["mean_db"] for m in st.s1_metrics if m["band"] == band and _db(m)]
            good = [m["mean_db"] for m in st.s1_metrics if m["band"] == band and m["mean_db"] is not None and _ok(m)]
            spk = [m["speckle"] for m in st.s1_metrics if m["band"] == band and m["speckle"] is not None]
            pol[band] = {"vals": vals, "good": good, "speckle": spk}
        rows = [["Characteristic", "VV", "VH"]]
        for label, fn in [
            ("Products in dB units", lambda d: str(len(d["vals"]))),
            ("Mean (all scenes)", lambda d: _n(mean(d["vals"]) if d["vals"] else None, 2, " dB")),
            ("Mean (PASS only)", lambda d: _n(mean(d["good"]) if d["good"] else None, 2, " dB")),
            ("Median", lambda d: _n(median(d["vals"]) if d["vals"] else None, 2, " dB")),
            ("σ across scenes", lambda d: _n(pstdev(d["vals"]) if len(d["vals"]) > 1 else None, 2, " dB")),
            ("Range (min – max)", lambda d: f"{min(d['vals']):.2f} – {max(d['vals']):.2f} dB" if d["vals"] else "—"),
            ("p5 – p95", lambda d: f"{_percentile(d['vals'], .05):.2f} – {_percentile(d['vals'], .95):.2f} dB" if d["vals"] else "—"),
            ("Mean speckle index", lambda d: _n(mean(d["speckle"]) if d["speckle"] else None, 3)),
        ]:
            rows.append([label, fn(pol["VV"]), fn(pol["VH"])])
        story.append(_table(rows, [2.6 * inch, 2.0 * inch, 2.0 * inch], S))
        ratio = _vv_vh_ratio(st)
        story.append(Paragraph("VV/VH Ratio (Decomposition Index)", S.sub2))
        if ratio["all"]:
            story.append(Paragraph(
                f"The VV−VH ratio (dB) is computed per scene that has both polarisations: mean "
                f"<b>{mean(ratio['all']):.2f} dB</b> (σ={pstdev(ratio['all']):.2f}, n={len(ratio['all'])}). "
                "A high ratio indicates surface scattering (bare soil, calm water), whereas a low ratio "
                "indicates volume scattering (dense vegetation), which raises VH relative to VV.", S.body))
            rows = [["Month", "n", "Mean VV−VH (dB)", "σ"]]
            for (y, m), vals in sorted(ratio["monthly"].items()):
                rows.append([f"{rs.MONTH_NAMES[m]} {y}", str(len(vals)), f"{mean(vals):.2f}",
                             f"{pstdev(vals):.2f}" if len(vals) > 1 else "—"])
            story.append(_table(rows, [2.0 * inch, 0.8 * inch, 2.0 * inch, 1.5 * inch], S))
        else:
            story.append(Paragraph("No scene has both VV and VH metrics.", S.note))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("SEASONAL PATTERN TABLE (Monthly, VV)", S.sub2))
        vv_months = [r for r in st.s1_monthly if r["band"] == "VV"]
        if vv_months:
            overall = mean(r["mean"] for r in vv_months if r["mean"] is not None)
            rows = [["Month", "n", "VV dB", "σ", "Min", "Max", "Valid", "Interpretation"]]
            for r in vv_months:
                rows.append([f"{rs.MONTH_NAMES[r['month']]} {r['year']}", str(r["n"]), _n(r["mean"], 2), _n(r["std"], 2),
                             _n(r["min"], 2), _n(r["max"], 2), _n((r["valid"] or 0) * 100, 0),
                             Paragraph(_month_interpretation(r, overall), S.cell)])
            story.append(_table(rows, [0.95 * inch, 0.35 * inch, 0.8 * inch, 0.5 * inch, 0.55 * inch, 0.5 * inch,
                                       0.55 * inch, 2.5 * inch], S))
        box = self._chart_s1_box(ctx)
        if box:
            story.append(_img(box))
            story.append(Paragraph("Figure 6.1 — Backscatter distribution per month (box = quartiles).", S.caption))

        story.append(CondPageBreak(3.5 * inch))
        story.append(Paragraph("6.2 Scene Acquisition &amp; Coverage", S.sub))
        story.append(Paragraph("Orbit Analysis Statistics", S.sub2))
        times = _s1_time_of_day(st)
        rows = [["Orbit", "Scenes", "UTC hour", "Incidence", "Rel. orbit", "First", "Last"]]
        for o in st.s1_orbit:
            hrs = times.get(o["orbit"], [])
            inc = f"{o['inc_near']:.1f}° – {o['inc_far']:.1f}°" if o["inc_near"] is not None and o["inc_far"] is not None else "not recorded"
            rows.append([o["orbit"], str(o["scenes"]), _hhmm(mean(hrs)) if hrs else "—", inc,
                         str(o["relative_orbits"]) if o["relative_orbits"] else "not recorded",
                         f"{o['first']:%Y-%m-%d}" if o["first"] else "—", f"{o['last']:%Y-%m-%d}" if o["last"] else "—"])
        story.append(_table(rows, [1.0 * inch, 0.5 * inch, 1.0 * inch, 1.1 * inch, 1.0 * inch, 0.9 * inch, 0.9 * inch], S))
        story.append(Paragraph(
            "Geometry: Sentinel-1 IW is a right-looking SAR. Ascending passes cross in the local evening, "
            "descending passes in the morning — the hour pattern above is consistent with that. The incidence "
            "angle/relative orbit columns exist in the schema but were not filled in by the download module "
            "for this dataset's scenes.", S.note))
        story.append(Paragraph("Scene Acquisition Timeline", S.sub2))
        story.append(Preformatted(_ascii_month_grid(st), S.pre))
        intervals = _intervals(st.obs_dates.get("SENTINEL1", []))
        if intervals:
            story.append(Paragraph(
                f"Interval between acquisition dates: mean {mean(intervals):.1f} days, median {median(intervals):.0f} days, "
                f"maximum {max(intervals)} days. {len(st.s1_scenes)} scenes recorded on "
                f"{len(st.obs_dates.get('SENTINEL1', []))} unique dates.", S.body))

        story.append(CondPageBreak(3.5 * inch))
        story.append(Paragraph("6.3 Data Quality Metrics", S.sub))
        story.append(Paragraph("Radiometric Quality", S.sub2))
        scores = [m["score"] for m in st.s1_metrics if m["score"] is not None]
        consist = [m["consistent"] for m in st.s1_metrics if m["consistent"] is not None]
        flags = st.quality_flags.get("SENTINEL1", {})
        spk_all = [m["speckle"] for m in st.s1_metrics if m["speckle"] is not None]
        rows = [["Metric", "Value", "Source"]]
        rows += [
            ["Mean quality score", _n(mean(scores) if scores else None, 1, "/100"), "quality_metrics.quality_score"],
            ["Minimum / maximum score", f"{min(scores):.1f} / {max(scores):.1f}" if scores else "—", "quality_metrics.quality_score"],
            ["PASS / FAIL", f"{flags.get('PASS', 0)} / {flags.get('FAIL', 0)}", "quality_metrics.quality_flag"],
            ["Radiometric consistency = TRUE", f"{sum(consist)}/{len(consist)} ({sum(consist) / len(consist) * 100:.0f}%)" if consist else "—",
             "quality_metrics.radiometric_consistency"],
            ["Mean speckle index (σ)", f"{mean(spk_all):.3f} ({pstdev(spk_all):.3f})" if len(spk_all) > 1 else "—",
             "quality_metrics.speckle_index"],
        ]
        story.append(_table(rows, [2.3 * inch, 1.8 * inch, 2.6 * inch], S))
        story.append(Paragraph("Geometric / Coverage Quality", S.sub2))
        valid = [m["valid_frac"] for m in st.s1_metrics if m["valid_frac"] is not None]
        if valid:
            rows = [["Valid pixel fraction", "Value"],
                    ["Mean", f"{mean(valid) * 100:.1f}%"],
                    ["Median", f"{median(valid) * 100:.1f}%"],
                    ["Products with valid < 50%", f"{sum(1 for v in valid if v < .5)} of {len(valid)}"],
                    ["Products with valid ≥ 90%", f"{sum(1 for v in valid if v >= .9)} of {len(valid)}"]]
            story.append(_table(rows, [3.3 * inch, 3.3 * inch], S))
        story.append(Paragraph(
            "Absolute geolocation and orthorectification accuracy (metres) is not measured by the pipeline — there are "
            "no ground control points in the schema. The valid pixel fraction is used as a proxy for footprint "
            "coverage over the AOI.", S.note))
        story.append(Paragraph("Coherence &amp; Interferometry", S.sub2))
        story.append(Paragraph(
            "The pipeline processes GRD (intensity) products, not SLC, so interferometric coherence cannot be "
            "computed from this data. As a substitute, the temporal stability of backscatter per season is reported below.", S.body))
        rows = [["Season", "n VV", "Mean VV (dB)", "σ VV (dB)", "Coeff. of variation"]]
        for key, label, months in rs.SEASONS:
            vals = [m["mean_db"] for m in st.s1_metrics if m["band"] == "VV" and m["mean_db"] is not None
                    and m["date"] and m["date"].month in months and _ok(m)]
            if vals:
                cv = pstdev(vals) / abs(mean(vals)) if len(vals) > 1 and mean(vals) else None
                rows.append([label, str(len(vals)), f"{mean(vals):.2f}", _n(pstdev(vals) if len(vals) > 1 else None, 2),
                             _n(cv, 3)])
        if len(rows) > 1:
            story.append(_table(rows, [2.2 * inch, 0.7 * inch, 1.2 * inch, 1.2 * inch, 1.2 * inch], S))
        hist = self._chart_s1_quality(ctx)
        if hist:
            story.append(_img(hist))
            story.append(Paragraph("Figure 6.2 — Histogram of the radiometric quality score per product.", S.caption))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("6.4 Data Artifacts &amp; Known Issues", S.sub))
        for block in _s1_artifacts(st):
            story.append(Paragraph(block["title"], S.sub2))
            for line in block["lines"]:
                story.append(Paragraph(f"• {line}", S.bullet))
            if block.get("rows"):
                story.append(_table(block["rows"], block["widths"], S))
        self._previews(story, ctx, S, "SENTINEL1")
        story.extend(_section_break())

    # -- Section 7 ------------------------------------------------------------

    def _section_modis(self, story, ctx, S) -> None:
        st = ctx.stats
        story.append(Paragraph("7. MODIS Deep Dive", S.section))
        story.append(Paragraph("7.1 Product Characteristics &amp; Surface Index Analysis", S.sub))
        story.append(_kv_table([
            ("Source product", ", ".join(st.nasa_products.get("MODIS", [])) or "MCDWD_L3_F2_NRT (pipeline default)"),
            ("Description", "MODIS NRT Global Flood Product (LANCE), 2-day Terra+Aqua composite"),
            ("Spatial resolution", "250 m (MCDWD product)"),
            ("Bands in dataset", ", ".join(sorted(st.raster.get("MODIS", {}).keys())) or "-"),
            ("Observation days", f"{len(st.obs_dates.get('MODIS', []))} of {st.period_days}"),
            ("FLOOD classes", "; ".join(f"{k}={v}" for k, v in rs.FLOOD_CLASSES.items()) + "; 255=insufficient data"),
        ], S))
        story.append(Paragraph(
            "Note: the original specification gave Land Surface Temperature (MOD11A2) as an example. This dataset's "
            "pipeline does not download LST; the statistics below use the bands that are actually present "
            "(NDVI, NDWI, FLOOD).", S.note))
        for band, label in (("NDVI", "NDVI"), ("NDWI", "NDWI")):
            rows_m = st.monthly_raster("MODIS", band)
            if not rows_m:
                continue
            story.append(Paragraph(f"Monthly {label} Statistics", S.sub2))
            rows = [["Month", "Days", f"Mean {label}", "σ across days", "Max pixel", "Min pixel", "Valid %"]]
            for r in rows_m:
                rows.append([f"{rs.MONTH_NAMES[r['month']]} {r['year']}", str(r["n"]), _n(r["mean"], 3), _n(r["std"], 3),
                             _n(r["max"], 2), _n(r["min"], 2), _n(r["valid"] * 100, 0)])
            story.append(_table(rows, [1.3 * inch, 0.6 * inch, 1.0 * inch, 1.0 * inch, 0.9 * inch, 0.9 * inch, 0.8 * inch], S))
            srows = [r for r in st.seasonal_raster("MODIS", band) if r["n"]]
            if srows:
                story.append(Paragraph(
                    f"Per season: " + "; ".join(f"{r['label']} mean {r['mean']:.3f} (n={r['n']})" for r in srows if r["mean"] is not None)
                    + ".", S.body))

        story.append(CondPageBreak(3.5 * inch))
        story.append(Paragraph("7.2 Cloud Cover &amp; Quality Assessment", S.sub))
        flood = st.raster_band("MODIS", "FLOOD")
        if flood:
            clear = [o.valid_frac for o in flood]
            story.append(Paragraph(
                f"Pixels coded 255 (insufficient data — mainly cloud cover) are used as a cloud proxy. "
                f"Mean cloud-free pixels: <b>{mean(clear) * 100:.1f}%</b>; days with &lt;50% valid pixels: "
                f"<b>{sum(1 for v in clear if v < .5)}</b> of {len(clear)}.", S.body))
            rows = [["Season", "Days", "Cloud-free (mean)", "Days valid < 50%", "Days valid ≥ 90%"]]
            for key, label, months in rs.SEASONS:
                sel = [o.valid_frac for o in flood if o.day.month in months]
                if sel:
                    rows.append([label, str(len(sel)), f"{mean(sel) * 100:.1f}%", str(sum(1 for v in sel if v < .5)),
                                 str(sum(1 for v in sel if v >= .9))])
            story.append(_table(rows, [2.0 * inch, 0.7 * inch, 1.5 * inch, 1.2 * inch, 1.2 * inch], S))
            rows = [["Month", "Days", "Cloud-free", "No water", "Surface water", "Recurrent flood", "Anomalous flood"]]
            months = sorted({(o.day.year, o.day.month) for o in flood})
            for ym in months:
                sel = [o for o in flood if (o.day.year, o.day.month) == ym and o.classes]
                if not sel:
                    continue
                rows.append([f"{rs.MONTH_NAMES[ym[1]][:3]} {ym[0]}", str(len(sel)), f"{mean(o.valid_frac for o in sel) * 100:.0f}%"]
                            + [f"{mean(o.classes.get(c, 0) for o in sel) * 100:.2f}%" for c in rs.FLOOD_CLASSES])
            story.append(Paragraph("FLOOD class composition per month (% of valid pixels)", S.sub2))
            story.append(_table(rows, [0.9 * inch, 0.5 * inch, 0.9 * inch, 1.0 * inch, 1.1 * inch, 1.1 * inch, 1.1 * inch], S))
            chart = self._chart_flood_classes(ctx)
            if chart:
                story.append(_img(chart))
                story.append(Paragraph("Figure 7.1 — MCDWD class composition per month.", S.caption))
        else:
            story.append(Paragraph("There are no FLOOD rasters to analyse.", S.note))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("7.3 Validation &amp; Accuracy", S.sub))
        story.append(Paragraph(
            "The schema does not store ground-truth station data, so RMSE/bias against field measurements cannot "
            "be computed. Instead a <b>cross-sensor consistency test</b> is performed — Pearson correlation "
            "between daily series that should be physically related:", S.body))
        rows = [["Variable pair", "n days", "Pearson r", "Physical expectation", "Result"]]
        for a, b, label, expect in _consistency_pairs():
            r, n = _pair_corr(st, a, b)
            verdict = "—" if r is None else ("✓ consistent" if (r > 0.2 if expect > 0 else r < -0.2) else "⚠ weak")
            rows.append([label, str(n), _n(r, 3), "positive" if expect > 0 else "negative", verdict])
        story.append(_table(rows, [2.4 * inch, 0.7 * inch, 0.9 * inch, 1.2 * inch, 1.4 * inch], S))
        self._previews(story, ctx, S, "MODIS")
        story.extend(_section_break())

    # -- Section 8 ------------------------------------------------------------

    def _section_gpm(self, story, ctx, S) -> None:
        st = ctx.stats
        story.append(Paragraph("8. GPM Deep Dive", S.section))
        story.append(Paragraph("8.1 Precipitation Product Analysis", S.sub))
        rain = st.raster_band("GPM", "RAIN_24H")
        vals = [o.mean for o in rain if o.mean is not None]
        story.append(_kv_table([
            ("Source product", ", ".join(st.nasa_products.get("GPM", [])) or "GPM_3IMERGDF"),
            ("Description", "IMERG Final Run, daily aggregate (gauge-adjusted)"),
            ("Resolution", "0.1° × 0.1° (~11 km); daily"),
            ("Product latency", "~3.5 months (Final Run)"),
            ("Bands in dataset", ", ".join(sorted(st.raster.get("GPM", {}).keys())) or "-"),
            ("Pixels per raster (AOI)", _gpm_pixels(rain)),
        ], S))
        if vals:
            rows = [["Statistic (RAIN_24H, AOI mean)", "Value"],
                    ["Mean", f"{mean(vals):.2f} mm/day"], ["Median", f"{median(vals):.2f} mm/day"],
                    ["σ", f"{pstdev(vals):.2f} mm/day"], ["p90 / p95 / p99",
                     f"{_percentile(vals, .9):.1f} / {_percentile(vals, .95):.1f} / {_percentile(vals, .99):.1f} mm/day"],
                    ["Maximum (AOI mean)", f"{max(vals):.1f} mm/day"],
                    ["Maximum (pixel)", f"{max(o.max for o in rain if o.max is not None):.1f} mm/day"],
                    ["Rainy days (≥0.1 mm)", f"{sum(1 for v in vals if v >= rs.RAINY_THRESHOLD_MM)} of {len(vals)} "
                     f"({sum(1 for v in vals if v >= rs.RAINY_THRESHOLD_MM) / len(vals) * 100:.0f}%)"],
                    ["Total accumulation over the period", f"{sum(vals):.0f} mm"]]
            story.append(_table(rows, [3.3 * inch, 3.3 * inch], S))
            story.append(Paragraph("Monthly Precipitation", S.sub2))
            rows = [["Month", "Days", "Mean mm/day", "σ", "Max px", "Total mm", "Rainy days"]]
            by_month = {}
            for o in rain:
                if o.mean is not None:
                    by_month.setdefault((o.day.year, o.day.month), []).append(o)
            for (y, m), obs in sorted(by_month.items()):
                mv = [o.mean for o in obs]
                rows.append([f"{rs.MONTH_NAMES[m]} {y}", str(len(obs)), f"{mean(mv):.2f}", f"{pstdev(mv):.2f}" if len(mv) > 1 else "—",
                             f"{max(o.max for o in obs):.1f}", f"{sum(mv):.0f}", str(sum(1 for v in mv if v >= rs.RAINY_THRESHOLD_MM))])
            story.append(_table(rows, [1.3 * inch, 0.6 * inch, 1.0 * inch, 0.7 * inch, 1.0 * inch, 1.0 * inch, 0.9 * inch], S))
            chart = self._chart_rain_distribution(ctx)
            if chart:
                story.append(_img(chart))
                story.append(Paragraph("Figure 8.1 — Daily rainfall distribution (dashed line = percentile).", S.caption))
            acc = []
            for band in ("RAIN_72H", "RAIN_7D"):
                bv = [o.mean for o in st.raster_band("GPM", band) if o.mean is not None]
                if bv:
                    acc.append([band, str(len(bv)), f"{mean(bv):.1f}", f"{max(bv):.1f}", f"{_percentile(bv, .95):.1f}"])
            if acc:
                story.append(Paragraph("Multi-day accumulation", S.sub2))
                story.append(_table([["Band", "Days", "Mean (mm)", "Max (mm)", "p95 (mm)"]] + acc,
                                    [1.4 * inch, 0.9 * inch, 1.3 * inch, 1.3 * inch, 1.3 * inch], S))
        else:
            story.append(Paragraph("There are no RAIN_24H rasters to analyse.", S.note))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("8.2 Extreme Event Analysis", S.sub))
        events = _extreme_events(st)
        if events:
            story.append(Paragraph(
                "The five days with the highest AOI-mean rainfall. <b>Duration</b> = number of consecutive days with rain "
                "≥10 mm/day that contain the peak day; <b>72-hour accumulation</b> is taken from the RAIN_72H band.", S.body))
            rows = [["Rank", "Date", "AOI peak", "Max px", "72 h", "Duration", "Season"]]
            for i, e in enumerate(events, 1):
                rows.append([str(i), f"{e['day']:%Y-%m-%d}", f"{e['mean']:.1f}", _n(e["max"], 1), _n(e["r72"], 1, " mm"),
                             f"{e['duration']} days", e["season"]])
            story.append(_table(rows, [0.5 * inch, 1.0 * inch, 1.3 * inch, 0.9 * inch, 1.2 * inch, 0.8 * inch, 1.0 * inch], S))
            story.append(Paragraph(
                "The cause (monsoon, cyclone, local convection) cannot be determined from rainfall data alone; "
                "the season column is given as context.", S.note))
            flood_by_day = {o.day: o for o in st.raster_band("MODIS", "FLOOD")}
            resp = []
            for e in events:
                after = [flood_by_day.get(e["day"] + timedelta(days=k)) for k in range(0, 4)]
                after = [o for o in after if o and o.mean is not None and o.valid_frac >= .3]
                if after:
                    resp.append(f"{e['day']:%d %b}: MODIS flood max {max(o.mean for o in after):.2f}% of pixels within 0–3 days")
            if resp:
                story.append(Paragraph("MODIS flood response after extreme events: " + "; ".join(resp) + ".", S.body))
        else:
            story.append(Paragraph("There is no rainfall data for extreme-event analysis.", S.note))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("8.3 Seasonal Precipitation Breakdown", S.sub))
        seasons = st.seasonal_raster("GPM", "RAIN_24H")
        total_sum = sum(r["sum"] or 0 for r in seasons)
        rows = [["Season", "Days", "Mean", "Max AOI", "Max px", "Days ≥0.1", "% of total"]]
        for r in seasons:
            if not r["n"]:
                rows.append([r["label"], "0", "—", "—", "—", "—", "outside the period"])
                continue
            rows.append([r["label"], str(r["n"]), _n(r["mean"], 2), _n(r["max_mean"], 1), _n(r["max_px"], 1),
                         str(r["wet_days"]), f"{(r['sum'] or 0) / total_sum * 100:.0f}%" if total_sum else "—"])
        story.append(_table(rows, [1.7 * inch, 0.5 * inch, 1.0 * inch, 0.8 * inch, 0.9 * inch, 0.9 * inch, 1.0 * inch], S))
        self._previews(story, ctx, S, "GPM")
        story.extend(_section_break())

    # -- Section 9 ------------------------------------------------------------

    def _section_quality(self, story, ctx, S, chart_dir) -> None:
        story.append(Paragraph("9. Data Quality &amp; Warnings", S.section))
        story.append(Paragraph("9.1 Data Health Scorecard", S.sub))
        lines = []
        for k, v in ctx.scores.items():
            if v is None:
                continue
            filled = int(round(v / 10))
            lines.append(f"{k:<24} {v:5.0f}/100  {'█' * filled}{'░' * (10 - filled)}  {_health_label(v)}")
        if lines:
            story.append(Preformatted("\n".join(lines), S.pre))
            for k, formula in _SCORE_FORMULAS.items():
                if ctx.scores.get(k) is not None:
                    story.append(Paragraph(f"• <b>{k}</b>: {formula}", S.bullet))
            chart = self._chart_scorecard(ctx)
            if chart:
                story.append(_img(chart, h=2.4))
        else:
            story.append(Paragraph("There is no data to assess for quality.", S.body))
        if ctx.quality:
            rows = [["Source", "Score type", "Score", "Flag", "Products", "Scenes/days"]]
            for item in ctx.quality:
                rows.append([_SOURCE_LABEL.get(item["source"], item["source"]),
                             "Radiometric" if item["kind"] == "RADIOMETRIC" else "Coverage band",
                             f"{item['quality_score']}/100", item["quality_flag"], str(item["product_count"]), str(item["scene_count"])])
            story.append(_table(rows, [1.1 * inch, 1.3 * inch, 0.9 * inch, 1.1 * inch, 1.0 * inch, 1.1 * inch], S))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("9.2 Source-Specific Issues &amp; Warnings", S.sub))
        for src in ("SENTINEL1", "MODIS", "GPM", "FUSION"):
            items = [i for i in ctx.issues if i["source"] == src]
            if src != "FUSION" and src not in ctx.dataset.get("sources", {}):
                continue
            if src == "FUSION" and not items:
                continue
            label = _SOURCE_LABEL.get(src, "Fusion")
            story.append(Paragraph(f"{label.upper()} QUALITY ISSUES", S.sub2))
            worst = _worst_severity(items)
            if not items:
                story.append(Paragraph("✓ EXCELLENT — no issues were detected by the automated checks.", S.body))
            else:
                header = {"HIGH": "✗ CRITICAL ISSUES PRESENT", "MEDIUM": "⚠ ISSUES REQUIRE ATTENTION",
                          "LOW": "✓ GOOD — minor issues only"}[worst]
                story.append(Paragraph(f"<b>{header}</b>", S.body))
                for n, i in enumerate(items, 1):
                    story.append(Paragraph(f"<b>{n}. {_e(i['title'])}</b>", S.bullet))
                    for k in ("detail", "action", "status"):
                        if i.get(k):
                            story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;• {k.capitalize() if k != 'action' else 'Recommended action'}: "
                                                   f"{_e(i[k])}", S.bullet))
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;• Severity: <b>{i['severity']}</b>", S.bullet))
            verdict = {"HIGH": "✗ FIX BEFORE USE", "MEDIUM": "⚠ APPROVED WITH CAVEATS", "LOW": "✓ APPROVED FOR USE",
                       None: "✓ APPROVED FOR USE"}[worst]
            story.append(Paragraph(f"Recommendation: <b>{verdict}</b>", S.body))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("9.3 Recommendations for Use", S.sub))
        rec = _usage_recommendations(ctx)
        for title, mark, items in (("SUITABLE FOR", "✓", rec["suitable"]), ("USE WITH CAUTION", "⚠", rec["caution"]),
                                   ("NOT SUITABLE FOR", "✗", rec["not_suitable"])):
            story.append(Paragraph(f"{mark} {title}", S.sub2))
            for t in items:
                story.append(Paragraph(f"• {_e(t)}", S.bullet))
        storage_chart = self._chart_storage_by_tier(ctx, chart_dir)
        if storage_chart is not None:
            story.append(CondPageBreak(3 * inch))
            story.append(Paragraph("Storage per Tier", S.sub2))
            story.append(_img(storage_chart))
        story.extend(_section_break())

    # -- Section 10: kesimpulan & prakiraan ---------------------------------------

    def _section_outlook(self, story, ctx, S) -> None:
        st = ctx.stats
        fcs = ctx.forecasts
        h = rf.horizon_for(st.period_days) if st.period_days else 0
        end = st.period_end
        story.append(Paragraph("10. Conclusions: Current Conditions &amp; Forecast", S.section))
        if not fcs or end is None:
            story.append(Paragraph(
                "This dataset does not have enough data (at least 6 observations per variable) to produce a forecast. "
                "Conclusions about current conditions can be read in Sections 5 and 9.", S.body))
            story.extend(_section_break())
            return
        horizon_end = end + timedelta(days=h)

        story.append(Paragraph("10.1 Forecast Methodology", S.sub))
        story.append(Paragraph(
            f"Forecast horizon = one third of the dataset period length: {st.period_days} days → <b>{h} days</b> "
            f"({end + timedelta(days=1)} to {horizon_end}). Only this dataset's data is used. Four simple "
            "models compete — <i>naive</i> (the last value continues), the period <i>mean</i>, <i>SES</i> "
            "(exponential smoothing) and <i>Holt damped</i> (a tapering trend). Each model is trained on the first "
            "2/3 of the data and tested on the last 1/3 — the test length equals the horizon, so the backtest error "
            "represents true forecasting ability. The model with the smallest error is refit on all the data.", S.body))
        story.append(Paragraph(
            "<b>Skill</b> = 1 − MAE(model)/MAE(naive): 0 means no better than guessing &ldquo;the latest condition "
            "continues&rdquo;. Confidence: <b>High</b> (skill ≥ 0.20 and ≥ 30 days of data), <b>Medium</b> (skill "
            "0.05–0.20), <b>Low</b> (skill &lt; 0.05 or a backtest is not possible). The 80%/95% bands are calibrated "
            "from the backtest error and clipped to the variable's physical range.", S.body))
        rows = [["Variable", "Model", "Train/test (days)", "Model MAE", "Naive MAE", "Skill", "Confidence"]]
        for f in fcs:
            bt = f.backtest
            rows.append([f.label, f.model, f"{bt.get('train_days', '—')}/{bt.get('test_days', '—')}",
                         rf.fmt(bt.get("mae"), "", 3), rf.fmt(bt.get("mae_naive"), "", 3),
                         rf.fmt(bt.get("skill"), "", 2), f.confidence])
        story.append(_table(rows, [1.6 * inch, 0.95 * inch, 0.95 * inch, 0.8 * inch, 0.8 * inch, 0.6 * inch, 0.8 * inch], S))

        chart = self._chart_forecast_panels(ctx)
        if chart:
            story.append(CondPageBreak(4.5 * inch))
            story.append(Paragraph("10.2 Forecast Visualisation", S.sub))
            rows_n = (len(fcs) + 1) // 2
            story.append(_img(chart, h=1.9 * rows_n + 0.3))
            story.append(Paragraph(
                f"Figure 10.1 — The last {2 * h} days + a {h}-day forecast. The vertical dotted line = the end of "
                "the dataset period. The bands widen forward because uncertainty grows with time distance.", S.caption))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("10.3 Current Conditions", S.sub))
        rows = [["Variable", "Recent", "Period mean", "Deviation", "Status"]]
        for f in fcs:
            z = (f.recent_mean - f.period_mean) / f.period_std if f.period_std else 0.0
            d = 3 if not f.unit else 2
            rows.append([f.label, Paragraph(f"{rf.fmt(f.recent_mean, f.unit, d)}<br/>(last {f.recent_days} days)", S.cell),
                         rf.fmt(f.period_mean, f.unit, d), f"{z:+.2f} σ", Paragraph(_status_text(f, z), S.cell)])
        story.append(_table(rows, [1.4 * inch, 1.3 * inch, 1.2 * inch, 0.7 * inch, 2.1 * inch], S))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph(f"10.4 {h}-Day Forecast", S.sub))
        rows = [["Variable", "Mean forecast", "80% range (end of horizon)", "Direction", "Confidence"]]
        for f in fcs:
            d = 3 if not f.unit else 2
            label_mean = rf.fmt(f.forecast_mean, f.unit, d) + (" (median)" if f.key == "rain" else "")
            rows.append([f.label, label_mean, f"{rf.fmt(f.lo80[-1], '', d)} – {rf.fmt(f.hi80[-1], f.unit, d)}",
                         {"up": "↑ up", "down": "↓ down", "stable": "→ stable"}[f.direction], f.confidence])
        story.append(_table(rows, [1.6 * inch, 1.5 * inch, 1.7 * inch, 0.8 * inch, 0.9 * inch], S))
        for f in fcs:
            for n in f.notes:
                story.append(Paragraph(f"• {_e(f.label)}: {_e(n)}", S.note))

        rain_fc = next((f for f in fcs if f.key == "rain"), None)
        rain = rf.rain_outlook(st, h) if rain_fc else None
        flood = rf.flood_scenario(st, rain_fc)
        if rain or flood:
            story.append(CondPageBreak(2.5 * inch))
            story.append(Paragraph("10.5 Rainfall Outlook &amp; Inundation Risk", S.sub))
        if rain:
            lines = [
                f"Probability of a rainy day (≥0.1 mm) in the dataset period: {rain['p_rain_day'] * 100:.0f}%; heavy-rain days "
                f"(≥{rain['heavy_threshold_mm']:.0f} mm): {rain['p_heavy_day'] * 100:.1f}% of days.",
                f"Over the next {h} days, if this period's pattern continues: on average ≈{rain['expected_heavy_days']:.1f} "
                f"heavy-rain days; probability of at least one event {rain['p_at_least_one_heavy'] * 100:.0f}%.",
            ]
            if "total_p50" in rain:
                lines.append(f"{h}-day rainfall accumulation: median {rain['total_p50']:.0f} mm (plausible p10–p90 range: "
                             f"{rain['total_p10']:.0f}–{rain['total_p90']:.0f} mm), from {rain['windows']} rolling {h}-day "
                             "windows within the dataset period.")
            for line in lines:
                story.append(Paragraph(f"• {line}", S.bullet))
        if flood:
            if flood.get("usable"):
                story.append(Paragraph(
                    f"• Inundation scenario: the relationship of MODIS flood % to 7-day rainfall (r = {flood['r']:.2f}, n = {flood['n']} "
                    f"clear days) gives ≈{flood['slope']:.3f} % flood pixels per mm. With a forecast 7-day rainfall of "
                    f"≈{flood['rain7_forecast']:.0f} mm (recent {flood['rain7_recent']:.0f} mm), the flood extent is expected to be "
                    f"<b>{flood['flood_forecast']:.2f}%</b> of pixels (80% range: {flood['flood_lo']:.2f}–{flood['flood_hi']:.2f}%), "
                    f"compared with {flood['flood_recent_model']:.2f}% under recent rainfall conditions.", S.bullet))
            else:
                story.append(Paragraph(
                    f"• The relationship between 7-day rainfall and MODIS flood extent is weak (r = {rf.fmt(flood.get('r'), '', 2)}), "
                    "so a rain-based inundation scenario is not presented.", S.bullet))

        story.append(CondPageBreak(3 * inch))
        story.append(Paragraph("10.6 Conclusions", S.sub))
        for i, (title, text) in enumerate(_outlook_conclusions(ctx, h, rain, flood), 1):
            story.append(Paragraph(f"<b>{i}. {title}</b> — {text}", S.bullet))
        story.append(Paragraph(
            "Note: this forecast is a statistical extrapolation of a single dataset's data, not a weather/hydrological "
            "model. It answers &ldquo;if this period's pattern continues, roughly what would it look like&rdquo;, and "
            "cannot anticipate seasonal changes or events outside the historical pattern. For operational decisions use "
            "official forecasts (e.g. BMKG) as the primary reference.", S.note))
        story.extend(_section_break())

    # -- Section 11 -----------------------------------------------------------

    def _section_json(self, story, ctx, S) -> None:
        story.append(Paragraph("11. JSON Summary &amp; Machine-Readable Export", S.section))
        story.append(Paragraph("11.1 Complete Metadata JSON", S.sub))
        story.append(Paragraph(
            "The following JSON is also saved as a separate file next to this PDF (same name, .json extension) and "
            "is available via <i>GET /api/datasets/{id}/report/json</i>. A <i>null</i> value means that quantity "
            "was not measured by the pipeline (see the notes in the relevant section).", S.body))
        payload = _json_summary(ctx)
        # Versi PDF diringkas: rincian yang sudah tampil sebagai tabel di section lain
        # (storage, quality, processing_levels, rekomendasi, detail backtest) hanya
        # dirujuk. File .json di samping PDF memuat versi lengkapnya.
        full = "<see the .json file>"
        for key in ("storage", "quality", "processing_levels", "recommendations"):
            payload[key] = full
        if payload.get("forecast"):
            fc = payload["forecast"]
            payload["forecast"] = {
                "horizon_days": fc["horizon_days"], "start": fc["start"], "end": fc["end"],
                "variables": {k: {kk: v[kk] for kk in ("model", "confidence", "direction", "forecast_mean", "end_interval_80")}
                              for k, v in fc["variables"].items()},
                "rain_outlook": full, "flood_scenario": full, "conclusions": full,
            }
        text = json.dumps(payload, indent=1, default=str, ensure_ascii=False)
        for line in text.splitlines():
            story.append(Paragraph(_e(line).replace(" ", "&nbsp;") or "&nbsp;", S.mono))

    def _previews(self, story, ctx, S, src) -> None:
        imgs = self._preview_images_for_source(ctx, src)
        if not imgs:
            return
        day = imgs[0][0].stem.split("_", 1)[0]
        colored = "grayscale" not in imgs[0][0].parent.name
        story.append(CondPageBreak(3.2 * inch))
        story.append(Paragraph(f"Example Preview{' (Coloured)' if colored else ''} — {day[:4]}-{day[4:6]}-{day[6:]}", S.sub2))
        # grid 3 kolom: gambar di atas, label band di bawahnya
        per_row, size = 3, 2.05 * inch
        cells = []
        for p, label in imgs:
            try:
                cells.append([Image(str(p), width=size, height=size, kind="proportional"), Paragraph(label, S.caption)])
            except Exception:
                logger.warning("[report] gagal menyisipkan preview %s", p)
        if not cells:
            return
        rows = [cells[i:i + per_row] for i in range(0, len(cells), per_row)]
        rows[-1] += [""] * (per_row - len(rows[-1]))
        grid = Table(rows, colWidths=[2.2 * inch] * per_row)
        grid.setStyle(TableStyle([("ALIGN", (0, 0), (-1, -1), "CENTER"), ("VALIGN", (0, 0), (-1, -1), "TOP")]))
        story.append(grid)
        story.append(Paragraph(_PREVIEW_LEGENDS.get(src, ""), S.note))


# -- content helpers ---------------------------------------------------------

_PREVIEW_BAND_LABELS = {
    "s1_vv": "Sentinel-1 VV", "s1_vh": "Sentinel-1 VH",
    "modis_flood": "MODIS Flood (MCDWD)", "modis_ndvi": "MODIS NDVI", "modis_ndwi": "MODIS NDWI",
    "gpm_rain_24h": "GPM rain 24 h", "gpm_rain_72h": "GPM rain 72 h", "gpm_rain_7d": "GPM rain 7 days",
}

_PREVIEW_LEGENDS = {
    "SENTINEL1": "Backscatter colour scale: dark purple = low (calm water, specular), yellow = high "
                 "(rough surfaces/vegetation, buildings).",
    "MODIS": "Flood: grey = no water, blue = permanent water, orange = seasonal flood, red = unusual flood, "
             "transparent = insufficient data (clouds). NDVI (RdYlGn, −0.2…0.8): red = water/built-up land, green = "
             "vegetation. NDWI (BrBG, −0.5…0.5): brown = dry, blue-green = water. Overlay = MODIS layer over "
             "S1 backscatter on the fusion grid.",
    "GPM": "YlGnBu scale starting at 0 mm: the darker the blue, the higher the rainfall (upper bound at the 98th "
           "percentile per file). Large pixels reflect the IMERG 0.1° resolution (~11 km); overlay = rainfall over "
           "S1 backscatter.",
}


def _preview_label(rest: str, kind: str) -> str:
    """'modis_flood_on_s1' -> 'MODIS Flood (MCDWD) — overlay on S1'."""
    base = rest.removesuffix("_on_s1")
    label = _PREVIEW_BAND_LABELS.get(base, base.replace("_", " ").upper())
    return f"{label} — overlay on S1" if rest.endswith("_on_s1") or kind == "composite" else label


def _preview_date_score(st, source: str, day: str) -> float:
    """Tanggal paling representatif untuk preview: S1 dengan scene PASS
    bersatuan dB terbanyak, MODIS dengan piksel bebas-awan terbanyak, GPM
    dengan curah hujan tertinggi (supaya skala warnanya informatif)."""
    if st is None:
        return 0.0
    try:
        d = datetime.strptime(day, "%Y%m%d").date()
    except ValueError:
        return 0.0
    if source == "SENTINEL1":
        return sum(1 for m in st.s1_metrics if m["date"] == d and _ok(m))
    if source == "MODIS":
        return next((o.valid_frac for o in st.raster_band("MODIS", "FLOOD") if o.day == d), 0.0)
    if source == "GPM":
        return next((o.mean or 0.0 for o in st.raster_band("GPM", "RAIN_24H") if o.day == d), 0.0)
    return 0.0

_FUSION_PSEUDOCODE = """\
def build_fusion_stacks(dataset, strategy="{strategy}", tol_days={tol}):
    # Axis 1 (DOWNLOAD): MODIS/GPM dates fetched from NASA
    aux_days = s1_dates if strategy == "CO_OCCURRENCE" else every_day(period)
    # Axis 2 (ASSEMBLE): dates that become one HDF5 file
    stack_days = every_day(period) if strategy == "FULL_COVERAGE" else s1_dates

    for day in stack_days:
        s1 = s1_scene_on(day)
        if s1 is None:                         # only happens in FULL_COVERAGE
            s1 = nearest_s1(day, max_offset=tol_days)     # may borrow ±tol
        modis = daily_file("MODIS", day)       # NDVI, NDWI, FLOOD (reprojected to the S1 grid)
        gpm   = daily_file("GPM", day)         # RAIN_24H/72H/7D   (reprojected to the S1 grid)

        stack = HDF5(grid=pinned_dataset_grid)
        stack["sentinel1"] = s1.bands if s1 else NaN     # not interpolated
        stack["modis"]     = modis     if modis else NaN
        stack["gpm"]       = gpm       if gpm   else NaN
        record = fusion_products(day, s1_offset_days=offset(s1, day),
                                 temporal_offset_modis=0, temporal_offset_gpm=0)
        n = count_present(s1, modis, gpm)       # 3 = complete, 2 = partial, 1 = single"""


_DIR_WORD = {"up": "increasing", "down": "decreasing", "stable": "relatively stable"}


def _status_text(f, z: float) -> str:
    level = ("far above" if z > 1 else "above" if z > 0.25 else
             "far below" if z < -1 else "below" if z < -0.25 else "near")
    meaning = {
        "vv": "wetter/rougher surface", "vh": "higher volume scattering (vegetation)",
        "ndvi": "greener vegetation", "ndwi": "wetter surface", "flood": "wider inundation",
        "rain": "wetter period",
    }[f.key]
    if level == "near":
        return "near the period mean (normal)"
    return f"{level} the mean" + (f"; {meaning}" if "above" in level else "")


def _outlook_conclusions(ctx, h: int, rain: dict | None, flood: dict | None) -> list[tuple[str, str]]:
    """Kesimpulan akhir: disusun dari angka prakiraan + keyakinannya, bukan
    kalimat baku -- setiap klaim menyebut dasar datanya."""
    by = {f.key: f for f in ctx.forecasts}
    out = []
    end = ctx.stats.period_end
    horizon_end = end + timedelta(days=h)

    def z(f):
        return (f.recent_mean - f.period_mean) / f.period_std if f.period_std else 0.0

    parts = []
    for key in ("rain", "flood", "ndwi", "ndvi", "vv"):
        f = by.get(key)
        if f:
            zz = z(f)
            word = "above" if zz > 0.25 else "below" if zz < -0.25 else "near"
            parts.append(f"{f.label.lower()} {word} the mean ({zz:+.1f}σ)")
    if parts:
        out.append((f"END-OF-PERIOD CONDITIONS (to {end})", "; ".join(parts) + "."))

    rain_f = by.get("rain")
    if rain_f:
        txt = (f"Rainfall is expected to be {_DIR_WORD[rain_f.direction]} (median ±{rain_f.forecast_mean:.1f} mm/day vs "
               f"{rain_f.recent_mean:.1f} mm/day recently; confidence {rain_f.confidence.lower()}).")
        if rain and "total_p50" in rain:
            txt += f" Plausible {h}-day accumulation: {rain['total_p10']:.0f}–{rain['total_p90']:.0f} mm."
        if rain:
            txt += f" Probability of ≥1 heavy-rain day: {rain['p_at_least_one_heavy'] * 100:.0f}%."
        out.append((f"RAINFALL OUTLOOK to {horizon_end}", txt))

    ff = by.get("flood")
    usable = bool(flood and flood.get("usable"))
    if ff or usable:
        signals = []
        if ff:
            signals.append(f"MODIS flood series {_DIR_WORD[ff.direction]} (±{ff.forecast_mean:.2f}% of pixels, confidence "
                           f"{ff.confidence.lower()})")
        if usable:
            delta = flood["flood_forecast"] - flood["flood_recent_model"]
            signals.append(f"rain-based scenario {flood['flood_forecast']:.2f}% ({delta:+.2f} points vs current rainfall conditions)")
        ups = int(ff is not None and ff.direction == "up") + int(
            usable and flood["flood_forecast"] > flood["flood_recent_model"] + 0.25)
        downs = int(ff is not None and ff.direction == "down") + int(
            usable and flood["flood_forecast"] < flood["flood_recent_model"] - 0.25)
        verdict = "INCREASING" if ups > downs else "DECREASING" if downs > ups else "UNCHANGED"
        out.append((f"INUNDATION RISK: {verdict}", "; ".join(signals) + "."))

    surf = [by[k] for k in ("ndvi", "ndwi", "vv", "vh") if k in by]
    if surf:
        out.append(("SURFACE &amp; VEGETATION",
                    "; ".join(f"{f.label} {_DIR_WORD[f.direction]} ({rf.fmt(f.forecast_mean, f.unit, 3 if not f.unit else 2)}, "
                              f"confidence {f.confidence.lower()})" for f in surf) + "."))

    conf = {c: [f.label for f in ctx.forecasts if f.confidence == c] for c in ("High", "Medium", "Low")}
    txt = "; ".join(f"{c.lower()}: {', '.join(v)}" for c, v in conf.items() if v)
    weak = conf["Low"]
    out.append(("CONFIDENCE LEVEL",
                f"Confidence {txt}."
                + (f" For {', '.join(weak)} the forecast is no better than assuming the latest condition continues — "
                   "treat it as an indication only." if weak else "")
                + f" A {h}-day horizon (one third of the period) keeps the extrapolation close to the observed data."))
    return out


def _executive_summary(ctx) -> list[str]:
    ds, st = ctx.dataset, ctx.stats
    src_txt = ", ".join(_SOURCE_LABEL.get(s, s) for s in sorted(ds.get("sources", {}))) or "no"
    location = ds.get("location_label") or "the configured region"
    paras = [
        f"The dataset <b>{_e(ds['name'])}</b> combines {src_txt} data over {_e(location)} from "
        f"{ds.get('date_start')} to {ds.get('date_end')} ({st.period_days} days). The pipeline recorded "
        f"{ds.get('total_scenes', 0)} scenes ({ds.get('completed_scenes', 0)} completed) and produced "
        f"{len(st.fusion)} fusion stacks with the {ds.get('fusion_strategy') or '-'} strategy.",
    ]
    facts = []
    vv = [m["mean_db"] for m in st.s1_metrics if m["band"] == "VV" and m["mean_db"] is not None and _ok(m)]
    if vv:
        facts.append(f"mean VV backscatter {mean(vv):.2f} dB (PASS scenes)")
    ndvi = [o.mean for o in st.raster_band("MODIS", "NDVI") if o.mean is not None]
    if ndvi:
        facts.append(f"mean NDVI {mean(ndvi):.3f}")
    rain = [o.mean for o in st.raster_band("GPM", "RAIN_24H") if o.mean is not None]
    if rain:
        facts.append(f"mean rainfall {mean(rain):.2f} mm/day with a total of {sum(rain):.0f} mm")
    if facts:
        paras.append("Geophysical summary: " + "; ".join(facts) + ".")
    overall = ctx.scores.get("Overall Data Health")
    n_issue = {s: sum(1 for i in ctx.issues if i["severity"] == s) for s in ("HIGH", "MEDIUM", "LOW")}
    if ctx.forecasts and st.period_end:
        h = rf.horizon_for(st.period_days)
        dirs = "; ".join(f"{f.label.lower()} {_DIR_WORD[f.direction]}" for f in ctx.forecasts
                         if f.key in ("rain", "flood", "ndwi"))
        if dirs:
            paras.append(f"Forecast for the next {h} days (to {st.period_end + timedelta(days=h)}): {dirs} — details and "
                         "confidence levels in Section 10.")
    paras.append(
        f"The overall data health score is {_n(overall, 0, '/100')} ({_health_label(overall or 0)}). The automated checks "
        f"found {n_issue['HIGH']} HIGH, {n_issue['MEDIUM']} MEDIUM and {n_issue['LOW']} LOW issues — details in Section 9."
    )
    return paras


def _source_spec_rows(ctx, src, levels) -> list[tuple[str, str]]:
    st = ctx.stats
    rows = [("Enabled", "Yes"), ("Processing levels", ", ".join(levels) or "-")]
    tiers = st.tiers.get(src, [])
    rows.append(("Tiers in dataset", " → ".join(t["tier"] for t in tiers) or "-"))
    rows.append(("Records in dataset (all tiers)", str(sum(t["files"] for t in tiers))))
    if src == "SENTINEL1":
        orbit = {o["orbit"]: o["scenes"] for o in st.s1_orbit}
        bands = sorted({m["band"] for m in st.s1_metrics})
        inc = [(o["inc_near"], o["inc_far"]) for o in st.s1_orbit if o["inc_near"] is not None]
        rows += [
            ("Mode / polarisation", f"IW, {' + '.join(bands) or 'VV + VH'} ({'dual-pol' if len(bands) == 2 else 'single-pol'})"),
            ("Ascending passes", str(orbit.get("ASCENDING", 0))),
            ("Descending passes", str(orbit.get("DESCENDING", 0))),
            ("Incidence angle", f"{min(a for a, _ in inc):.1f}° – {max(b for _, b in inc):.1f}°" if inc else "not recorded in satellite_scenes"),
            ("Spatial resolution", "10 m (GRD IW)"),
            ("Unique acquisition dates", f"{len(st.obs_dates.get(src, []))} (average revisit {_n(st.revisit_days(src), 1, ' days')})"),
        ]
    elif src == "MODIS":
        rows += [
            ("Product", ", ".join(st.nasa_products.get("MODIS", [])) or "MCDWD_L3_F2_NRT"),
            ("Bands", ", ".join(sorted(st.raster.get("MODIS", {}))) or "-"),
            ("Observation days", f"{len(st.obs_dates.get(src, []))} ({_n(st.completeness(src), 1, '%')} of the period)"),
            ("Mean valid pixels", _n(_mean_valid(st, "MODIS"), 1, "%")),
        ]
    elif src == "GPM":
        rows += [
            ("Product", ", ".join(st.nasa_products.get("GPM", [])) or "GPM_3IMERGDF"),
            ("Bands", ", ".join(sorted(st.raster.get("GPM", {}))) or "-"),
            ("Observation days", f"{len(st.obs_dates.get(src, []))} ({_n(st.completeness(src), 1, '%')} of the period)"),
            ("Resolution", "0.1° daily"),
        ]
    return rows


def _validation_checks(ctx) -> list[tuple[bool | None, str, str]]:
    ds, st = ctx.dataset, ctx.stats
    checks = []
    bounds = _bbox_bounds(ds.get("bbox_wkt"))
    if bounds:
        w, s, e, n = bounds
        ok = -180 <= w < e <= 180 and -90 <= s < n <= 90
        checks.append((ok, "Geolocation bounds check", f"bbox {w:.3f},{s:.3f},{e:.3f},{n:.3f} valid EPSG:4326" if ok else "invalid bbox"))
    out_of_period = [d for src in st.obs_dates for d in st.obs_dates[src]
                     if st.period_start and (d < st.period_start or d > st.period_end)]
    checks.append((not out_of_period, "Temporal range check",
                   "all observations are within the period" if not out_of_period else f"{len(out_of_period)} observations outside the period"))
    big_gaps = {src: st.gaps(src) for src in ("SENTINEL1", "MODIS", "GPM") if src in ds.get("sources", {})}
    n_gaps = sum(len(g) for g in big_gaps.values())
    checks.append((True if n_gaps == 0 else None, "Temporal continuity",
                   "no gaps >10 days" if n_gaps == 0 else
                   ", ".join(f"{_SOURCE_LABEL[s]}: {len(g)} gap(s)" for s, g in big_gaps.items() if g)))
    invalid = sum(t["invalid"] for items in st.tiers.values() for t in items)
    checks.append((invalid == 0, "Metadata completeness / validity", f"{invalid} products flagged is_valid = false"))
    lin = [m for m in st.s1_metrics if m["linear"]]
    checks.append((not lin if st.s1_metrics else None, "Unit check (S1 backscatter in dB)",
                   f"{len(lin)} products appear to be in linear units (min ≥ 0, mean > −1)" if lin else
                   ("all products are in dB units" if st.s1_metrics else "no S1 metrics")))
    s1_bad = [m for m in st.s1_metrics if _db(m) and not (-40 <= m["mean_db"] <= 5)]
    checks.append((not s1_bad if st.s1_metrics else None, "Radiometric range check (S1, −40…+5 dB)",
                   f"{len(s1_bad)} products out of range" if st.s1_metrics else "no S1 metrics"))
    ndvi_bad = [o for o in st.raster_band("MODIS", "NDVI") if o.min is not None and (o.min < -1.0001 or o.max > 1.0001)]
    if st.raster.get("MODIS"):
        checks.append((not ndvi_bad, "Data type / range check (NDVI −1…1)", f"{len(ndvi_bad)} rasters out of range"))
    rain_bad = [o for o in st.raster_band("GPM", "RAIN_24H") if o.min is not None and o.min < 0]
    if st.raster.get("GPM"):
        checks.append((not rain_bad, "Physical range check (rain ≥ 0)", f"{len(rain_bad)} rasters with negative values"))
    return checks


def _ascii_flow(src, tiers, stage_by_name) -> str:
    desc = {
        "SENTINEL1": {"RAW": "raw GRD (download)", "ALIGNED": "σ0 calibration + crop to AOI",
                      "DESPECKLED": "Lee filter (speckle)", "COG": "Cloud-Optimized GeoTIFF"},
        "MODIS": {"ALIGNED": "daily MCDWD, crop + reproject", "INDICES": "NDVI / NDWI / FLOOD",
                  "COG": "Cloud-Optimized GeoTIFF"},
        "GPM": {"ALIGNED": "daily IMERG, crop", "ACCUMULATED": "24 h / 72 h / 7-day accumulation",
                "COG": "Cloud-Optimized GeoTIFF"},
    }[src]
    stage_for = {("SENTINEL1", "ALIGNED"): "CROP", ("SENTINEL1", "DESPECKLED"): "LEE_FILTER",
                 ("SENTINEL1", "RAW"): "DOWNLOAD"}
    lines = []
    prev = None
    for t in tiers:
        if prev is not None:
            loss = (prev["dates"] - t["dates"]) / prev["dates"] * 100 if prev["dates"] else 0
            size = (t["avg_mb"] / prev["avg_mb"] * 100) if prev["avg_mb"] else 0
            stage = stage_by_name.get(stage_for.get((src, t["tier"]), ""))
            lines.append("    │")
            lines.append(f"    ↓  [{desc.get(t['tier'], t['tier'])}]")
            if stage and stage["avg_sec"] is not None:
                lines.append(f"    ↓  [Processing time: {_fmt_sec(stage['avg_sec'])}/job on average, {stage['jobs']} jobs]")
            lines.append(f"    ↓  [Data loss: {loss:.1f}% of dates | size/file: {size:.0f}% of the previous tier]")
            lines.append("    │")
        label = f"{t['tier']} ({t['level']})"
        lines.append(f"┌{'─' * 64}┐")
        lines.append(f"│ {label:<30}{t['files']:>6} files {t['dates']:>4} dates {_fmt_mb(t['avg_mb']):>9} │")
        lines.append(f"└{'─' * 64}┘")
        prev = t
    return "\n".join(lines)


def _ablation_narrative(src, tiers, stage_by_name, st) -> list[str]:
    out = []
    first, last = tiers[0], tiers[-1]
    if first["dates"]:
        out.append(f"Date retention {first['tier']} → {last['tier']}: {last['dates']}/{first['dates']} "
                   f"({last['dates'] / first['dates'] * 100:.1f}%).")
    if first["avg_mb"]:
        out.append(f"Average size per file fell from {_fmt_mb(first['avg_mb'])} to {_fmt_mb(last['avg_mb'])} "
                   f"({(1 - last['avg_mb'] / first['avg_mb']) * 100:.1f}% smaller); total {_fmt_mb(first['size_mb'])} → "
                   f"{_fmt_mb(last['size_mb'])}.")
    if src == "SENTINEL1":
        crop, lee = stage_by_name.get("CROP"), stage_by_name.get("LEE_FILTER")
        if crop and lee and crop["avg_sec"] and lee["avg_sec"]:
            out.append(f"The most expensive stage is calibration + crop ({_fmt_sec(crop['avg_sec'])}/scene) compared with the Lee filter "
                       f"({_fmt_sec(lee['avg_sec'])}/scene).")
        pas = [m["speckle"] for m in st.s1_metrics if m["speckle"] is not None]
        if pas:
            out.append(f"Mean speckle index on the final product {mean(pas):.3f}. Absolute radiometric/geometric accuracy "
                       "(±dB, ±m) is not measured by the pipeline, so it is not reported as an improvement figure.")
    invalid = sum(t["invalid"] for t in tiers)
    out.append(f"Products flagged invalid across all tiers: {invalid}.")
    return out


def _case_studies(ctx) -> list[dict]:
    ds, st = ctx.dataset, ctx.stats
    out = []
    raster_day = lambda src, band, d: next((o for o in st.raster_band(src, band) if o.day == d), None)  # noqa: E731
    s1_by_day = {}
    for m in st.s1_metrics:
        if m["date"]:
            s1_by_day.setdefault(m["date"], []).append(m)
    s1_scene_time = {s["date"]: s["datetime"] for s in st.s1_scenes if s["date"]}

    def source_rows(d, f):
        rows = [["Source", "Date / time", "Offset", "Data summary", "Quality"]]
        ms = s1_by_day.get(d, [])
        if f and f["s1_scene_id"] is None:
            rows.append(["Sentinel-1", "—", "—", "none (NaN in the stack)", "✗ absent"])
        else:
            t = s1_scene_time.get(d)
            vvs = [m for m in ms if m["band"] == "VV"]
            vhs = [m for m in ms if m["band"] == "VH"]
            summ = ", ".join(f"{b} {mean(x['mean_db'] for x in lst):.2f} dB" for b, lst in (("VV", vvs), ("VH", vhs)) if lst) or "—"
            q = ", ".join(sorted({m["flag"] for m in ms})) or "—"
            rows.append(["Sentinel-1", f"{t:%Y-%m-%d %H:%M} UTC" if t else str(d),
                         f"{f['s1_offset_days']} days" if f and f["s1_offset_days"] is not None else "0 days",
                         summ, ("✓ " if q == "PASS" else "⚠ ") + q])
        for src, band, fmt in (("MODIS", "NDVI", "NDVI {:.3f}"), ("GPM", "RAIN_24H", "{:.2f} mm/day")):
            o = raster_day(src, band, d)
            present = o is not None and (f is None or f[f"{src.lower()}_scene_id"] is not None)
            if not present:
                rows.append([_SOURCE_LABEL[src], "—", "—", "none (NaN in the stack)", "✗ absent"])
                continue
            rows.append([_SOURCE_LABEL[src], f"{d} (daily)", "0 days", fmt.format(o.mean) if o.mean is not None else "—",
                         f"{'✓' if o.valid_frac >= .7 else '⚠'} valid {o.valid_frac * 100:.0f}%"])
        return rows

    widths = [1.0 * inch, 1.5 * inch, 0.7 * inch, 2.0 * inch, 1.5 * inch]

    def record_json(f, d):
        rec = {
            "feature_date": str(d),
            "processing_level": f["level"] if f else None,
            "fusion_strategy": f["strategy"] if f else ds.get("fusion_strategy"),
            "confidence": {3: "complete", 2: "partial", 1: "single"}.get(f["n_sources"]) if f else "no_stack",
            "sources": {
                "sentinel_1": {"scene_id": f["s1_scene_id"] if f else None, "s1_offset_days": f["s1_offset_days"] if f else None},
                "modis": {"nasa_scene_id": f["modis_scene_id"] if f else None, "offset_days": f["modis_offset_days"] if f else None},
                "gpm": {"nasa_scene_id": f["gpm_scene_id"] if f else None, "offset_days": f["gpm_offset_days"] if f else None},
            },
            "feature_stack_path": Path(f["path"]).name if f else None,
        }
        return json.dumps(rec, indent=2)

    # Case 1: complete, offset 0, S1 PASS, MODIS valid tertinggi
    complete = [f for f in st.fusion if f["n_sources"] == 3 and not f["s1_offset_days"]]
    if complete:
        def score(f):
            o = raster_day("MODIS", "NDVI", f["date"])
            passes = all(m["flag"] == "PASS" for m in s1_by_day.get(f["date"], [])) if s1_by_day.get(f["date"]) else False
            return (passes, o.valid_frac if o else 0)
        f = max(complete, key=score)
        out.append({
            "title": "Case Study 1: Optimal Alignment (all three sources)",
            "paras": [f"Date <b>{f['date']}</b> (level {f['level']}) — a complete stack with a 0-day offset for all three "
                      "sources and the best MODIS coverage among complete stacks. This example is chosen automatically from "
                      "<i>fusion_products</i>."],
            "rows": source_rows(f["date"], f), "widths": widths, "pre": "Fused record (fusion_products):\n" + record_json(f, f["date"]),
        })
    else:
        out.append({"title": "Case Study 1: Optimal Alignment (all three sources)",
                    "paras": ["There is no stack with all three sources in this dataset."]})

    # Case 2: parsial -- 2 source, atau S1 dipinjam; fallback: stack lengkap dengan MODIS paling tertutup awan
    partial = [f for f in st.fusion if f["n_sources"] == 2 or (f["s1_offset_days"] or 0) != 0]
    if partial:
        f = partial[0]
        missing = [n for n, k in (("Sentinel-1", "s1_scene_id"), ("MODIS", "modis_scene_id"), ("GPM", "gpm_scene_id")) if f[k] is None]
        paras = [f"Date <b>{f['date']}</b>: " + (f"source {', '.join(missing)} is not available. " if missing else "")
                 + (f"The S1 scene was borrowed from {abs(f['s1_offset_days'])} days {'before' if f['s1_offset_days'] < 0 else 'after'}. "
                    if f["s1_offset_days"] else "")
                 + "The pipeline does not interpolate; missing layers are written as NaN and consumers use "
                   "<i>n_sources</i>/the offset to filter."]
        out.append({"title": "Case Study 2: Partial Alignment (2 of 3)", "paras": paras,
                    "rows": source_rows(f["date"], f), "widths": widths, "pre": record_json(f, f["date"])})
    else:
        cloudy = []
        for f in st.fusion:
            o = raster_day("MODIS", "FLOOD", f["date"])
            if o:
                cloudy.append((o.valid_frac, f))
        paras = ["No stack lost a source or borrowed an S1 scene from another date — all stacks are "
                 "structurally complete."]
        if cloudy:
            vf, f = min(cloudy, key=lambda x: x[0])
            paras.append(f"The worst <i>effective</i> partial case: <b>{f['date']}</b> — MODIS is present but only "
                         f"<b>{vf * 100:.0f}%</b> of FLOOD pixels are valid (the rest is cloud-covered). For dates like this "
                         "the MODIS information in the stack is only partial even though the record is labelled complete.")
            out.append({"title": "Case Study 2: Partial Alignment (2 of 3)", "paras": paras,
                        "rows": source_rows(f["date"], f), "widths": widths})
        else:
            out.append({"title": "Case Study 2: Partial Alignment (2 of 3)", "paras": paras})

    # Case 3: hari hanya auxiliary (tanpa S1 dan tanpa stack)
    fused_days = {f["date"] for f in st.fusion}
    s1_days = sorted(st.obs_dates.get("SENTINEL1", []))
    aux_days = sorted((set(st.obs_dates.get("MODIS", [])) | set(st.obs_dates.get("GPM", []))) - fused_days - set(s1_days))
    tol = ds.get("s1_match_tolerance_days") or 0
    if aux_days and s1_days:
        # hari dengan jarak terjauh ke S1 terdekat -- paling informatif
        def dist(d):
            return min(abs((d - s).days) for s in s1_days)
        d = max(aux_days, key=dist)
        before = max((s for s in s1_days if s < d), default=None)
        after = min((s for s in s1_days if s > d), default=None)
        gap = dist(d)
        paras = [
            f"Date <b>{d}</b> has MODIS/GPM data but no Sentinel-1 scene. Nearest S1: "
            f"{before or '—'} (before) and {after or '—'} (after); minimum distance <b>{gap} days</b>.",
            f"With the {ds.get('fusion_strategy')} strategy, no stack is assembled for this date. "
            + (f"Under FULL_COVERAGE this day would get a borrowed S1 (distance {gap} ≤ tolerance {tol} days)."
               if gap <= tol else
               f"Even under FULL_COVERAGE the S1 layer would be NaN because the {gap}-day distance exceeds the ±{tol}-day "
               "tolerance (no gap-fill is performed)."),
            f"In total there are {len(aux_days)} such days in the dataset period.",
        ]
        out.append({"title": "Case Study 3: Single Source (auxiliary only) + Gap-Fill", "paras": paras,
                    "rows": source_rows(d, {"s1_scene_id": None, "modis_scene_id": 1 if raster_day("MODIS", "NDVI", d) else None,
                                            "gpm_scene_id": 1 if raster_day("GPM", "RAIN_24H", d) else None,
                                            "s1_offset_days": None}),
                    "widths": widths})
    else:
        out.append({"title": "Case Study 3: Single Source + Gap-Fill",
                    "paras": ["There are no days with only auxiliary data in this dataset."]})
    return out


def _trend_narrative(st, src) -> list[str]:
    out = []
    if src == "SENTINEL1":
        for band in ("VV", "VH"):
            pts = [(m["date"], m["mean_db"]) for m in st.s1_metrics if m["band"] == band and m["date"]
                   and m["mean_db"] is not None and _ok(m)]
            if not pts:
                continue
            seas = []
            for key, label, months in rs.SEASONS:
                v = [x for d, x in pts if d.month in months]
                if v:
                    seas.append(f"{label}: {mean(v):.2f} dB (σ={pstdev(v):.2f}, n={len(v)})")
            slope = rs.linear_trend(sorted(pts))
            monthly = {}
            for d, x in pts:
                monthly.setdefault((d.year, d.month), []).append(x)
            mm = {k: mean(v) for k, v in monthly.items()}
            amp = max(mm.values()) - min(mm.values()) if len(mm) > 1 else None
            out.append(f"<b>{band}</b> (PASS scenes only) — " + "; ".join(seas) + ". "
                       + (f"Month-to-month amplitude {amp:.2f} dB. " if amp is not None else "")
                       + (f"Linear trend {slope:+.2f} dB/30 days." if slope is not None else ""))
        if out:
            out.append("Physical interpretation: an increase in backscatter usually follows higher soil/plant moisture, "
                       "whereas a sharp drop in VV can indicate standing water (specular reflection). "
                       "The short data period limits separating the seasonal signal from scene-to-scene variation.")
    elif src == "MODIS":
        for band in ("NDVI", "NDWI"):
            pts = [(o.day, o.mean) for o in st.raster_band("MODIS", band) if o.mean is not None and o.valid_frac >= .5]
            if not pts:
                continue
            slope = rs.linear_trend(pts)
            hi = max(pts, key=lambda p: p[1])
            lo = min(pts, key=lambda p: p[1])
            out.append(f"<b>{band}</b> (days with ≥50% valid pixels, n={len(pts)}) — mean {mean(x for _, x in pts):.3f}, "
                       f"highest {hi[1]:.3f} ({hi[0]}), lowest {lo[1]:.3f} ({lo[0]})"
                       + (f", trend {slope:+.4f}/30 days." if slope is not None else "."))
    elif src == "GPM":
        vals = [(o.day, o.mean) for o in st.raster_band("GPM", "RAIN_24H") if o.mean is not None]
        if vals:
            seas = [f"{r['label']}: {r['mean']:.2f} mm/day" for r in st.seasonal_raster("GPM", "RAIN_24H") if r["n"] and r["mean"] is not None]
            wet = sum(1 for _, v in vals if v >= rs.RAINY_THRESHOLD_MM)
            peak = max(vals, key=lambda p: p[1])
            out.append(f"Mean {mean(v for _, v in vals):.2f} mm/day; {wet} of {len(vals)} days were rainy. "
                       f"Peak {peak[1]:.1f} mm/day on {peak[0]}. Per season — " + "; ".join(seas) + ".")
    return out


def _monthly_combined(st) -> list[dict]:
    months = _period_months(st)
    out = []
    for y, m in months:
        def mm(src, band, _y=y, _m=m):
            v = [o.mean for o in st.raster_band(src, band) if o.day.year == _y and o.day.month == _m and o.mean is not None]
            return (mean(v) if v else None), v
        s1 = {r["band"]: r for r in st.s1_monthly if r["year"] == y and r["month"] == m}
        rain, rain_v = mm("GPM", "RAIN_24H")
        row = {
            "year": y, "month": m,
            "s1_n": len([d for d in st.obs_dates.get("SENTINEL1", []) if d.year == y and d.month == m]),
            "vv": s1.get("VV", {}).get("mean"), "vh": s1.get("VH", {}).get("mean"),
            "ndvi": mm("MODIS", "NDVI")[0], "ndwi": mm("MODIS", "NDWI")[0], "flood": mm("MODIS", "FLOOD")[0],
            "rain": rain, "rain_sum": sum(rain_v) if rain_v else None,
        }
        if any(row[k] is not None for k in ("vv", "ndvi", "rain")) or row["s1_n"]:
            out.append(row)
    return out


def _key_findings(ctx, table) -> list[tuple[str, str]]:
    ds, st = ctx.dataset, ctx.stats
    out = []
    rain_rows = [r for r in table if r["rain"] is not None]
    if len(rain_rows) >= 2:
        wet = max(rain_rows, key=lambda r: r["rain"])
        dry = min(rain_rows, key=lambda r: r["rain"])
        ratio = wet["rain"] / dry["rain"] if dry["rain"] else None
        strong = ratio is not None and ratio >= 3
        out.append(("SEASONAL SIGNAL " + ("STRONG" if strong else "MODERATE"),
                    f"Wettest month {rs.MONTH_NAMES[wet['month']]} {wet['year']} ({wet['rain']:.2f} mm/day) vs driest "
                    f"{rs.MONTH_NAMES[dry['month']]} {dry['year']} ({dry['rain']:.2f} mm/day)"
                    + (f", ratio {ratio:.1f}×." if ratio else ".")))
    corr_vv_rain = _monthly_corr(table, "vv", "rain")
    corr_flood_rain = _monthly_corr(table, "flood", "rain")
    if corr_vv_rain is not None or corr_flood_rain is not None:
        out.append(("CROSS-SENSOR RELATIONSHIP",
                    f"Monthly correlation VV–rain r={_n(corr_vv_rain, 2)}, MODIS flood–rain r={_n(corr_flood_rain, 2)} "
                    f"(n={len(table)} months; indicative because n is small)."))
    span = st.period_days
    out.append(("NOT ENOUGH FOR A LONG-TERM TREND" if span < 730 else "PERIOD ADEQUATE FOR TRENDS",
                f"Period {span} days. The linear slopes reported above describe intra-seasonal variation, not "
                "a climate trend; at least several years of data are needed to infer a trend."))
    comp = {s: st.completeness(s) for s in ("SENTINEL1", "MODIS", "GPM") if s in ds.get("sources", {})}
    out.append(("DATA COMPLETENESS",
                ", ".join(f"{_SOURCE_LABEL[s]} {_n(v, 1, '%')} of days" for s, v in comp.items())
                + f". Mean S1 revisit {_n(st.revisit_days('SENTINEL1'), 1, ' days')}; MODIS mean "
                  f"{_n(_mean_valid(st, 'MODIS'), 0, '%')} valid pixels per day (clouds)."))
    if st.fusion:
        complete = sum(1 for f in st.fusion if f["n_sources"] == 3)
        out.append(("FUSION SUCCESS RATE",
                    f"{complete}/{len(st.fusion)} stacks complete with all three sources ({complete / len(st.fusion) * 100:.0f}%), "
                    f"all with an S1 offset of {'0 days' if not any(f['s1_offset_days'] for f in st.fusion) else 'varying length'}."))
    return out


def _s1_artifacts(st) -> list[dict]:
    out = []
    lin = [m for m in st.s1_metrics if m["linear"]]
    if lin:
        dates = sorted({m["date"] for m in lin if m["date"]})
        orbits = sorted({m["orbit"] or "?" for m in lin})
        rows = [["Date", "Orbit", "Band", "Mean", "Min", "Max", "Flag"]]
        for m in lin[:10]:
            rows.append([str(m["date"]), m["orbit"] or "-", m["band"], _n(m["mean_db"], 3), _n(m["min_db"], 2),
                         _n(m["max_db"], 1), m["flag"]])
        out.append({
            "title": f"BACKSCATTER STORED IN LINEAR UNITS ({len(lin)} products)",
            "lines": [f"Issue: {len(lin)} products ({len(dates)} dates, orbit {', '.join(orbits)}) have values in the "
                      "*_db columns with a minimum ≥ 0 and a mean ≈ 0 — the pattern of linear power values, not dB.",
                      f"Dates: {', '.join(str(d) for d in dates[:10])}{' …' if len(dates) > 10 else ''}",
                      f"Impact: {sum(1 for m in lin if m['flag'] == 'PASS')} of them are flagged PASS, so a naive mean "
                      "in dB is biased toward 0 dB. This report excludes them from all dB statistics.",
                      "Severity: HIGH",
                      "Resolution: OPEN — check the 10·log10 conversion in the calibration/QA stage for this track, then "
                      "recompute quality_metrics."],
            "rows": rows, "widths": [0.9 * inch, 1.0 * inch, 0.5 * inch, 0.8 * inch, 0.7 * inch, 0.8 * inch, 0.7 * inch],
        })
    fails = [m for m in st.s1_metrics if m["flag"] == "FAIL"]
    if fails:
        rows = [["Date", "Band", "Score", "Valid %", "Max dB", "Scene"]]
        for m in fails[:12]:
            rows.append([str(m["date"]), m["band"], _n(m["score"], 1), _n((m["valid_frac"] or 0) * 100, 0), _n(m["max_db"], 2),
                         Paragraph(_e((m["product_identifier"] or "")[:44]), _cell_style())])
        zero_max = sum(1 for m in fails if m["max_db"] is not None and m["max_db"] > -0.5)
        sev = "MEDIUM" if len(fails) / max(len(st.s1_metrics), 1) > 0.1 else "LOW"
        out.append({
            "title": f"RADIOMETRIC QUALITY FAILURES ({len(fails)} products)",
            "lines": [f"Issue: {len(fails)} of {len(st.s1_metrics)} products are flagged FAIL "
                      f"({len(fails) / len(st.s1_metrics) * 100:.0f}%).",
                      f"Indication: {zero_max} FAIL products have a maximum backscatter ≈ 0 dB — the typical pattern of "
                      "fill pixels (zero-fill) at the footprint edge that get counted.",
                      f"Severity: {sev}",
                      "Resolution: FLAGGED — the flag is stored in quality_metrics; filter flag = 'PASS' for sensitive analysis."],
            "rows": rows, "widths": [0.9 * inch, 0.5 * inch, 0.5 * inch, 0.6 * inch, 0.6 * inch, 3.6 * inch],
        })
    low_cov = [m for m in st.s1_metrics if m["valid_frac"] is not None and m["valid_frac"] < 0.5]
    if low_cov:
        dates = sorted({str(m["date"]) for m in low_cov})
        out.append({"title": f"PARTIAL FOOTPRINT COVERAGE ({len(low_cov)} products)",
                    "lines": [f"Products with &lt;50% valid pixels over the AOI on dates: {', '.join(dates[:10])}"
                              + (" …" if len(dates) > 10 else ""),
                              "Common cause: the AOI is at the swath edge so it is only partly covered by the scene.",
                              "Severity: LOW — Status: FLAGGED (valid_pixels recorded)"]})
    vv = [m for m in st.s1_metrics if m["band"] == "VV" and m["mean_db"] is not None and _ok(m)]
    if len(vv) > 4:
        mu, sd = mean(x["mean_db"] for x in vv), pstdev(x["mean_db"] for x in vv)
        outl = [m for m in vv if sd and abs(m["mean_db"] - mu) > 2 * sd]
        out.append({"title": "BACKSCATTER OUTLIERS (|z| &gt; 2, VV, scene PASS)",
                    "lines": [f"{len(outl)} scene(s) identified"
                              + (": " + ", ".join(f"{m['date']} ({m['mean_db']:.2f} dB)" for m in outl[:6]) if outl else "."),
                              f"Reference: mean {mu:.2f} dB, σ {sd:.2f} dB.",
                              "Severity: " + ("LOW — outliers can be real (flooding, rain during acquisition)." if outl else "none.")]})
    if not any(s["inc_near"] is not None for s in st.s1_scenes):
        out.append({"title": "METADATA GAPS",
                    "lines": ["Incidence angle and relative orbit are not filled in for this dataset's scenes — angle "
                              "normalisation (γ0 / cosine correction) cannot be verified from the metadata.",
                              "Severity: LOW — Status: OPEN"]})
    if not out:
        out.append({"title": "No artefacts detected", "lines": ["All automated checks passed."]})
    return out


def _collect_issues(ctx) -> list[dict]:
    ds, st = ctx.dataset, ctx.stats
    issues = []

    def add(source, severity, title, detail, action, status):
        issues.append({"source": source, "severity": severity, "title": title, "detail": detail,
                       "action": action, "status": status})

    srcs = ds.get("sources", {})
    if "SENTINEL1" in srcs:
        lin = [m for m in st.s1_metrics if m["linear"]]
        if lin:
            add("SENTINEL1", "HIGH", f"{len(lin)} S1 products in linear units in the dB columns",
                "Dates: " + ", ".join(sorted({str(m['date']) for m in lin})[:8]),
                "Fix the 10·log10 conversion and recompute quality_metrics; filter these products until fixed.",
                "OPEN")
        fails = [m for m in st.s1_metrics if m["flag"] == "FAIL"]
        if fails:
            share = len(fails) / len(st.s1_metrics)
            add("SENTINEL1", "MEDIUM" if share > 0.1 else "LOW",
                f"{len(fails)} S1 products failed radiometric QA ({share * 100:.0f}%)",
                "Dates: " + ", ".join(sorted({str(m['date']) for m in fails})[:8]),
                "Filter quality_flag = 'PASS' for backscatter analysis; check for zero-fill at the swath edge.",
                "FLAGGED IN METADATA")
        for a, b, n in st.gaps("SENTINEL1"):
            add("SENTINEL1", "MEDIUM", f"S1 acquisition gap of {n} days", f"{a} to {b}",
                "Consider a larger pairing tolerance or the FULL_COVERAGE strategy.", "OPEN")
        if st.s1_scenes and not any(s["inc_near"] is not None for s in st.s1_scenes):
            add("SENTINEL1", "LOW", "Incidence angle not recorded",
                "satellite_scenes.incidence_angle_near/far is empty.", "Fill it from the SAFE metadata at download time.", "OPEN")
        orb = {o["orbit"]: o["scenes"] for o in st.s1_orbit}
        if len(orb) == 2 and min(orb.values()) / max(orb.values()) < 0.6:
            add("SENTINEL1", "LOW", "Unbalanced number of ascending/descending passes", str(orb),
                "Analyse each orbit separately — different geometry affects backscatter.", "INFO")
        if not st.s1_metrics and st.s1_scenes:
            add("SENTINEL1", "MEDIUM", "S1 scenes without quality metrics", f"{len(st.s1_scenes)} scenes",
                "Run the QUALITY_ANALYTICS stage.", "OPEN")
    if "MODIS" in srcs:
        flood = st.raster_band("MODIS", "FLOOD")
        if flood:
            poor = sum(1 for o in flood if o.valid_frac < .5)
            share = poor / len(flood)
            if share > 0.2:
                worst = min(rs.SEASONS, key=lambda s: mean([o.valid_frac for o in flood if o.day.month in s[2]] or [1]))
                add("MODIS", "MEDIUM" if share < 0.5 else "HIGH",
                    f"High cloud cover: {poor} days (<50% valid pixels)",
                    f"{share * 100:.0f}% of days; worst in {worst[1]}.",
                    "Use multi-day composites or complement with S1 (sees through clouds) for flood detection.",
                    "INHERENT TO OPTICAL SENSOR")
        for a, b, n in st.gaps("MODIS"):
            add("MODIS", "MEDIUM", f"MODIS data gap of {n} days", f"{a} to {b}", "Check the LANCE download log.", "OPEN")
        missing = [q for q in ctx.quality if q["source"] == "MODIS" and q["quality_score"] < 100]
        for q in missing:
            low = {b: v for b, v in (q.get("bands") or {}).items() if v < 100}
            add("MODIS", "LOW", "Uneven band coverage", ", ".join(f"{b} {v:.0f}%" for b, v in low.items()),
                "Check the dates that are missing particular bands.", "FLAGGED")
    if "GPM" in srcs:
        rain = st.raster_band("GPM", "RAIN_24H")
        if rain:
            add("GPM", "LOW", "Coarse resolution relative to the AOI", f"{_gpm_pixels(rain)} 0.1° pixels per raster",
                "Treat rainfall as a regional mean value, not per S1 pixel.", "INHERENT")
            add("GPM", "LOW", "IMERG Final latency of ~3.5 months", "Not suitable for near-real-time applications.",
                "Use IMERG Early/Late for operational use.", "INHERENT")
        for a, b, n in st.gaps("GPM"):
            add("GPM", "MEDIUM", f"GPM data gap of {n} days", f"{a} to {b}", "Check the GES DISC download log.", "OPEN")
    if len(srcs) > 1:
        s1_days = set(st.obs_dates.get("SENTINEL1", []))
        fused = {f["date"] for f in st.fusion}
        missing = s1_days - fused
        if s1_days and missing:
            add("FUSION", "MEDIUM" if len(missing) / len(s1_days) > .1 else "LOW",
                f"{len(missing)} S1 dates without a fusion stack", ", ".join(str(d) for d in sorted(missing)[:8]),
                "Run refusion for those dates.", "OPEN")
        partial = [f for f in st.fusion if f["n_sources"] < 3]
        if partial:
            add("FUSION", "LOW", f"{len(partial)} incomplete stacks", ", ".join(str(f["date"]) for f in partial[:8]),
                "Filter by the number of sources when building training data.", "FLAGGED")
    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    issues.sort(key=lambda i: order[i["severity"]])
    return issues


_SCORE_FORMULAS = {
    "Completeness Score": "mean of (% pipeline scenes completed, % MODIS observation days, % GPM observation days).",
    "Radiometric Quality": "mean Sentinel-1 quality_score (quality_metrics).",
    "Spatial Coverage": "mean valid-pixel fraction per product across all sources × 100.",
    "Fusion Success": "S1 dates that produced a complete 3-source stack ÷ S1 dates × 100.",
    "Overall Data Health": "mean of the available components above.",
}


def _compute_scores(ctx) -> dict:
    ds, st = ctx.dataset, ctx.stats
    comp = []
    if ds.get("total_scenes"):
        comp.append(ds["completed_scenes"] / ds["total_scenes"] * 100)
    for src in ("MODIS", "GPM"):
        if src in ds.get("sources", {}) and st.completeness(src) is not None:
            comp.append(min(st.completeness(src), 100))
    radiometric = next((q["quality_score"] for q in ctx.quality if q["source"] == "SENTINEL1"), None)
    valid = [m["valid_frac"] for m in st.s1_metrics if m["valid_frac"] is not None]
    valid += [o.valid_frac for bands in st.raster.values() for obs in bands.values() for o in obs]
    s1_days = set(st.obs_dates.get("SENTINEL1", []))
    complete_days = {f["date"] for f in st.fusion if f["n_sources"] == 3}
    fusion = len(s1_days & complete_days) / len(s1_days) * 100 if s1_days and len(ds.get("sources", {})) > 1 else None
    scores = {
        "Completeness Score": mean(comp) if comp else None,
        "Radiometric Quality": float(radiometric) if radiometric is not None else None,
        "Spatial Coverage": mean(valid) * 100 if valid else None,
        "Fusion Success": fusion,
    }
    present = [v for v in scores.values() if v is not None]
    return {"Overall Data Health": mean(present) if present else None, **scores}


def _usage_recommendations(ctx) -> dict:
    ds, st = ctx.dataset, ctx.stats
    srcs = ds.get("sources", {})
    suitable, caution, not_suitable = [], [], []
    if len(srcs) > 1 and st.fusion:
        suitable.append(f"Training multi-sensor machine-learning models ({len(st.fusion)} fusion stacks on the same grid)")
    if st.raster.get("GPM"):
        suitable.append("Characterising daily rainfall and analysing regional-scale extreme rainfall events")
    if st.raster.get("MODIS"):
        suitable.append("Daily inundation/flood mapping on clear days (MCDWD) and NDVI/NDWI monitoring")
    if st.s1_metrics:
        suitable.append("Cloud-penetrating surface-water detection with Sentinel-1 (PASS scenes)")
    if st.period_days >= 90:
        suitable.append("Seasonal characterisation within the dataset period")

    if st.s1_metrics and any(m["flag"] == "FAIL" for m in st.s1_metrics):
        caution.append("Absolute backscatter analysis — filter out FAIL scenes first")
    if st.raster.get("MODIS") and (_mean_valid(st, "MODIS") or 100) < 80:
        caution.append("MODIS analysis in cloudy seasons — many pixels have insufficient data")
    if st.raster.get("GPM"):
        caution.append("Rainfall analysis at sub-kilometre scale (GPM resolution 0.1°)")
    if (st.revisit_days("SENTINEL1") or 0) > 3:
        caution.append(f"Daily S1-based monitoring — mean revisit {st.revisit_days('SENTINEL1')} days")

    if st.period_days < 730:
        not_suitable.append(f"Detecting climate / inter-annual trends (the period is only {st.period_days} days)")
    if "GPM" in srcs:
        not_suitable.append("Operational nowcasting with IMERG Final (latency ~3.5 months)")
    not_suitable.append("Absolute accuracy validation without field data (there is no ground truth in the dataset)")
    return {"suitable": suitable, "caution": caution, "not_suitable": not_suitable}


def _json_summary(ctx: _ReportContext) -> dict:
    ds, st = ctx.dataset, ctx.stats
    bounds = _bbox_bounds(ds.get("bbox_wkt"))
    srcs = ds.get("sources", {})

    def tier_count(src, tier):
        return next((t["dates"] for t in st.tiers.get(src, []) if t["tier"] == tier), None)

    def stage_sec(name):
        return next((round(j["avg_sec"], 2) for j in st.stage_jobs if j["stage"] == name and j["avg_sec"] is not None), None)

    def transition(src, a, b, **extra):
        ia, ib = tier_count(src, a), tier_count(src, b)
        if ia is None or ib is None:
            return None
        return {"records_input": ia, "records_output": ib,
                "loss_percent": round((ia - ib) / ia * 100, 2) if ia else None, **extra}

    def r(x, d=3):
        return None if x is None else round(x, d)

    vv = [m["mean_db"] for m in st.s1_metrics if m["band"] == "VV" and m["mean_db"] is not None and _ok(m)]
    vh = [m["mean_db"] for m in st.s1_metrics if m["band"] == "VH" and m["mean_db"] is not None and _ok(m)]
    ratio = _vv_vh_ratio(st)["all"]
    ndvi = [o.mean for o in st.raster_band("MODIS", "NDVI") if o.mean is not None]
    ndwi = [o.mean for o in st.raster_band("MODIS", "NDWI") if o.mean is not None]
    flood = st.raster_band("MODIS", "FLOOD")
    rain = [o.mean for o in st.raster_band("GPM", "RAIN_24H") if o.mean is not None]
    orbit = {o["orbit"]: o["scenes"] for o in st.s1_orbit}
    s1_scores = [m["score"] for m in st.s1_metrics if m["score"] is not None]
    rec = _usage_recommendations(ctx)
    n_by = {k: sum(1 for f in st.fusion if f["n_sources"] == k) for k in (3, 2, 1)}

    sources = {}
    if "SENTINEL1" in srcs:
        sources["sentinel_1"] = {
            "enabled": True, "processing_levels": srcs["SENTINEL1"],
            "records_ingested": sum(t["files"] for t in st.tiers.get("SENTINEL1", [])),
            "unique_acquisition_dates": len(st.obs_dates.get("SENTINEL1", [])),
            "ascending_passes": orbit.get("ASCENDING", 0), "descending_passes": orbit.get("DESCENDING", 0),
            "quality_metrics": {
                "mean_vv_backscatter_db": r(mean(vv) if vv else None), "std_vv_backscatter_db": r(pstdev(vv) if len(vv) > 1 else None),
                "mean_vh_backscatter_db": r(mean(vh) if vh else None), "std_vh_backscatter_db": r(pstdev(vh) if len(vh) > 1 else None),
                "vv_vh_ratio_db": r(mean(ratio) if ratio else None),
                "mean_quality_score": r(mean(s1_scores) if s1_scores else None, 2),
                "pass_count": st.quality_flags.get("SENTINEL1", {}).get("PASS", 0),
                "fail_count": st.quality_flags.get("SENTINEL1", {}).get("FAIL", 0),
                "completeness_percent": st.completeness("SENTINEL1"),
                "mean_revisit_days": st.revisit_days("SENTINEL1"),
                "geolocation_accuracy_m": None, "radiometric_accuracy_db": None,
                "thermal_noise_floor_db": None, "mean_coherence": None,
            },
        }
    if "MODIS" in srcs:
        sources["modis"] = {
            "enabled": True, "processing_levels": srcs["MODIS"],
            "product_types": st.nasa_products.get("MODIS", []),
            "bands": sorted(st.raster.get("MODIS", {})),
            "records_ingested": sum(t["files"] for t in st.tiers.get("MODIS", [])),
            "quality_metrics": {
                "mean_ndvi": r(mean(ndvi) if ndvi else None), "std_ndvi": r(pstdev(ndvi) if len(ndvi) > 1 else None),
                "mean_ndwi": r(mean(ndwi) if ndwi else None),
                "mean_flood_pixel_percent": r(mean(o.mean for o in flood if o.mean is not None) if flood else None),
                "cloud_free_percent": r(mean(o.valid_frac for o in flood) * 100 if flood else None, 1),
                "quality_flag_distribution": st.quality_flags.get("MODIS"),
                "completeness_percent": st.completeness("MODIS"),
                "mean_lst_celsius": None, "validation_rmse_k": None, "validation_bias_k": None,
            },
        }
    if "GPM" in srcs:
        sources["gpm"] = {
            "enabled": True, "processing_levels": srcs["GPM"],
            "product_type": ", ".join(st.nasa_products.get("GPM", [])) or None,
            "bands": sorted(st.raster.get("GPM", {})),
            "records_ingested": sum(t["files"] for t in st.tiers.get("GPM", [])),
            "quality_metrics": {
                "mean_precip_mm_day": r(mean(rain) if rain else None),
                "median_precip_mm_day": r(median(rain) if rain else None),
                "max_precip_mm_day": r(max(rain) if rain else None),
                "percentile_95_mm_day": r(_percentile(rain, .95) if rain else None),
                "total_precip_mm": r(sum(rain) if rain else None, 1),
                "rainy_days_percent": r(sum(1 for v in rain if v >= rs.RAINY_THRESHOLD_MM) / len(rain) * 100 if rain else None, 1),
                "completeness_percent": st.completeness("GPM"),
                "gauge_adjustment_percent": None, "uncertainty_percent": None,
            },
        }

    total_records = sum(t["files"] for items in st.tiers.values() for t in items)
    return {
        "report_metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "report_version": REPORT_VERSION,
            "schema_version": "trinity_datalab_v1",
            "generator": f"Trinity DataLab Report Engine v{REPORT_VERSION}",
        },
        "dataset": {
            "id": str(ds["dataset_uuid"]), "dataset_id": ds["dataset_id"], "name": ds["name"],
            "description": ds.get("description"), "created_at": ds["created_at"], "updated_at": ds["updated_at"],
            "total_records": total_records,
            "temporal_completeness_percent": r(ctx.scores.get("Completeness Score"), 1),
            "data_quality_score": r(ctx.scores.get("Overall Data Health"), 1),
            "status": ds["status"],
        },
        "spatial_bounds": {
            "north": bounds[3], "south": bounds[1], "east": bounds[2], "west": bounds[0], "crs": "EPSG:4326",
            "area_km2": round(_bbox_area_km2(bounds), 1), "location_label": ds.get("location_label"),
        } if bounds else {"wkt": ds.get("bbox_wkt"), "crs": "EPSG:4326"},
        "temporal_range": {
            "start": ds.get("date_start"), "end": ds.get("date_end"), "duration_days": st.period_days,
            "completeness_percent": r(ctx.scores.get("Completeness Score"), 1),
        },
        "sources": sources,
        "fusion_strategy": {
            "method": ds.get("fusion_strategy"),
            "temporal_resolution": "daily",
            "s1_match_tolerance_days": ds.get("s1_match_tolerance_days"),
            "interpolation_method": None,
            "records_complete_sources": n_by[3], "records_partial_sources": n_by[2], "records_single_source": n_by[1],
            "fusion_success_rate": r(ctx.scores["Fusion Success"] / 100, 3) if ctx.scores.get("Fusion Success") is not None else None,
        },
        "processing_levels": {
            "sentinel_1": {
                "raw_to_aligned": transition("SENTINEL1", "RAW", "ALIGNED", avg_processing_time_seconds=stage_sec("CROP")),
                "aligned_to_despeckled": transition("SENTINEL1", "ALIGNED", "DESPECKLED", avg_processing_time_seconds=stage_sec("LEE_FILTER")),
                "despeckled_to_cog": transition("SENTINEL1", "DESPECKLED", "COG"),
            },
            "modis": {"aligned_to_indices": transition("MODIS", "ALIGNED", "INDICES"),
                      "indices_to_cog": transition("MODIS", "INDICES", "COG")},
            "gpm": {"aligned_to_accumulated": transition("GPM", "ALIGNED", "ACCUMULATED"),
                    "accumulated_to_cog": transition("GPM", "ACCUMULATED", "COG")},
        },
        "forecast": _forecast_json(ctx),
        "quality_scores": {k: r(v, 1) for k, v in ctx.scores.items()},
        "storage": ctx.breakdown,
        "quality": ctx.quality,
        "warnings": [{"source": i["source"].lower(), "severity": i["severity"].lower(), "message": i["title"],
                      "detail": i["detail"]} for i in ctx.issues],
        "recommendations": rec["suitable"] + [f"Use with caution: {c}" for c in rec["caution"]]
                           + [f"Not suitable: {n}" for n in rec["not_suitable"]],
    }


def _forecast_json(ctx) -> dict | None:
    st = ctx.stats
    if not ctx.forecasts or not st.period_end:
        return None
    h = rf.horizon_for(st.period_days)
    rain_fc = next((f for f in ctx.forecasts if f.key == "rain"), None)
    rain = rf.rain_outlook(st, h) if rain_fc else None
    flood = rf.flood_scenario(st, rain_fc)

    def rnd(x):
        return None if x is None else round(float(x), 4)

    return {
        "horizon_days": h,
        "rule": "horizon = period_days / 3; this dataset's data only",
        "start": st.period_end + timedelta(days=1),
        "end": st.period_end + timedelta(days=h),
        "variables": {
            f.key: {
                "label": f.label, "unit": f.unit, "model": f.model, "confidence": f.confidence,
                "direction": f.direction, "recent_mean": rnd(f.recent_mean), "period_mean": rnd(f.period_mean),
                "forecast_mean": rnd(f.forecast_mean), "end_interval_80": [rnd(f.lo80[-1]), rnd(f.hi80[-1])],
                "end_interval_95": [rnd(f.lo95[-1]), rnd(f.hi95[-1])], "backtest": f.backtest, "notes": f.notes,
            } for f in ctx.forecasts
        },
        "rain_outlook": rain,
        "flood_scenario": flood,
        "conclusions": [f"{t}: {x}".replace("&amp;", "&") for t, x in _outlook_conclusions(ctx, h, rain, flood)],
    }


# -- small helpers -----------------------------------------------------------

def _fig():
    fig, ax = plt.subplots(figsize=(6.8, 3.1))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color=_GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    ax.tick_params(labelsize=8, colors=_INK)
    ax.title.set_fontsize(10)
    return fig, ax


def _shade_seasons(ax, start: date, end: date) -> None:
    """Pita musim (recessive) di belakang deret waktu, label kode musim di atas."""
    d = date(start.year, start.month, 1)
    shade = {"DJF": "#E8EEF7", "JJA": "#F7EFE6", "MAM": "#F3F3F3", "SON": "#F3F3F3"}
    while d <= end:
        nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
        s = rs.season_of(d.month)
        ax.axvspan(max(d, start - timedelta(days=1)), min(nxt, end + timedelta(days=1)), color=shade[s], zorder=0, linewidth=0)
        mid = max(d, start) + (min(nxt, end) - max(d, start)) / 2
        ax.text(mid, 1.0, s, transform=ax.get_xaxis_transform(), ha="center", va="bottom", fontsize=6.5, color="#888888")
        d = nxt


def _draw_forecast(ax, fc, color, last_x=None, last_y=None, legend=True) -> None:
    """Prakiraan: garis putus dari observasi terakhir + pita 95% (muda) dan 80%."""
    xs = ([last_x] if last_x is not None else []) + list(fc.days)
    m = ([last_y] if last_y is not None else []) + list(fc.mean)
    pad = [last_y] if last_y is not None else []
    ax.fill_between(xs, pad + fc.lo95, pad + fc.hi95, color=color, alpha=0.10, linewidth=0,
                    label="95% interval" if legend else None)
    ax.fill_between(xs, pad + fc.lo80, pad + fc.hi80, color=color, alpha=0.22, linewidth=0,
                    label="80% interval" if legend else None)
    ax.plot(xs, m, color=color, linewidth=1.4, linestyle="--",
            label=f"forecast {fc.label.split()[-1]}" if legend else None)


def _mark_forecast_start(ax, day, label=True) -> None:
    if day is None:
        return
    ax.axvline(day, color="#666666", linewidth=0.8, linestyle=":")
    if label:
        ax.text(day, 0.02, "  forecast →", transform=ax.get_xaxis_transform(), fontsize=7, color="#555555")


def _section_break() -> list:
    """Section baru mulai di halaman baru hanya kalau sisa halaman < ~1/3 --
    menghindari halaman yang hanya berisi sisa beberapa baris section sebelumnya."""
    return [Spacer(1, 0.25 * inch), CondPageBreak(3.3 * inch)]


def _img(path: Path, w: float = 6.6, h: float = 3.0) -> Image:
    return Image(str(path), width=w * inch, height=h * inch)


def _ok(m) -> bool:
    """Metrik S1 yang layak untuk statistik dB: flag PASS dan bukan satuan linear."""
    return m["flag"] == "PASS" and not m["linear"]


def _db(m) -> bool:
    """Metrik S1 bernilai dB (termasuk FAIL) -- untuk statistik 'semua scene'."""
    return m["mean_db"] is not None and not m["linear"]


def _cell_style():
    return ParagraphStyle("CellSmall", fontName=_FONTS["body"], fontSize=7, leading=8.5)


def _table(rows: list[list], col_widths: list[float], S: _Styles | None = None) -> Table:
    col_widths = _fit_widths(col_widths)
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    style = [
        ("FONTNAME", (0, 0), (-1, -1), _FONTS["body"]),
        ("FONTNAME", (0, 0), (-1, 0), _FONTS["bold"]),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#BBBBBB")),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4C72B0")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]
    for i in range(1, len(rows)):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#F4F6FA")))
    t.setStyle(TableStyle(style))
    t.spaceAfter = 8
    return t


def _fit_widths(widths: list[float]) -> list[float]:
    total = sum(widths)
    if total > _TEXT_W:
        return [w * _TEXT_W / total for w in widths]
    return widths


def _kv_table(pairs: list[tuple[str, str]], S: _Styles) -> Table:
    rows = [[Paragraph(f"<b>{_e(k)}</b>", S.cell), Paragraph(_e(str(v)), S.cell)] for k, v in pairs]
    t = Table(rows, colWidths=[2.2 * inch, 4.5 * inch])
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#CCCCCC")),
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F4F6FA")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    t.spaceAfter = 8
    return t


def _make_page_decorator(dataset_name: str, generated_at: datetime, font: str = "Helvetica"):
    """Header/footer (report.md, "Header/Footer: Page number, dataset name,
    generation date") -- digambar lewat canvas langsung."""
    def _decorate(canvas, doc):
        canvas.saveState()
        canvas.setFont(font, 8)
        canvas.setFillColor(colors.grey)
        canvas.drawString(0.75 * inch, A4[1] - 0.55 * inch, f"Trinity DataLab — {dataset_name}")
        canvas.drawRightString(A4[0] - 0.75 * inch, A4[1] - 0.55 * inch, f"Generated {generated_at:%Y-%m-%d}")
        canvas.setStrokeColor(colors.HexColor("#DDDDDD"))
        canvas.line(0.75 * inch, A4[1] - 0.62 * inch, A4[0] - 0.75 * inch, A4[1] - 0.62 * inch)
        canvas.drawCentredString(A4[0] / 2, 0.5 * inch, f"Page {doc.page}")
        canvas.restoreState()
    return _decorate


def _bbox_bounds(wkt: str | None) -> tuple[float, float, float, float] | None:
    if not wkt:
        return None
    try:
        from shapely import wkt as shp_wkt
        return tuple(shp_wkt.loads(wkt).bounds)
    except Exception:
        return None


def _cos_deg(lat: float) -> float:
    import math
    return math.cos(math.radians(lat))


def _bbox_area_km2(b) -> float:
    w, s, e, n = b
    return (e - w) * 111.32 * _cos_deg((s + n) / 2) * (n - s) * 110.57


def _ascii_bbox(b, label: str) -> str:
    """Peta ASCII: bingkai konteks (bbox diperluas 50%) dengan area studi dan
    titik pusat ★, label lintang/bujur di tepi."""
    w, s, e, n = b
    dx, dy = (e - w) * 0.5 or 0.1, (n - s) * 0.5 or 0.1
    W, E, S_, N = w - dx, e + dx, s - dy, n + dy
    cols, rows = 50, 13
    grid = [[" "] * cols for _ in range(rows)]
    cx = lambda lon: int(round((lon - W) / (E - W) * (cols - 1)))  # noqa: E731
    cy = lambda lat: int(round((N - lat) / (N - S_) * (rows - 1)))  # noqa: E731
    x0, x1, y0, y1 = cx(w), cx(e), cy(n), cy(s)
    for x in range(x0, x1 + 1):
        grid[y0][x] = grid[y1][x] = "─"
    for y in range(y0, y1 + 1):
        grid[y][x0] = grid[y][x1] = "│"
    grid[y0][x0], grid[y0][x1], grid[y1][x0], grid[y1][x1] = "┌", "┐", "└", "┘"
    title = "[Study Region]"
    for i, ch in enumerate(title):
        if x0 + 2 + i < x1:
            grid[y0 + 1][x0 + 2 + i] = ch
    mx, my = cx((w + e) / 2), cy((s + n) / 2)
    tag = f"★ {label[:14]}"
    start = max(x0 + 1, min(mx - 1, x1 - len(tag)))
    for i, ch in enumerate(tag):
        if start + i < x1:
            grid[my][start + i] = ch
    lat_lbl = lambda v: f"{abs(v):.2f}°{'S' if v < 0 else 'N'}"  # noqa: E731
    out = [f"┌{'─' * (cols + 12)}┐", f"│{('CONTEXT: ' + label).center(cols + 12)[:cols + 12]}│"]
    for y in range(rows):
        lbl = lat_lbl(n) if y == y0 else lat_lbl(s) if y == y1 else lat_lbl((s + n) / 2) if y == my else ""
        out.append(f"│{lbl:>9} {''.join(grid[y])}  │")
    lon_line = [" "] * cols
    for x, v in ((x0, w), (x1, e)):
        txt = f"{v:.2f}°E"
        st_ = max(0, min(cols - len(txt), x - len(txt) // 2))
        for i, ch in enumerate(txt):
            lon_line[st_ + i] = ch
    out.append(f"│{'':>9} {''.join(lon_line)}  │")
    out.append(f"└{'─' * (cols + 12)}┘")
    return "\n".join(out)


def _period_months(st) -> list[tuple[int, int]]:
    if not st.period_start or not st.period_end:
        return []
    out = []
    y, m = st.period_start.year, st.period_start.month
    while (y, m) <= (st.period_end.year, st.period_end.month):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _days_in_period_month(st, y, m) -> int:
    first = date(y, m, 1)
    last = date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)
    a, b = max(first, st.period_start), min(last, st.period_end)
    return max((b - a).days + 1, 0)


def _ascii_daily_timeline(st, src) -> str:
    days = set(st.obs_dates.get(src, []))
    lines = ["           " + "".join(str(d % 10) for d in range(1, 32)) + "   obs"]
    for y, m in _period_months(st):
        row = []
        n_obs = 0
        for d in range(1, 32):
            try:
                day = date(y, m, d)
            except ValueError:
                row.append(" ")
                continue
            if day < st.period_start or day > st.period_end:
                row.append(" ")
            elif day in days:
                row.append("▓")
                n_obs += 1
            else:
                row.append("░")
        lines.append(f"{rs.MONTH_NAMES[m][:3]} {y}   {''.join(row)}  {n_obs:>3}/{_days_in_period_month(st, y, m)}")
    lines.append("Legend: ▓ = Observation, ░ = Gap")
    return "\n".join(lines)


def _ascii_month_grid(st) -> str:
    counts = {}
    for s in st.s1_scenes:
        if s["date"]:
            counts[(s["date"].year, s["date"].month)] = counts.get((s["date"].year, s["date"].month), 0) + 1
    months = set(_period_months(st))
    years = sorted({y for y, _ in months})
    lines = ["        " + "  ".join(n[:3].upper() for n in rs.MONTH_NAMES[1:])]
    for y in years:
        cells = []
        for m in range(1, 13):
            if (y, m) not in months:
                cells.append("   ")
                continue
            c = counts.get((y, m), 0)
            cells.append("▓▓▓" if c >= 8 else "▓▓░" if c >= 4 else "▓░░" if c >= 1 else "░░░")
        lines.append(f"{y}:   " + "  ".join(cells))
        lines.append("       " + "  ".join(f"{counts.get((y, m), 0):>3}" if (y, m) in months else "   " for m in range(1, 13)))
    lines.append("Legend: ▓▓▓ ≥ 8 scenes, ▓▓░ 4–7, ▓░░ 1–3, ░░░ 0 (number = scene count)")
    return "\n".join(lines)


def _s1_time_of_day(st) -> dict[str, list[float]]:
    out = {}
    for s in st.s1_scenes:
        dt = s["datetime"]
        if dt is None:
            continue
        utc = dt.astimezone(timezone.utc)
        h = utc.hour + utc.minute / 60
        if h == 0 and utc.second == 0:
            continue  # hanya tanggal, tanpa jam
        out.setdefault(s["orbit"], []).append(h)
    return out


def _hhmm(h: float) -> str:
    return f"{int(h) % 24:02d}:{int(round((h % 1) * 60)) % 60:02d}"


def _intervals(days) -> list[int]:
    days = sorted(days)
    return [(b - a).days for a, b in zip(days, days[1:])]


def _vv_vh_ratio(st) -> dict:
    by_scene = {}
    for m in st.s1_metrics:
        if m["mean_db"] is not None and _ok(m):
            by_scene.setdefault(m["scene_id"], {})[m["band"]] = (m["mean_db"], m["date"])
    all_, monthly = [], {}
    for bands in by_scene.values():
        if "VV" in bands and "VH" in bands:
            r = bands["VV"][0] - bands["VH"][0]
            all_.append(r)
            d = bands["VV"][1]
            if d:
                monthly.setdefault((d.year, d.month), []).append(r)
    return {"all": all_, "monthly": monthly}


def _month_interpretation(r, overall) -> str:
    if r["mean"] is None:
        return "—"
    diff = r["mean"] - overall
    level = ("far above" if diff > 1.5 else "above" if diff > 0.5 else
             "far below" if diff < -1.5 else "below" if diff < -0.5 else "near")
    txt = f"{level} the period mean ({diff:+.2f} dB)"
    if (r["valid"] or 1) < 0.6:
        txt += "; low valid coverage — low weight"
    if r["std"] and r["std"] > 3:
        txt += "; high variability (FAIL/zero-fill scenes present)"
    return txt


def _mean_valid(st, src) -> float | None:
    obs = [o.valid_frac for bands in st.raster.get(src, {}).values() for o in bands]
    return mean(obs) * 100 if obs else None


def _consistency_pairs():
    return [
        (("MODIS", "NDWI"), ("MODIS", "FLOOD"), "NDWI ↔ % flood pixels (MODIS)", 1),
        (("GPM", "RAIN_7D"), ("MODIS", "FLOOD"), "7-day rain (GPM) ↔ % flood (MODIS)", 1),
        (("GPM", "RAIN_7D"), ("MODIS", "NDWI"), "7-day rain (GPM) ↔ NDWI (MODIS)", 1),
        (("GPM", "RAIN_7D"), ("SENTINEL1", "VV"), "7-day rain (GPM) ↔ VV (S1)", -1),
    ]


def _pair_corr(st, a, b) -> tuple[float | None, int]:
    def series(src, band):
        if src == "SENTINEL1":
            out = {}
            for m in st.s1_metrics:
                if m["band"] == band and m["mean_db"] is not None and _ok(m) and m["date"]:
                    out.setdefault(m["date"], []).append(m["mean_db"])
            return {d: mean(v) for d, v in out.items()}
        return {o.day: o.mean for o in st.raster_band(src, band)
                if o.mean is not None and (src != "MODIS" or o.valid_frac >= .5)}
    sa, sb = series(*a), series(*b)
    common = sorted(set(sa) & set(sb))
    return rs.pearson([sa[d] for d in common], [sb[d] for d in common]), len(common)


def _monthly_corr(table, a, b) -> float | None:
    pairs = [(r[a], r[b]) for r in table if r.get(a) is not None and r.get(b) is not None]
    return rs.pearson([x for x, _ in pairs], [y for _, y in pairs]) if len(pairs) >= 3 else None


_EVENT_MM = 10.0  # batas hujan (mm/hari) untuk menghitung durasi kejadian


def _extreme_events(st) -> list[dict]:
    rain = [o for o in st.raster_band("GPM", "RAIN_24H") if o.mean is not None]
    by_day = {o.day: o for o in rain}
    r72 = {o.day: o.mean for o in st.raster_band("GPM", "RAIN_72H")}
    out = []
    for o in sorted(rain, key=lambda o: o.mean, reverse=True)[:5]:
        dur = 1
        d = o.day - timedelta(days=1)
        while d in by_day and by_day[d].mean >= _EVENT_MM:
            dur += 1
            d -= timedelta(days=1)
        d = o.day + timedelta(days=1)
        while d in by_day and by_day[d].mean >= _EVENT_MM:
            dur += 1
            d += timedelta(days=1)
        season = next(lbl for k, lbl, ms in rs.SEASONS if o.day.month in ms)
        out.append({"day": o.day, "mean": o.mean, "max": o.max, "r72": r72.get(o.day), "duration": dur, "season": season})
    return out


def _gpm_pixels(rain) -> str:
    if not rain:
        return "-"
    return str(max(o.pixels for o in rain))


def _tier_endpoints(st, src):
    tiers = st.tiers.get(src) or []
    return (tiers[0], tiers[-1]) if len(tiers) >= 2 else (None, None)


def _worst_severity(items):
    for s in ("HIGH", "MEDIUM", "LOW"):
        if any(i["severity"] == s for i in items):
            return s
    return None


def _percentile(vals, q) -> float:
    s = sorted(vals)
    if not s:
        return float("nan")
    k = (len(s) - 1) * q
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def _health_label(score: float) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Acceptable"
    return "Poor"


def _n(x, d: int = 2, unit: str = "") -> str:
    if x is None:
        return "—"
    try:
        if x != x:  # NaN
            return "—"
    except TypeError:
        pass
    return f"{x:,.{d}f}{unit}"


def _fmt_mb(mb) -> str:
    if mb is None:
        return "—"
    return _fmt_bytes(mb * 1024 * 1024)


def _fmt_sec(s) -> str:
    if s is None:
        return "—"
    if s < 1:
        return f"{s * 1000:.0f} ms"
    if s < 120:
        return f"{s:.1f} s"
    return f"{s / 60:.1f} min"


def _fmt_bytes(n) -> str:
    n = n or 0
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _e(s) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# kompatibilitas nama lama
_escape = _e
