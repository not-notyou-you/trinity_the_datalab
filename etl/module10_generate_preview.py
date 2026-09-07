# etl/module10_generate_preview.py
"""
Tier PREVIEW: render PNG siap-pandang dari produk GOLD satu tanggal akuisisi.

Posisi di pipeline:

    ... -> GOLD_EXPORT (gold/) -> PREVIEW (preview/) -> FUSION (fusion/)

PREVIEW dijalankan SETELAH semua input GOLD satu tanggal lengkap (Sentinel-1
dari scene itu sendiri, MODIS/GPM dari `module9_fusion.ensure_aux_inputs_for_date`)
dan SEBELUM fusion menulis HDF5. Urutan itu bukan kebetulan: dataset yang cuma
meminta tier FUSION akan menghapus gold/ di tahap cleanup, jadi kalau preview
digenerate belakangan tidak ada lagi rasternya untuk dirender. Dengan urutan
ini, PNG di preview/ tetap jadi rekaman visual GOLD walaupun GeoTIFF-nya
sendiri sudah dipangkas.

Dua jenis render, dua tujuan berbeda:

    grayscale/  Stretch persentil 2–98 per-berkas, colormap netral (abu-abu).
                Untuk pembacaan ilmiah: tidak ada hue yang mengarang struktur
                yang tidak ada di data, dan kontras dimaksimalkan ke sebaran
                nilai berkas itu sendiri.
    colored/    Colormap per-source dengan rentang yang punya arti fisik
                (NDVI/NDWI dipatok -1..1, hujan dipatok mulai 0). Untuk
                publikasi/presentasi: warna bisa dibaca lintas tanggal karena
                skalanya tidak ikut bergeser mengikuti isi berkas.

Keduanya ditulis RGBA/LA — piksel NoData jadi transparan, bukan hitam. Hitam
adalah nilai yang sah untuk backscatter rendah (air tenang), jadi memetakan
NoData ke hitam persis menghapus beda antara "air" dan "tidak ada data".

Nama berkas GOLD tidak pernah ditebak dari string literal di sini: MODIS/GPM
dicari lewat `module7.band_filename`/`module8.band_filename` (tempat pola nama
itu didefinisikan), dan Sentinel-1 lewat dict yang dioper orchestrator atau
glob per-band sebagai cadangan untuk pemakaian CLI.

Colormap sengaja jadi konstanta modul, bukan setelan di config.json — sama
seperti `COG_PROFILE` di module4 dan `FUSION_LAYERS` di module9. Arti warna
adalah bagian dari kontrak data (NDVI hijau = vegetasi), bukan preferensi
tampilan; membuatnya bisa diubah per-instalasi berarti dua dataset dengan
warna sama bisa berarti hal berbeda.

CLI:
    python -m etl.module10_generate_preview <dataset_id> <dataset_name> <YYYYMMDD>
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date as date_type, datetime, timezone
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling

from etl import folder_manager as fm
from etl import module7_modis_download as m7
from etl import module8_gpm_download as m8

logger = logging.getLogger(__name__)

MODULE = "MODULE10_PREVIEW"

# Lebar maksimum PNG. 1024 px cukup untuk dilihat penuh di layar dan di-zoom
# sedikit, tapi tetap ~200–600 KB per berkas — raster S1 penuh (belasan ribu
# piksel) akan jadi puluhan MB per PNG dan membuat tier ini lebih besar dari
# GOLD yang dirender-nya.
MAX_WIDTH = 1024

# Stretch persentil default untuk folder grayscale/. 2–98 memangkas ekor
# outlier (speckle terang, piksel rusak) yang kalau ikut akan menekan seluruh
# citra jadi abu-abu rata.
PCT_LOW = 2.0
PCT_HIGH = 98.0

# Level kompresi PNG. 6 adalah titik henti yang wajar: 9 cuma menghemat ~3%
# untuk citra kontinu seperti ini tapi 3–4x lebih lambat.
PNG_COMPRESS_LEVEL = 6


@dataclass(frozen=True)
class PreviewSpec:
    """Satu lapisan yang dirender. `key` sekaligus jadi nama berkas PNG-nya
    (`{key}.png`) di kedua subfolder, supaya grayscale dan colored bisa
    dipasangkan di UI tanpa tabel pemetaan terpisah."""

    key: str
    source: str  # sentinel1 | modis | gpm
    band: str  # band_name seperti di data_products
    label: str
    units: str
    cmap: str
    # "percentile": rentang dari isi berkas (2–98).
    # "fixed":      rentang tetap vmin..vmax, arti fisik lintas tanggal.
    # "zero_based": 0..persentil-atas — nol selalu berarti nol.
    scale: str
    vmin: float | None = None
    vmax: float | None = None
    # Nilai di bawah ini dianggap "tidak ada fenomena" dan dibuat transparan
    # di render colored (hujan 0 mm bukan informasi, cuma latar).
    transparent_below: float | None = None
    # Konversi ke desibel sebelum di-stretch. Wajib untuk backscatter SAR —
    # lihat _maybe_to_db().
    log_db: bool = False
    interpretation: str = ""


# Urutan di sini adalah urutan tampil di UI dan di preview_metadata.json.
PREVIEW_SPECS: tuple[PreviewSpec, ...] = (
    PreviewSpec(
        key="s1_vv",
        source="sentinel1",
        band="VV",
        label="Sentinel-1 VV",
        units="dB",
        # Sequential perseptual-uniform: backscatter adalah besaran berurut,
        # bukan divergen, jadi tidak boleh pakai colormap dua-kutub.
        cmap="viridis",
        scale="percentile",
        log_db=True,
        interpretation=(
            "Backscatter ko-polarisasi. Gelap = permukaan halus yang "
            "memantulkan sinyal menjauh dari sensor (air tenang, jalan aspal, "
            "sawah tergenang); terang = permukaan kasar atau bangunan."
        ),
    ),
    PreviewSpec(
        key="s1_vh",
        source="sentinel1",
        band="VH",
        label="Sentinel-1 VH",
        units="dB",
        cmap="viridis",
        scale="percentile",
        log_db=True,
        interpretation=(
            "Backscatter silang-polarisasi, didominasi hamburan volume. "
            "Lebih peka ke vegetasi dan tegakan daripada VV, dan kontras "
            "air-vs-darat biasanya lebih tajam."
        ),
    ),
    PreviewSpec(
        key="modis_ndvi",
        source="modis",
        band="NDVI",
        label="MODIS NDVI",
        units="indeks",
        # Divergen di sekitar 0 karena tanda NDVI-nya sendiri bermakna:
        # negatif = air/awan, positif = vegetasi.
        cmap="RdYlGn",
        scale="fixed",
        vmin=-1.0,
        vmax=1.0,
        interpretation=(
            "Normalized Difference Vegetation Index. Merah (<0) = air, awan, "
            "atau lahan terbuka; kuning (~0.2) = vegetasi jarang; hijau (>0.6) "
            "= kanopi rapat. Skala dipatok -1..1 sehingga warna bisa "
            "dibandingkan langsung antar tanggal."
        ),
    ),
    PreviewSpec(
        key="modis_ndwi",
        source="modis",
        band="NDWI",
        label="MODIS NDWI",
        units="indeks",
        cmap="BrBG",
        scale="fixed",
        vmin=-1.0,
        vmax=1.0,
        interpretation=(
            "Normalized Difference Water Index (formulasi McFeeters: "
            "green/NIR). Cokelat (<0) = daratan kering; biru-hijau (>0) = "
            "badan air terbuka. Ambang genangan biasanya diambil di sekitar 0."
        ),
    ),
    PreviewSpec(
        key="gpm_rain_24h",
        source="gpm",
        band="RAIN_24H",
        label="GPM Curah Hujan 24 jam",
        units="mm",
        # Sequential gelap-di-atas: intensitas hujan tinggi harus jadi warna
        # paling pekat, bukan paling terang, supaya menonjol di atas latar.
        cmap="YlGnBu",
        scale="zero_based",
        transparent_below=0.1,
        interpretation=(
            "Akumulasi hujan 24 jam menjelang akuisisi. Pemicu langsung banjir "
            "kilat. Piksel <0.1 mm dibuat transparan supaya area kering tidak "
            "terbaca sebagai 'hujan sangat sedikit'."
        ),
    ),
    PreviewSpec(
        key="gpm_rain_72h",
        source="gpm",
        band="RAIN_72H",
        label="GPM Curah Hujan 72 jam",
        units="mm",
        cmap="YlGnBu",
        scale="zero_based",
        transparent_below=0.1,
        interpretation=(
            "Akumulasi 3 hari. Menangkap hujan bertingkat yang menjenuhkan "
            "tanah sebelum kejadian puncak."
        ),
    ),
    PreviewSpec(
        key="gpm_rain_7d",
        source="gpm",
        band="RAIN_7D",
        label="GPM Curah Hujan 7 hari",
        units="mm",
        cmap="YlGnBu",
        scale="zero_based",
        transparent_below=0.1,
        interpretation=(
            "Akumulasi sepekan. Proksi kondisi kelembapan awal — hujan yang "
            "sama menghasilkan genangan jauh lebih luas di atas tanah jenuh."
        ),
    ),
)

# Komposit RGB Sentinel-1: kanal warna dipetakan ke besaran polarimetrik, bukan
# ke warna asli apa pun. Ini konvensi baku untuk GRD dual-pol.
S1_RGB_KEY = "s1_rgb_composite"
S1_RGB_LABEL = "Sentinel-1 Komposit RGB (VV / VH / VV-VH)"
S1_RGB_INTERPRETATION = (
    "False color: R = VV, G = VH, B = selisih VV-VH (dB). Air terbuka jadi "
    "gelap/kebiruan (VV dan VH sama-sama rendah), lahan bervegetasi jadi "
    "kehijauan (VH relatif tinggi), area terbangun jadi merah muda hingga "
    "putih (VV sangat tinggi). Berguna untuk memisahkan genangan dari bayangan "
    "topografi yang di citra satu-band terlihat sama gelapnya."
)


# ---------------------------------------------------------------------------
# Pencarian berkas GOLD
# ---------------------------------------------------------------------------

def _s1_gold_path(
    dataset_id: int, dataset_name: str, s1_scene_key: str | None, band: str
) -> Path | None:
    """Cari COG GOLD Sentinel-1 satu band. Nama berkasnya diturunkan dari nama
    berkas SILVER (module4 memakai `silver_path.name` apa adanya), jadi tidak
    ada satu fungsi penamaan yang bisa dipanggil seperti pada MODIS/GPM —
    band-nya dicocokkan lewat sufiks `_{BAND}_lee.tif` yang ditulis
    module3_lee_filter."""
    if not s1_scene_key:
        return None
    scene_dir = fm.get_scene_dir(dataset_id, dataset_name, "gold", "sentinel1", s1_scene_key)
    if not scene_dir.is_dir():
        return None
    matches = sorted(scene_dir.glob(f"*_{band.upper()}_lee.tif"))
    if not matches:
        # Cadangan longgar: instalasi lama bisa punya sufiks berbeda.
        matches = sorted(p for p in scene_dir.glob("*.tif") if f"_{band.upper()}" in p.name)
    return matches[0] if matches else None


def _aux_gold_path(
    dataset_id: int, dataset_name: str, source: str, band: str, date_key: str
) -> Path | None:
    """Cari COG GOLD MODIS/GPM satu band untuk satu tanggal."""
    if source == "modis":
        filename = m7.band_filename(band, date_key)
    elif source == "gpm":
        # band RAIN_24H -> window "24h", bentuk yang dipakai module8.
        filename = m8.band_filename(band.removeprefix("RAIN_").lower(), date_key)
    else:  # pragma: no cover - dijaga PREVIEW_SPECS
        raise ValueError(f"source aux tidak dikenal: {source!r}")
    path = fm.get_scene_dir(dataset_id, dataset_name, "gold", source, date_key) / filename
    return path if path.exists() else None


def resolve_gold_inputs(
    dataset_id: int,
    dataset_name: str,
    date_key: str,
    s1_scene_key: str | None = None,
    s1_gold_files: dict[str, str] | None = None,
) -> dict[str, Path]:
    """
    Petakan `PreviewSpec.key` -> path GOLD yang ada di disk.

    `s1_gold_files` ({band: path}) adalah keluaran `export_scene_to_gold` yang
    dioper orchestrator: dipakai lebih dulu karena itu jawaban pasti untuk
    scene yang baru saja diproses. Tanpa itu (pemakaian CLI / regenerasi),
    path dicari lewat glob di folder scene.

    Key yang berkasnya tidak ada sengaja tidak muncul di hasil, bukan
    dipetakan ke None — pemanggil melaporkannya sebagai "dilewati", dan
    dataset yang MODIS/GPM-nya gagal diunduh tetap dapat preview Sentinel-1.
    """
    inputs: dict[str, Path] = {}
    for spec in PREVIEW_SPECS:
        if spec.source == "sentinel1":
            path: Path | None = None
            if s1_gold_files and s1_gold_files.get(spec.band):
                candidate = Path(s1_gold_files[spec.band])
                path = candidate if candidate.exists() else None
            if path is None:
                path = _s1_gold_path(dataset_id, dataset_name, s1_scene_key, spec.band)
        else:
            path = _aux_gold_path(dataset_id, dataset_name, spec.source, spec.band, date_key)
        if path is not None:
            inputs[spec.key] = path
    return inputs


# ---------------------------------------------------------------------------
# Pembacaan raster & normalisasi
# ---------------------------------------------------------------------------

@dataclass
class _Layer:
    """Satu raster yang sudah dibaca-turun dan dimask."""

    data: np.ndarray  # float32, nilai di piksel mask tidak berarti
    mask: np.ndarray  # True = NoData
    height: int
    width: int
    src_height: int
    src_width: int
    stats: dict = field(default_factory=dict)

    @property
    def valid(self) -> np.ndarray:
        return self.data[~self.mask]


# Lantai dB. Sigma0 linear bisa menyentuh nol di piksel bayangan radar, dan
# log10(0) = -inf akan meracuni seluruh perhitungan persentil. -40 dB jauh di
# bawah backscatter apa pun yang punya arti fisik di citra GRD.
DB_FLOOR = -40.0


def _maybe_to_db(data: np.ndarray, mask: np.ndarray) -> tuple[np.ndarray, bool]:
    """Konversi sigma0 linear -> desibel, kalau datanya memang masih linear.

    GOLD Sentinel-1 di pipeline ini menyimpan sigma0 LINEAR (config
    `cog_convert_db` default false), sebarannya menjulur ekstrem ke kanan:
    persentil 98 ada di ~1.2 sementara maksimumnya ratusan. Stretch persentil
    langsung di atas nilai linear menempelkan hampir semua piksel darat ke
    ujung gelap dan preview-nya jadi bintik-bintik gelap tak terbaca.
    Backscatter SAR memang dibaca dalam dB justru karena itu.

    Konversinya dijaga auto-deteksi, bukan diasumsikan: kalau ada nilai valid
    yang <= 0 atau persentil atasnya sudah negatif, datanya sudah dalam dB
    (mis. instalasi yang menyalakan `cog_convert_db`) dan mengambil log kedua
    kali akan merusaknya.
    """
    valid = data[~mask]
    if valid.size == 0:
        return data, False
    if valid.min() <= 0 or np.percentile(valid, PCT_HIGH) <= 0:
        return data, False  # sudah dB
    with np.errstate(divide="ignore", invalid="ignore"):
        db = 10.0 * np.log10(np.where(mask, 1.0, data))
    return np.maximum(db, DB_FLOOR).astype(np.float32), True


def _read_downsampled(
    path: Path, max_width: int = MAX_WIDTH, log_db: bool = False
) -> _Layer:
    """Baca band 1 dengan downsampling langsung di driver.

    Downsample dilakukan rasterio (bukan setelah membaca penuh) supaya raster
    S1 belasan ribu piksel tidak pernah masuk memori utuh: COG punya overview
    internal, jadi pembacaan ini membaca level piramida yang sudah ada.

    `log_db=True` mengubah sigma0 linear jadi dB setelah masking — lihat
    _maybe_to_db(). Rata-rata downsample sengaja dilakukan di ranah linear
    (rata-rata daya), baru dikonversi: merata-ratakan nilai dB adalah
    rata-rata geometrik daya, yang bukan yang dimaksud di sini.
    """
    with rasterio.open(path) as src:
        scale = min(1.0, max_width / src.width)
        out_w = max(1, int(round(src.width * scale)))
        out_h = max(1, int(round(src.height * scale)))
        data = src.read(
            1, out_shape=(out_h, out_w), resampling=Resampling.average
        ).astype(np.float32)
        nodata = src.nodata
        src_h, src_w = src.height, src.width

    mask = ~np.isfinite(data)
    if nodata is not None and np.isfinite(nodata):
        mask |= np.isclose(data, float(nodata))

    converted = False
    if log_db:
        data, converted = _maybe_to_db(data, mask)

    layer = _Layer(
        data=data, mask=mask, height=out_h, width=out_w,
        src_height=src_h, src_width=src_w,
    )
    valid = layer.valid
    layer.stats = {
        "nodata_percent": round(float(mask.mean()) * 100, 2),
        "min": round(float(valid.min()), 4) if valid.size else None,
        "max": round(float(valid.max()), 4) if valid.size else None,
        "mean": round(float(valid.mean()), 4) if valid.size else None,
        "converted_to_db": converted,
    }
    return layer


def _stretch_range(layer: _Layer, spec: PreviewSpec | None) -> tuple[float, float]:
    """Rentang (lo, hi) yang dipakai memetakan nilai ke 0..1.

    Tanpa `spec` (dan untuk folder grayscale/) selalu persentil: tujuannya
    kontras maksimum untuk berkas itu sendiri. Dengan `spec`, mode diambil
    dari `spec.scale` supaya render colored bisa dibandingkan antar tanggal.
    """
    valid = layer.valid
    if valid.size == 0:
        return 0.0, 1.0

    if spec is not None and spec.scale == "fixed":
        return float(spec.vmin), float(spec.vmax)

    if spec is not None and spec.scale == "zero_based":
        hi = float(np.percentile(valid, PCT_HIGH))
        # Hujan sering nyaris seluruhnya nol; persentil atas bisa jatuh di 0
        # dan membuat seluruh citra jenuh. Jatuh balik ke nilai maksimum.
        if hi <= 0:
            hi = float(valid.max())
        return 0.0, hi if hi > 0 else 1.0

    lo = float(np.percentile(valid, PCT_LOW))
    hi = float(np.percentile(valid, PCT_HIGH))
    if hi <= lo:
        hi = lo + 1.0
    return lo, hi


def _normalize(layer: _Layer, lo: float, hi: float) -> np.ndarray:
    """Nilai -> 0..1, dijepit. Piksel NoData diisi 0 (tidak dipakai: alpha-nya
    nol) supaya tidak ada NaN yang bocor ke colormap."""
    span = hi - lo if hi > lo else 1.0
    norm = (layer.data - lo) / span
    norm = np.clip(norm, 0.0, 1.0)
    return np.where(layer.mask, 0.0, norm)


def _alpha(layer: _Layer, spec: PreviewSpec | None = None) -> np.ndarray:
    """Kanal alpha uint8: 0 di NoData, dan 0 juga di bawah
    `transparent_below` kalau spec memintanya."""
    opaque = ~layer.mask
    if spec is not None and spec.transparent_below is not None:
        opaque &= layer.data >= spec.transparent_below
    return (opaque * 255).astype(np.uint8)


# ---------------------------------------------------------------------------
# Penulisan PNG
# ---------------------------------------------------------------------------

def _save_png(img, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, format="PNG", optimize=True, compress_level=PNG_COMPRESS_LEVEL)
    return path


def _render_grayscale(layer: _Layer, out_path: Path) -> Path:
    """PNG mode LA: satu kanal luminansi (nilai ilmiah) + satu kanal alpha
    (masker NoData). Tetap 'satu kanal data' seperti yang dimaksud tier ini —
    alpha bukan data, dia cuma memisahkan 'gelap' dari 'tidak ada'."""
    from PIL import Image

    lo, hi = _stretch_range(layer, None)
    gray = (_normalize(layer, lo, hi) * 255).astype(np.uint8)
    img = Image.fromarray(np.dstack([gray, _alpha(layer)]), mode="LA")
    return _save_png(img, out_path)


def _render_colored(layer: _Layer, spec: PreviewSpec, out_path: Path) -> Path:
    """PNG RGBA hasil colormap matplotlib."""
    from matplotlib import colormaps
    from PIL import Image

    lo, hi = _stretch_range(layer, spec)
    rgba = (colormaps[spec.cmap](_normalize(layer, lo, hi)) * 255).astype(np.uint8)
    rgba[..., 3] = _alpha(layer, spec)
    img = Image.fromarray(rgba, mode="RGBA")
    return _save_png(img, out_path)


def _render_s1_rgb(vv: _Layer, vh: _Layer, out_path: Path) -> Path | None:
    """Komposit false-color VV/VH/(VV-VH).

    Ketiga kanal di-stretch persentil sendiri-sendiri: VV dan VH punya rentang
    dB yang berbeda (VH umumnya 5–10 dB lebih rendah), jadi memakai satu
    rentang bersama akan membuat kanal hijau nyaris gelap total dan komposit
    ini kehilangan gunanya.
    """
    from PIL import Image

    if vv.data.shape != vh.data.shape:
        logger.warning(
            "[M10] komposit RGB dilewati: dimensi VV %s != VH %s",
            vv.data.shape, vh.data.shape,
        )
        return None

    diff = _Layer(
        data=vv.data - vh.data,
        mask=vv.mask | vh.mask,
        height=vv.height, width=vv.width,
        src_height=vv.src_height, src_width=vv.src_width,
    )

    channels = []
    for lyr in (vv, vh, diff):
        lo, hi = _stretch_range(lyr, None)
        channels.append((_normalize(lyr, lo, hi) * 255).astype(np.uint8))

    alpha = ((~(vv.mask | vh.mask)) * 255).astype(np.uint8)
    img = Image.fromarray(np.dstack(channels + [alpha]), mode="RGBA")
    return _save_png(img, out_path)


# ---------------------------------------------------------------------------
# Metadata sidecar
# ---------------------------------------------------------------------------

def _write_json(path: Path, payload: dict) -> Path:
    """Tulis JSON secara atomik (tmp lalu replace), pola yang sama dengan
    `folder_manager.write_dataset_metadata` — sidecar yang setengah tertulis
    saat proses dihentikan akan membuat UI gagal parse."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    tmp.replace(path)
    return path


def _grayscale_info(entries: list[dict]) -> dict:
    return {
        "kind": "grayscale",
        "purpose": "Representasi ilmiah — pembacaan nilai relatif per berkas.",
        "colormap": "gray (linear, tanpa hue)",
        "image_mode": "LA (luminansi 8-bit + alpha)",
        "stretch": {
            "method": "percentile",
            "percentile_low": PCT_LOW,
            "percentile_high": PCT_HIGH,
            "per_file": True,
            "note": (
                "Rentang dihitung ulang dari piksel valid tiap berkas, jadi "
                "kontras maksimal untuk berkas itu — tapi tingkat abu-abu "
                "TIDAK bisa dibandingkan antar tanggal atau antar band. "
                "Untuk perbandingan lintas waktu pakai folder colored/, yang "
                "skalanya dipatok."
            ),
        },
        "pre_transform": (
            "Band Sentinel-1 dikonversi ke desibel (10*log10) sebelum "
            "di-stretch: GOLD menyimpan sigma0 linear yang sebarannya "
            "menjulur ekstrem, dan stretch persentil langsung di atasnya "
            "menghasilkan citra gelap tak terbaca. MODIS dan GPM dirender "
            "apa adanya. Kolom 'transform' tiap gambar mencatat mana yang "
            "kena konversi."
        ),
        "nodata": "Transparan (alpha=0). Tidak dipetakan ke hitam karena hitam adalah nilai sah untuk backscatter rendah / air.",
        "interpretation": (
            "Gelap = nilai rendah pada rentang persentil berkas ini; terang = "
            "nilai tinggi. Untuk SAR, gelap umumnya permukaan halus (air, "
            "aspal); terang umumnya permukaan kasar atau bangunan."
        ),
        "images": entries,
    }


def _colored_info(entries: list[dict]) -> dict:
    return {
        "kind": "colored",
        "purpose": "Publikasi dan presentasi — warna yang bisa dibaca lintas tanggal.",
        "image_mode": "RGBA (8-bit per kanal)",
        "colormap_strategy": {
            "sentinel1": (
                "viridis, sequential perseptual-uniform. Backscatter adalah "
                "besaran berurut, jadi colormap divergen akan mengarang titik "
                "tengah yang tidak punya arti fisik. Ditambah satu komposit "
                "false-color RGB (VV/VH/VV-VH)."
            ),
            "modis": (
                "Divergen dengan titik tengah nol, dipatok -1..1: NDVI RdYlGn "
                "(merah = air/lahan terbuka, hijau = vegetasi), NDWI BrBG "
                "(cokelat = kering, biru-hijau = air). Tanda indeksnya sendiri "
                "bermakna, jadi nol harus jatuh tepat di tengah colormap."
            ),
            "gpm": (
                "YlGnBu sequential mulai dari nol, batas atas persentil 98 per "
                "berkas. Nol adalah nol sungguhan (bukan minimum data), dan "
                "piksel di bawah 0.1 mm dibuat transparan supaya area kering "
                "tidak terbaca sebagai hujan ringan."
            ),
        },
        "nodata": "Transparan (alpha=0), sama seperti grayscale/.",
        "ideal_use_cases": [
            "Gambar untuk laporan, poster, dan presentasi",
            "Perbandingan kondisi antar tanggal (skala terpatok untuk MODIS)",
            "Overlay cepat di atas peta dasar — latar transparan langsung pas",
        ],
        "caveat": (
            "Warna sudah dikuantisasi ke 8-bit dan sebagian rentangnya "
            "dijepit. Untuk analisis kuantitatif pakai COG di gold/ atau "
            "HDF5 di fusion/, bukan PNG ini."
        ),
        "images": entries,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate_previews(
    dataset_id: int,
    dataset_name: str,
    acquisition_date: date_type | datetime | str,
    s1_scene_key: str | None = None,
    s1_gold_files: dict[str, str] | None = None,
    max_width: int = MAX_WIDTH,
    overwrite: bool = True,
) -> dict:
    """
    Render seluruh preview untuk satu tanggal akuisisi.

    Args:
        acquisition_date: tanggal scene S1; dinormalisasi ke kunci YYYYMMDD.
        s1_scene_key:     product_identifier scene S1 (untuk mencari gold/).
        s1_gold_files:    {band: path} keluaran export_scene_to_gold, kalau ada.
        overwrite:        True (default) me-render ulang semua PNG. Preview
                          adalah turunan murni dan murah, jadi menulis ulang
                          lebih aman daripada menyimpan PNG basi dari GOLD
                          versi lama. False melewati berkas yang sudah ada —
                          untuk mengisi ulang preview yang hilang saja.

    Returns:
        Ringkasan yang sama isinya dengan preview_metadata.json, ditambah
        daftar path absolut di key "files".

    Tidak pernah melempar karena satu band gagal: band yang berkas GOLD-nya
    tidak ada atau rusak masuk ke daftar "skipped" beserta alasannya. Preview
    adalah artefak turunan — kegagalan render tidak boleh menjatuhkan scene
    yang datanya sendiri baik-baik saja.
    """
    date_key = fm.date_key(acquisition_date)
    preview_dir = fm.ensure_preview_dir(dataset_id, dataset_name, date_key)
    gray_dir = fm.ensure_preview_kind_dir(dataset_id, dataset_name, date_key, "grayscale")
    color_dir = fm.ensure_preview_kind_dir(dataset_id, dataset_name, date_key, "colored")

    inputs = resolve_gold_inputs(
        dataset_id, dataset_name, date_key,
        s1_scene_key=s1_scene_key, s1_gold_files=s1_gold_files,
    )

    gray_entries: list[dict] = []
    color_entries: list[dict] = []
    written: list[Path] = []
    skipped: list[dict] = []
    layers: dict[str, _Layer] = {}

    for spec in PREVIEW_SPECS:
        src_path = inputs.get(spec.key)
        if src_path is None:
            skipped.append({
                "key": spec.key,
                "source": spec.source,
                "band": spec.band,
                "reason": "berkas GOLD tidak ada di disk",
            })
            continue

        gray_path = gray_dir / f"{spec.key}.png"
        color_path = color_dir / f"{spec.key}.png"
        if not overwrite and gray_path.exists() and color_path.exists():
            skipped.append({
                "key": spec.key, "source": spec.source, "band": spec.band,
                "reason": "sudah ada (overwrite=False)",
            })
            continue

        try:
            layer = _read_downsampled(src_path, max_width=max_width, log_db=spec.log_db)
        except Exception as exc:
            logger.exception("[M10] gagal baca %s untuk %s", src_path, spec.key)
            skipped.append({
                "key": spec.key, "source": spec.source, "band": spec.band,
                "reason": f"gagal dibaca: {exc}",
            })
            continue

        if layer.valid.size == 0:
            skipped.append({
                "key": spec.key, "source": spec.source, "band": spec.band,
                "reason": "seluruh piksel NoData",
            })
            continue

        layers[spec.key] = layer

        common = {
            "key": spec.key,
            "source": spec.source,
            "band": spec.band,
            "label": spec.label,
            "units": spec.units,
            "source_file": str(src_path),
            "width": layer.width,
            "height": layer.height,
            "source_width": layer.src_width,
            "source_height": layer.src_height,
            "statistics": layer.stats,
            "transform": "10*log10(sigma0)" if layer.stats.get("converted_to_db") else "none",
        }

        try:
            _render_grayscale(layer, gray_path)
            g_lo, g_hi = _stretch_range(layer, None)
            gray_entries.append({
                **common,
                "file": gray_path.name,
                "colormap": "gray",
                "value_range": [round(g_lo, 4), round(g_hi, 4)],
                "range_method": f"persentil {PCT_LOW}-{PCT_HIGH}",
                "size_bytes": gray_path.stat().st_size,
            })
            written.append(gray_path)

            _render_colored(layer, spec, color_path)
            c_lo, c_hi = _stretch_range(layer, spec)
            color_entries.append({
                **common,
                "file": color_path.name,
                "colormap": spec.cmap,
                "value_range": [round(c_lo, 4), round(c_hi, 4)],
                "range_method": spec.scale,
                "transparent_below": spec.transparent_below,
                "interpretation": spec.interpretation,
                "size_bytes": color_path.stat().st_size,
            })
            written.append(color_path)
        except Exception as exc:
            logger.exception("[M10] gagal render %s", spec.key)
            skipped.append({
                "key": spec.key, "source": spec.source, "band": spec.band,
                "reason": f"gagal render: {exc}",
            })

    # Komposit RGB hanya mungkin kalau VV dan VH dua-duanya berhasil dibaca.
    if "s1_vv" in layers and "s1_vh" in layers:
        rgb_path = color_dir / f"{S1_RGB_KEY}.png"
        try:
            if _render_s1_rgb(layers["s1_vv"], layers["s1_vh"], rgb_path) is not None:
                color_entries.append({
                    "key": S1_RGB_KEY,
                    "source": "sentinel1",
                    "band": "VV+VH",
                    "label": S1_RGB_LABEL,
                    "units": "dB (per kanal)",
                    "file": rgb_path.name,
                    "colormap": "false-color RGB",
                    "range_method": f"persentil {PCT_LOW}-{PCT_HIGH} per kanal",
                    "channels": {"R": "VV", "G": "VH", "B": "VV - VH"},
                    "width": layers["s1_vv"].width,
                    "height": layers["s1_vv"].height,
                    "interpretation": S1_RGB_INTERPRETATION,
                    "size_bytes": rgb_path.stat().st_size,
                })
                written.append(rgb_path)
        except Exception as exc:
            logger.exception("[M10] gagal render komposit RGB")
            skipped.append({
                "key": S1_RGB_KEY, "source": "sentinel1", "band": "VV+VH",
                "reason": f"gagal render: {exc}",
            })

    gray_json = _write_json(gray_dir / "grayscale_info.json", _grayscale_info(gray_entries))
    color_json = _write_json(color_dir / "colored_info.json", _colored_info(color_entries))
    written += [gray_json, color_json]

    sources_present = sorted({e["source"] for e in gray_entries})
    metadata = {
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "acquisition_date": date_key,
        "s1_scene_key": s1_scene_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator": MODULE,
        "tier": "PREVIEW",
        "derived_from": "GOLD",
        "max_width_px": max_width,
        "png_compress_level": PNG_COMPRESS_LEVEL,
        "sources_present": sources_present,
        "counts": {
            "grayscale": len(gray_entries),
            "colored": len(color_entries),
            "total_png": len(gray_entries) + len(color_entries),
            "skipped": len(skipped),
        },
        "kinds": {
            "grayscale": {
                "dir": "grayscale",
                "info": "grayscale_info.json",
                "files": [e["file"] for e in gray_entries],
            },
            "colored": {
                "dir": "colored",
                "info": "colored_info.json",
                "files": [e["file"] for e in color_entries],
            },
        },
        "skipped": skipped,
        "usage": {
            "grayscale": "Pembacaan ilmiah satu berkas; kontras dioptimalkan per berkas.",
            "colored": "Publikasi dan perbandingan lintas tanggal; skala terpatok.",
            "not_for": "Analisis kuantitatif — pakai gold/*.tif atau fusion/*.h5.",
        },
    }
    meta_path = _write_json(preview_dir / "preview_metadata.json", metadata)
    written.append(meta_path)

    total_mb = sum(p.stat().st_size for p in written if p.exists()) / (1024 ** 2)
    logger.info(
        "[M10] PREVIEW %s: %d grayscale + %d colored PNG (%d dilewati, %.2f MB)",
        date_key, len(gray_entries), len(color_entries), len(skipped), total_mb,
    )

    return {**metadata, "files": [str(p) for p in written], "total_size_mb": round(total_mb, 3)}


def run(
    dataset_id: int,
    dataset_name: str,
    acquisition_date: date_type | datetime | str,
    **kwargs,
) -> dict:
    """Alias konsisten dengan module2/module3/module4 yang juga mengekspos `run`."""
    return generate_previews(dataset_id, dataset_name, acquisition_date, **kwargs)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) < 4:
        print(
            "Usage: python -m etl.module10_generate_preview "
            "<dataset_id> <dataset_name> <YYYYMMDD> [s1_scene_key]"
        )
        raise SystemExit(1)

    result = generate_previews(
        int(sys.argv[1]),
        sys.argv[2],
        sys.argv[3],
        s1_scene_key=sys.argv[4] if len(sys.argv) > 4 else None,
    )
    print(json.dumps(result["counts"], indent=2))
    for item in result["skipped"]:
        print(f"  dilewati {item['key']}: {item['reason']}")
