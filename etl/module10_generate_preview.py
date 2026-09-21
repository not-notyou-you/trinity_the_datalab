# etl/module10_generate_preview.py
"""
Tier PREVIEW: render PNG siap-pandang dari produk satu tanggal akuisisi.

Posisi di pipeline:

    ... -> GOLD_EXPORT (gold/) -> PREVIEW (preview/) -> FUSION (fusion/)

PREVIEW dijalankan SETELAH semua input satu tanggal lengkap (Sentinel-1 dari
scene itu sendiri, MODIS/GPM dari `module9_fusion.ensure_aux_inputs_for_date`)
dan SEBELUM fusion menulis HDF5. Urutan itu bukan kebetulan: dataset yang cuma
meminta tier FUSION akan menghapus gold/ di tahap cleanup, jadi kalau preview
digenerate belakangan tidak ada lagi rasternya untuk dirender. Dengan urutan
ini, PNG di preview/ tetap jadi rekaman visual tier sumbernya walaupun
GeoTIFF-nya sendiri sudah dipangkas.

DARI TIER MANA?
`processing_level` menentukan tier yang dibaca, mengikuti aturan yang sama
dengan fusion (DOCS/ETL.md, "Preview Stage"):

    PROCESSED -> gold/    (COG hasil Lee filter / NDVI-NDWI / akumulasi)
    RAW       -> bronze/  (S1 terkalibrasi+crop, MODIS FLOOD saja,
                           GPM curah hujan harian saja)

Level RAW karena itu me-render lebih sedikit lapisan — bukan karena gagal,
tapi karena band turunannya memang tidak pernah dihitung di jalur itu. Level
ikut ke path output (`preview/{tanggal}/{LEVEL}/...`) supaya dataset yang
meminta sebuah sumber di KEDUA level bisa menyimpan dua set PNG berdampingan
tanpa saling menimpa.

SATU TANGGAL, SATU SET PNG
Folder preview dikunci per tanggal, bukan per scene -- sama dengan granularitas
FUSION. Kalau AOI tertutup dua scene Sentinel-1 di hari yang sama (dua orbit,
atau dua frame berurutan), render scene kedua MENIMPA milik scene pertama. Ini
disengaja, tapi tidak boleh diam-diam: saat scene_key di sidecar lama berbeda
dari scene yang sedang dirender, modul menulis WARNING dan mencatat scene yang
tergantikan di `replaced_s1_scene_key` pada preview_metadata.json.

Tiga jenis render, tiga tujuan berbeda:

    grayscale/  Stretch persentil 2–98 per-berkas, colormap netral (abu-abu).
                Untuk pembacaan ilmiah: tidak ada hue yang mengarang struktur
                yang tidak ada di data, dan kontras dimaksimalkan ke sebaran
                nilai berkas itu sendiri.
    colored/    Colormap per-source dengan rentang yang punya arti fisik
                (NDVI -0.2..0.8, NDWI -0.5..0.5, hujan mulai 0, peta banjir
                MODIS per kelas). Untuk
                publikasi/presentasi: warna bisa dibaca lintas tanggal karena
                skalanya tidak ikut bergeser mengikuti isi berkas.
    composite/  False-color RGB Sentinel-1 (R=VV, G=VH, B=VV-VH). Folder
                terpisah dari colored/ karena isinya bukan satu band yang
                diberi warna melainkan tiga band yang digabung — legenda
                colormap tidak berlaku untuknya, dan menyimpannya di colored/
                membuat sidecar colored_info.json memuat satu entri yang
                skema-nya berbeda dari yang lain.

GRID BERSAMA
Kalau ada Sentinel-1, semua lapisan MODIS/GPM direproject ke grid preview S1
(ukuran PNG, extent, dan posisi piksel identik) dengan nearest-neighbour. Tanpa
itu MODIS 500 m di AOI kecil jadi PNG belasan piksel, dan GPM punya extent
berbeda sehingga tidak bisa ditumpuk. Nearest dipakai supaya piksel sensor
kasar tetap tampil sebagai blok — seperti di Worldview/GEE — bukan gradien
hasil interpolasi. Tanpa S1, raster kecil diperbesar kelipatan bulat (nearest)
mendekati MAX_SIDE.

Keduanya ditulis RGBA/LA — piksel NoData jadi transparan, bukan hitam. Hitam
adalah nilai yang sah untuk backscatter rendah (air tenang), jadi memetakan
NoData ke hitam persis menghapus beda antara "air" dan "tidak ada data".

Nama berkas sumber tidak pernah ditebak dari string literal di sini: MODIS/GPM
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
from affine import Affine
from rasterio.enums import Resampling
from rasterio.warp import reproject

from etl import folder_manager as fm
from etl import module7_modis_download as m7
from etl import module8_gpm_download as m8

logger = logging.getLogger(__name__)

MODULE = "MODULE10_PREVIEW"

# Sisi TERPANJANG maksimum PNG. 1024 px cukup untuk dilihat penuh di layar dan
# di-zoom sedikit, tapi tetap ~200–600 KB per berkas — raster S1 penuh
# (belasan ribu piksel) akan jadi puluhan MB per PNG dan membuat tier ini lebih
# besar dari GOLD yang dirender-nya.
#
# Sisi terpanjang, bukan lebar: scene Sentinel-1 yang cuma menyerempet AOI
# menghasilkan crop berbentuk jalur sempit (mis. 1488 x 8789 piksel). Membatasi
# lebarnya saja membuat PNG-nya jadi 1024 x 6048 — enam kali lebih tinggi dari
# batas yang dikira berlaku, beberapa MB per berkas, dan di galeri tampil
# sebagai pita panjang di samping preview tanggal lain yang normal.
MAX_SIDE = 1024

# Nama lama; masih diekspor supaya pemanggil luar tidak patah.
MAX_WIDTH = MAX_SIDE

# Stretch persentil default untuk folder grayscale/. 2–98 memangkas ekor
# outlier (speckle terang, piksel rusak) yang kalau ikut akan menekan seluruh
# citra jadi abu-abu rata.
PCT_LOW = 2.0
PCT_HIGH = 98.0

# Opasitas lapisan kontinu (hujan, NDVI, NDWI) saat ditumpuk di atas citra S1.
OVERLAY_OPACITY = 0.55

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
    # "percentile":  rentang dari isi berkas (2–98).
    # "fixed":       rentang tetap vmin..vmax, arti fisik lintas tanggal.
    # "zero_based":  0..persentil-atas — nol selalu berarti nol.
    # "categorical": nilai kelas diskret; warna dari `categories`, grayscale
    #                vmin..vmax, resampling selalu nearest.
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
    # Untuk scale="categorical": (nilai, warna hex, alpha 0-255, label). Nilai
    # yang tidak terdaftar dirender transparan.
    categories: tuple[tuple[int, str, int, str], ...] = ()

    @property
    def categorical(self) -> bool:
        return self.scale == "categorical"


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
        key="modis_flood",
        source="modis",
        band="FLOOD",
        label="MODIS Flood 2 hari (MCDWD)",
        units="kelas",
        cmap="categorical",
        scale="categorical",
        vmin=0.0,
        vmax=3.0,
        # Palet mengikuti konvensi peta banjir NASA: air referensi biru, banjir
        # merah. "No water" abu-abu semi-transparan supaya area yang teramati
        # kering tetap beda dari "insufficient data" (255, transparan).
        categories=(
            (0, "#bdbdbd", 110, "Tidak ada air"),
            (1, "#2171b5", 255, "Air permanen (referensi)"),
            (2, "#fd8d3c", 255, "Banjir musiman (berulang)"),
            (3, "#e31a1c", 255, "Banjir (tidak biasa)"),
        ),
        interpretation=(
            "Peta banjir MODIS MCDWD komposit 2 hari (Terra+Aqua, 250 m). Biru = "
            "air permanen, oranye = banjir musiman, merah = banjir tidak biasa, "
            "abu-abu = teramati tanpa air. Transparan = data tidak cukup "
            "(umumnya tertutup awan)."
        ),
    ),
    PreviewSpec(
        key="modis_ndvi",
        source="modis",
        band="NDVI",
        label="MODIS NDVI",
        units="indeks",
        cmap="RdYlGn",
        scale="fixed",
        # Rentang palet NDVI yang lazim (NASA/GEE): nilai < -0.2 hanya air
        # dan > 0.8 hanya kanopi paling rapat, jadi -1..1 membuang separuh
        # colormap dan membuat kota seperti Jakarta (0..0.4) kuning rata.
        vmin=-0.2,
        vmax=0.8,
        interpretation=(
            "Normalized Difference Vegetation Index (komposit 8 hari MOD09A1, "
            "awan dibuang). Merah (<0) = air atau lahan terbangun; kuning "
            "(~0.3) = vegetasi jarang; hijau (>0.6) = kanopi rapat. Skala "
            "dipatok -0.2..0.8 sehingga warna bisa dibandingkan antar tanggal. "
            "Transparan = awan."
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
        # Tetap simetris di nol (ambang air McFeeters), tapi dipersempit:
        # daratan jarang di bawah -0.5 dan air terbuka sudah jelas di +0.5.
        vmin=-0.5,
        vmax=0.5,
        interpretation=(
            "Normalized Difference Water Index (formulasi McFeeters: "
            "green/NIR). Cokelat (<0) = daratan kering; biru-hijau (>0) = "
            "badan air terbuka. Ambang genangan biasanya diambil di sekitar 0. "
            "Transparan = awan."
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
            "Curah hujan satu hari kalender UTC tanggal akuisisi (IMERG "
            "harian, sel 0.1 derajat ~11 km). Pemicu langsung banjir kilat. "
            "Piksel <0.1 mm dibuat transparan supaya area kering tidak "
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

# Tier yang dibaca tiap level, dan sufiks nama berkas Sentinel-1 yang ditulis
# tahap terakhir level itu (module3_lee_filter -> "_lee", module2_crop ->
# "_crop"). Satu-satunya tempat pemetaan level -> tier untuk PREVIEW; fusion
# menyatakan aturan yang sama lewat SourcePlan.tier_for_run().
_TIER_BY_LEVEL: dict[str, str] = {"PROCESSED": "cog", "RAW": "aligned"}
_S1_SUFFIX_BY_LEVEL: dict[str, str] = {"PROCESSED": "_lee", "RAW": "_crop"}


def tier_for_level(processing_level: str | None) -> str:
    """Tier on-disk yang dirender untuk level ini: PROCESSED -> gold,
    RAW -> bronze (DOCS/ETL.md, "Preview Stage")."""
    return _TIER_BY_LEVEL[fm.normalize_preview_level(processing_level)]


def _s1_source_path(
    dataset_id: int, dataset_name: str, s1_scene_key: str | None, band: str,
    processing_level: str,
) -> Path | None:
    """Cari raster Sentinel-1 satu band di tier yang sesuai level. Nama
    berkasnya tidak punya fungsi penamaan terpusat seperti MODIS/GPM (module4
    memakai `silver_path.name` apa adanya), jadi band-nya dicocokkan lewat
    sufiks yang ditulis tahap terakhir level itu: `_{BAND}_lee.tif` untuk
    PROCESSED, `_{BAND}_crop.tif` untuk RAW."""
    if not s1_scene_key:
        return None
    level = fm.normalize_preview_level(processing_level)
    scene_dir = fm.get_scene_dir(
        dataset_id, dataset_name, _TIER_BY_LEVEL[level], "sentinel1", s1_scene_key
    )
    if not scene_dir.is_dir():
        return None
    matches = sorted(
        scene_dir.glob(f"*_{band.upper()}{_S1_SUFFIX_BY_LEVEL[level]}.tif")
    )
    if not matches:
        # Cadangan longgar: instalasi lama bisa punya sufiks berbeda.
        matches = sorted(p for p in scene_dir.glob("*.tif") if f"_{band.upper()}" in p.name)
    return matches[0] if matches else None


def _aux_source_path(
    dataset_id: int, dataset_name: str, source: str, band: str, date_key: str,
    processing_level: str,
) -> Path | None:
    """Cari raster MODIS/GPM satu band untuk satu tanggal di tier level ini.

    Nama berkasnya identik di bronze/ dan gold/ (module7/module8 memakai
    `band_filename()` yang sama untuk semua target), jadi cuma folder induknya
    yang berbeda. Band turunan (NDVI/NDWI, akumulasi 72h/7d) memang tidak ada
    di bronze/ — jalur RAW tidak pernah menghitungnya — jadi pencariannya
    gagal wajar dan lapisan itu masuk daftar "dilewati"."""
    if source == "modis":
        filename = m7.band_filename(band, date_key)
    elif source == "gpm":
        # band RAIN_24H -> window "24h", bentuk yang dipakai module8.
        filename = m8.band_filename(band.removeprefix("RAIN_").lower(), date_key)
    else:  # pragma: no cover - dijaga PREVIEW_SPECS
        raise ValueError(f"source aux tidak dikenal: {source!r}")
    tier = tier_for_level(processing_level)
    path = fm.get_scene_dir(dataset_id, dataset_name, tier, source, date_key) / filename
    return path if path.exists() else None


def resolve_source_inputs(
    dataset_id: int,
    dataset_name: str,
    date_key: str,
    s1_scene_key: str | None = None,
    s1_files: dict[str, str] | None = None,
    processing_level: str = fm.DEFAULT_PREVIEW_LEVEL,
) -> dict[str, Path]:
    """
    Petakan `PreviewSpec.key` -> path raster yang ada di disk, di tier yang
    sesuai `processing_level`.

    `s1_files` ({band: path}) adalah keluaran tahap terakhir S1 yang dioper
    orchestrator (COG GOLD untuk PROCESSED, hasil crop BRONZE untuk RAW):
    dipakai lebih dulu karena itu jawaban pasti untuk scene yang baru saja
    diproses. Tanpa itu (pemakaian CLI / regenerasi), path dicari lewat glob
    di folder scene.

    Key yang berkasnya tidak ada sengaja tidak muncul di hasil, bukan
    dipetakan ke None — pemanggil melaporkannya sebagai "dilewati", dan
    dataset yang MODIS/GPM-nya gagal diunduh tetap dapat preview Sentinel-1.
    """
    level = fm.normalize_preview_level(processing_level)
    inputs: dict[str, Path] = {}
    for spec in PREVIEW_SPECS:
        if spec.source == "sentinel1":
            path: Path | None = None
            if s1_files and s1_files.get(spec.band):
                candidate = Path(s1_files[spec.band])
                path = candidate if candidate.exists() else None
            if path is None:
                path = _s1_source_path(
                    dataset_id, dataset_name, s1_scene_key, spec.band, level
                )
        else:
            path = _aux_source_path(
                dataset_id, dataset_name, spec.source, spec.band, date_key, level
            )
        if path is not None:
            inputs[spec.key] = path
    return inputs


def resolve_gold_inputs(
    dataset_id: int,
    dataset_name: str,
    date_key: str,
    s1_scene_key: str | None = None,
    s1_gold_files: dict[str, str] | None = None,
) -> dict[str, Path]:
    """Alias lama `resolve_source_inputs` untuk level PROCESSED (tier GOLD).

    Dipertahankan karena nama ini sudah dipakai di luar modul; kode baru
    sebaiknya memanggil resolve_source_inputs() dan menyatakan levelnya."""
    return resolve_source_inputs(
        dataset_id, dataset_name, date_key,
        s1_scene_key=s1_scene_key, s1_files=s1_gold_files,
        processing_level="PROCESSED",
    )


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


@dataclass(frozen=True)
class _PreviewGrid:
    """Grid PNG bersama (biasanya grid preview Sentinel-1)."""

    transform: Affine
    crs: object
    width: int
    height: int


def _preview_shape(width: int, height: int, max_side: int) -> tuple[int, int]:
    """(out_w, out_h) PNG untuk raster `width` x `height`.

    Sisi terpanjang melebihi `max_side` -> seluruh gambar diperkecil sampai
    sisi itu pas (rasio aspek dipertahankan). Kedua sisi masih di bawah ->
    diperbesar dengan faktor BULAT terbesar yang tidak melewati `max_side`,
    supaya tiap piksel sumber jadi blok persegi utuh (MODIS 21 px -> 1008 px),
    bukan PNG belasan piksel yang di UI tampil sebagai titik atau di-blur
    browser.

    Yang dibatasi sisi terpanjang, bukan lebar: lihat MAX_SIDE."""
    longest = max(width, height)
    if longest > max_side:
        scale = max_side / longest
        return max(1, int(round(width * scale))), max(1, int(round(height * scale)))
    factor = max(1, max_side // longest)
    return width * factor, height * factor


def _preview_grid(path: Path, max_side: int = MAX_SIDE) -> _PreviewGrid:
    """Grid PNG untuk raster `path` — dipakai sebagai grid bersama semua
    lapisan satu tanggal kalau `path` adalah Sentinel-1."""
    with rasterio.open(path) as src:
        out_w, out_h = _preview_shape(src.width, src.height, max_side)
        transform = src.transform * Affine.scale(src.width / out_w, src.height / out_h)
        return _PreviewGrid(transform, src.crs, out_w, out_h)


def _read_downsampled(
    path: Path,
    max_side: int = MAX_SIDE,
    log_db: bool = False,
    grid: _PreviewGrid | None = None,
    categorical: bool = False,
) -> _Layer:
    """Baca band 1 ke ukuran preview.

    Tanpa `grid`: ukuran dari _preview_shape(). Downsample dilakukan rasterio
    (bukan setelah membaca penuh) supaya raster S1 belasan ribu piksel tidak
    pernah masuk memori utuh: COG punya overview internal, jadi pembacaan ini
    membaca level piramida yang sudah ada. Upsample selalu nearest.

    Dengan `grid`: raster direproject ke grid itu (extent & ukuran identik
    dengan preview S1). Nearest kalau sumbernya lebih kasar dari grid atau
    datanya kategorikal; average kalau sumbernya lebih halus.

    `log_db=True` mengubah sigma0 linear jadi dB setelah masking — lihat
    _maybe_to_db(). Rata-rata downsample sengaja dilakukan di ranah linear
    (rata-rata daya), baru dikonversi: merata-ratakan nilai dB adalah
    rata-rata geometrik daya, yang bukan yang dimaksud di sini.
    """
    with rasterio.open(path) as src:
        nodata = src.nodata
        src_h, src_w = src.height, src.width
        if grid is not None:
            out_w, out_h = grid.width, grid.height
            coarser = abs(src.transform.a) >= abs(grid.transform.a)
            resampling = (
                Resampling.nearest if categorical or coarser else Resampling.average
            )
            data = np.full((out_h, out_w), np.nan, dtype=np.float32)
            reproject(
                source=rasterio.band(src, 1),
                destination=data,
                src_transform=src.transform,
                src_crs=src.crs,
                src_nodata=nodata,
                dst_transform=grid.transform,
                dst_crs=grid.crs,
                dst_nodata=np.nan,
                resampling=resampling,
            )
        else:
            out_w, out_h = _preview_shape(src.width, src.height, max_side)
            upsample = out_w >= src.width
            resampling = (
                Resampling.nearest if categorical or upsample else Resampling.average
            )
            data = src.read(1, out_shape=(out_h, out_w), resampling=resampling).astype(
                np.float32
            )

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

    if spec is not None and spec.scale in ("fixed", "categorical"):
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


def _gray_range(layer: _Layer, spec: PreviewSpec | None) -> tuple[float, float]:
    """Rentang grayscale: persentil per berkas, kecuali data kategorikal —
    persentil kelas diskret (mis. 98% piksel bernilai 1) menjepit kelas
    lain ke putih/hitam, jadi kelas dipetakan ke vmin..vmax tetap."""
    if spec is not None and spec.categorical:
        return _stretch_range(layer, spec)
    return _stretch_range(layer, None)


def _render_grayscale(layer: _Layer, out_path: Path, spec: PreviewSpec | None = None) -> Path:
    """PNG mode LA: satu kanal luminansi (nilai ilmiah) + satu kanal alpha
    (masker NoData). Tetap 'satu kanal data' seperti yang dimaksud tier ini —
    alpha bukan data, dia cuma memisahkan 'gelap' dari 'tidak ada'."""
    from PIL import Image

    lo, hi = _gray_range(layer, spec)
    gray = (_normalize(layer, lo, hi) * 255).astype(np.uint8)
    img = Image.fromarray(np.dstack([gray, _alpha(layer)]), mode="LA")
    return _save_png(img, out_path)


def _colored_rgba(layer: _Layer, spec: PreviewSpec) -> np.ndarray:
    """Array RGBA uint8 untuk render colored."""
    from matplotlib import colormaps
    from matplotlib.colors import to_rgb

    if spec.categorical:
        rgba = np.zeros((layer.height, layer.width, 4), dtype=np.uint8)
        values = np.where(layer.mask, -1, np.rint(layer.data)).astype(np.int32)
        for value, color, alpha, _label in spec.categories:
            hit = values == value
            rgba[hit, :3] = [int(round(c * 255)) for c in to_rgb(color)]
            rgba[hit, 3] = alpha
        return rgba

    lo, hi = _stretch_range(layer, spec)
    rgba = (colormaps[spec.cmap](_normalize(layer, lo, hi)) * 255).astype(np.uint8)
    rgba[..., 3] = _alpha(layer, spec)
    return rgba


def _render_colored(layer: _Layer, spec: PreviewSpec, out_path: Path) -> Path:
    """PNG RGBA hasil colormap matplotlib (atau palet kelas)."""
    from PIL import Image

    img = Image.fromarray(_colored_rgba(layer, spec), mode="RGBA")
    return _save_png(img, out_path)


def _class_percentages(layer: _Layer, spec: PreviewSpec) -> dict[str, float]:
    """Persentase piksel preview per kelas (dari seluruh piksel, termasuk
    NoData) — ringkasan yang lebih berguna daripada min/max/mean kelas."""
    total = layer.data.size or 1
    values = np.where(layer.mask, -1, np.rint(layer.data)).astype(np.int32)
    out = {
        label: round(float((values == value).sum()) / total * 100, 2)
        for value, _color, _alpha, label in spec.categories
    }
    out["Tidak ada data"] = round(float(layer.mask.sum()) / total * 100, 2)
    return out


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


def overlay_filename(spec_key: str) -> str:
    return f"{spec_key}_on_s1.png"


def _render_overlay_on_s1(
    base: _Layer, layer: _Layer, spec: PreviewSpec, out_path: Path
) -> Path | None:
    """Lapisan MODIS/GPM (berwarna, transparan di NoData) ditumpuk di atas
    citra Sentinel-1 grayscale sebagai peta dasar. Kedua array sudah berada di
    grid yang sama (lihat "GRID BERSAMA"), jadi tinggal alpha-compositing."""
    from PIL import Image

    if base.data.shape != layer.data.shape:
        logger.warning(
            "[M10] overlay %s dilewati: dimensi dasar %s != lapisan %s",
            spec.key, base.data.shape, layer.data.shape,
        )
        return None

    lo, hi = _stretch_range(base, None)
    gray = (_normalize(base, lo, hi) * 255).astype(np.float32)
    # Piksel NoData S1 (di luar swath) dijadikan putih, bukan hitam: hitam
    # adalah nilai sah untuk air tenang.
    gray = np.where(base.mask, 255.0, gray)

    fg = _colored_rgba(layer, spec).astype(np.float32)
    alpha = fg[..., 3:4] / 255.0
    if not spec.categorical:
        # Lapisan kontinu opak di seluruh AOI (mis. hujan 7 hari > 0.1 mm di
        # mana-mana) akan menutup peta dasar sepenuhnya.
        alpha = alpha * OVERLAY_OPACITY
    rgb = fg[..., :3] * alpha + gray[..., None] * (1.0 - alpha)
    img = Image.fromarray(np.clip(rgb, 0, 255).astype(np.uint8), mode="RGB")
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


# Nama opsi di datasets.preview_options -> nama folder render.
_OPTION_TO_KIND: dict[str, str] = {
    "GRAYSCALE": "grayscale",
    "COLORED": "colored",
    "COMPOSITE": "composite",
}


def _normalize_options(options) -> set[str]:
    """Ubah datasets.preview_options jadi himpunan nama folder render.

    None berarti "tidak dinyatakan" -> ketiganya, yaitu perilaku modul ini
    sebelum opsi per-varian ada. Himpunan KOSONG yang dinyatakan eksplisit
    dihormati apa adanya (dataset yang memang tidak mau PNG apa pun);
    membedakan keduanya itulah alasan default-nya None dan bukan tuple penuh.
    Opsi tak dikenal dibuang dengan warning — preview adalah artefak turunan,
    menjatuhkan scene karena satu string asing di kolom array jauh lebih mahal
    daripada merender lebih sedikit varian.
    """
    if options is None:
        return set(_OPTION_TO_KIND.values())
    out: set[str] = set()
    for raw in options:
        kind = _OPTION_TO_KIND.get(str(raw).strip().upper())
        if kind is None:
            logger.warning("[M10] opsi preview tidak dikenal diabaikan: %r", raw)
            continue
        out.add(kind)
    return out


def _composite_info(entries: list[dict]) -> dict:
    """Sidecar composite/. Skemanya sengaja beda dari colored_info: komposit
    tidak punya colormap maupun rentang nilai tunggal — yang perlu dijelaskan
    adalah pemetaan kanal ke besaran."""
    return {
        "kind": "composite",
        "count": len(entries),
        "description": (
            "Komposit false-color RGB dari beberapa band sekaligus. Warna di "
            "sini menyatakan hubungan antar-band, bukan nilai satu besaran, "
            "jadi tidak ada colorbar yang bisa dipasang padanya. Berkas "
            "*_on_s1.png adalah pengecualian: lapisan MODIS/GPM berwarna "
            "(lihat 'legend'/colored/) ditumpuk di atas citra Sentinel-1 "
            "grayscale sebagai peta dasar."
        ),
        "not_for": "Analisis kuantitatif — pakai gold/*.tif atau fusion/*.h5.",
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
                "FLOOD: palet kelas (abu-abu = tanpa air, biru = air "
                "permanen, oranye = banjir musiman, merah = banjir tidak biasa, "
                "transparan = data tidak cukup); lihat 'legend' tiap entri. "
                "NDVI RdYlGn dipatok -0.2..0.8 (merah = air/lahan terbangun, "
                "hijau = vegetasi); NDWI BrBG dipatok -0.5..0.5 dengan nol di "
                "tengah (cokelat = kering, biru-hijau = air). Awan transparan."
            ),
            "gpm": (
                "YlGnBu sequential mulai dari nol, batas atas persentil 98 per "
                "berkas. Nol adalah nol sungguhan (bukan minimum data), dan "
                "piksel di bawah 0.1 mm dibuat transparan supaya area kering "
                "tidak terbaca sebagai hujan ringan."
            ),
        },
        "nodata": "Transparan (alpha=0), sama seperti grayscale/.",
        "grid": (
            "Kalau Sentinel-1 ada, MODIS/GPM direproject ke grid preview S1 "
            "(nearest-neighbour; lihat 'aligned_to') sehingga semua PNG satu "
            "tanggal berukuran sama dan bisa ditumpuk. Piksel MODIS 250/500 m "
            "dan sel GPM 0.1 derajat tampil sebagai blok."
        ),
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


def _previous_s1_scene_key(metadata_path: Path, date_key: str) -> str | None:
    """s1_scene_key dari sidecar render sebelumnya, HANYA kalau sidecar itu
    menggambarkan tanggal yang sama.

    Syarat tanggal itu bukan kehati-hatian berlebih: satu folder preview
    dipakai bersama seluruh tanggal dataset (lihat `fm.get_preview_dir` —
    `scene_key` tidak ikut ke path), jadi sidecar yang ada di sana hampir
    selalu milik tanggal LAIN. Tanpa syarat ini setiap scene berikutnya akan
    dituduh menimpa scene sebelumnya.

    None kalau sidecar belum ada, tanggalnya beda, atau isinya tidak terbaca —
    sidecar rusak bukan alasan menggagalkan render baru."""
    try:
        with open(metadata_path, encoding="utf-8") as f:
            payload = json.load(f)
        if payload.get("acquisition_date") != date_key:
            return None
        value = payload.get("s1_scene_key")
    except (OSError, ValueError, AttributeError):
        return None
    return value if isinstance(value, str) and value else None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate_previews(
    dataset_id: int,
    dataset_name: str,
    acquisition_date: date_type | datetime | str,
    s1_scene_key: str | None = None,
    s1_gold_files: dict[str, str] | None = None,
    max_side: int = MAX_SIDE,
    overwrite: bool = True,
    processing_level: str = fm.DEFAULT_PREVIEW_LEVEL,
    s1_files: dict[str, str] | None = None,
    options: tuple[str, ...] | list[str] | None = None,
    max_width: int | None = None,
) -> dict:
    """
    Render seluruh preview untuk satu tanggal akuisisi pada SATU level
    pemrosesan.

    Args:
        acquisition_date: tanggal scene S1; dinormalisasi ke kunci YYYYMMDD.
        s1_scene_key:     product_identifier scene S1 (untuk glob folder scene).
        s1_files:         {band: path} keluaran tahap terakhir S1 pada level
                          ini (COG GOLD untuk PROCESSED, hasil crop BRONZE
                          untuk RAW), kalau pemanggil sudah punya.
        s1_gold_files:    nama lama `s1_files`, masih diterima.
        max_side:         batas sisi TERPANJANG PNG (lihat MAX_SIDE).
        max_width:        nama lama `max_side`, masih diterima. Artinya ikut
                          berubah: sekarang membatasi sisi terpanjang, bukan
                          lebar — itu memang perbaikannya.
        processing_level: RAW atau PROCESSED. Menentukan tier yang dibaca
                          (bronze/ vs gold/) DAN folder output
                          preview/{tanggal}/{LEVEL}/.
        options:          varian yang dirender, dari datasets.preview_options
                          ("GRAYSCALE", "COLORED", "COMPOSITE"). None = ketiganya.
        overwrite:        True (default) me-render ulang semua PNG. Preview
                          adalah turunan murni dan murah, jadi menulis ulang
                          lebih aman daripada menyimpan PNG basi dari raster
                          versi lama. False melewati berkas yang sudah ada —
                          untuk mengisi ulang preview yang hilang saja.

    Returns:
        Ringkasan yang sama isinya dengan preview_metadata.json, ditambah
        daftar path absolut di key "files".

    Tidak pernah melempar karena satu band gagal: band yang berkas sumbernya
    tidak ada atau rusak masuk ke daftar "skipped" beserta alasannya. Preview
    adalah artefak turunan — kegagalan render tidak boleh menjatuhkan scene
    yang datanya sendiri baik-baik saja.
    """
    if max_width is not None:
        max_side = max_width
    level = fm.normalize_preview_level(processing_level)
    tier = tier_for_level(level)
    wanted = _normalize_options(options)
    date_key = fm.date_key(acquisition_date)
    preview_dir = fm.ensure_preview_dir(dataset_id, dataset_name, date_key)
    level_dir = fm.get_preview_level_dir(dataset_id, dataset_name, date_key, level)

    # Dibaca SEBELUM render, karena render menimpa sidecar yang sama. Lihat
    # "SATU TANGGAL, SATU SET PNG" di docstring modul.
    replaced_scene_key = _previous_s1_scene_key(
        level_dir / fm.dated_filename(date_key, "preview_metadata.json"), date_key
    )
    if replaced_scene_key == s1_scene_key or not s1_scene_key:
        replaced_scene_key = None
    if replaced_scene_key:
        logger.warning(
            "[M10] PREVIEW %s level=%s: scene %s menimpa preview scene %s "
            "(nama berkas PNG hanya berprefiks tanggal, bukan scene)",
            date_key, level, s1_scene_key, replaced_scene_key,
        )

    # Folder tiap varian dibuat hanya kalau varian itu diminta: folder kosong
    # akan membuat API melaporkan varian yang sebenarnya tidak pernah dirender.
    def _kind_dir(kind: str) -> Path | None:
        if kind not in wanted:
            return None
        return fm.ensure_preview_kind_dir(
            dataset_id, dataset_name, date_key, kind, level
        )

    gray_dir = _kind_dir("grayscale")
    color_dir = _kind_dir("colored")
    composite_dir = _kind_dir("composite")

    inputs = resolve_source_inputs(
        dataset_id, dataset_name, date_key,
        s1_scene_key=s1_scene_key,
        s1_files=s1_files or s1_gold_files,
        processing_level=level,
    )

    gray_entries: list[dict] = []
    color_entries: list[dict] = []
    written: list[Path] = []
    skipped: list[dict] = []
    layers: dict[str, _Layer] = {}

    # Grid bersama dari Sentinel-1 (lihat "GRID BERSAMA" di docstring modul).
    grid: _PreviewGrid | None = None
    grid_source: str | None = None
    for s1_key in ("s1_vv", "s1_vh"):
        if s1_key in inputs:
            try:
                grid = _preview_grid(inputs[s1_key], max_side=max_side)
                grid_source = s1_key
            except Exception:
                logger.exception("[M10] gagal baca grid %s, lapisan aux tanpa grid bersama", s1_key)
            break

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

        # Prefiks tanggal wajib sejak relayout: folder preview tidak lagi
        # bersarang di bawah folder tanggal, jadi tanpa ini render tanggal
        # kedua akan menimpa tanggal pertama dengan nama yang sama.
        png_name = fm.dated_filename(date_key, f"{spec.key}.png")
        gray_path = gray_dir / png_name if gray_dir else None
        color_path = color_dir / png_name if color_dir else None
        wanted_paths = [p for p in (gray_path, color_path) if p is not None]
        if not overwrite and wanted_paths and all(p.exists() for p in wanted_paths):
            skipped.append({
                "key": spec.key, "source": spec.source, "band": spec.band,
                "reason": "sudah ada (overwrite=False)",
            })
            continue

        try:
            layer = _read_downsampled(
                src_path, max_side=max_side, log_db=spec.log_db,
                grid=grid if spec.source != "sentinel1" else None,
                categorical=spec.categorical,
            )
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
            "aligned_to": grid_source if spec.source != "sentinel1" and grid is not None else None,
        }
        if spec.categorical:
            common["statistics"] = {**layer.stats, "class_percent": _class_percentages(layer, spec)}

        try:
            if gray_path is not None:
                _render_grayscale(layer, gray_path, spec)
                g_lo, g_hi = _gray_range(layer, spec)
                gray_entries.append({
                    **common,
                    "file": gray_path.name,
                    "colormap": "gray",
                    "value_range": [round(g_lo, 4), round(g_hi, 4)],
                    "range_method": (
                        "kelas tetap" if spec.categorical
                        else f"persentil {PCT_LOW}-{PCT_HIGH}"
                    ),
                    "size_bytes": gray_path.stat().st_size,
                })
                written.append(gray_path)

            if color_path is not None:
                _render_colored(layer, spec, color_path)
                c_lo, c_hi = _stretch_range(layer, spec)
                entry = {
                    **common,
                    "file": color_path.name,
                    "colormap": spec.cmap,
                    "value_range": [round(c_lo, 4), round(c_hi, 4)],
                    "range_method": spec.scale,
                    "transparent_below": spec.transparent_below,
                    "interpretation": spec.interpretation,
                    "size_bytes": color_path.stat().st_size,
                }
                if spec.categorical:
                    entry["legend"] = [
                        {"value": v, "color": c, "alpha": a, "label": lbl}
                        for v, c, a, lbl in spec.categories
                    ]
                # PNG yang seluruhnya transparan tampak "rusak" di UI padahal
                # datanya sah (mis. hujan 0 mm di seluruh AOI) — tandai.
                if not _colored_rgba(layer, spec)[..., 3].any():
                    entry["all_transparent"] = True
                    entry["note"] = (
                        f"Seluruh piksel di bawah {spec.transparent_below} {spec.units} "
                        "(tidak ada hujan)" if spec.transparent_below is not None
                        else "Tidak ada kelas yang bisa ditampilkan"
                    )
                color_entries.append(entry)
                written.append(color_path)
        except Exception as exc:
            logger.exception("[M10] gagal render %s", spec.key)
            skipped.append({
                "key": spec.key, "source": spec.source, "band": spec.band,
                "reason": f"gagal render: {exc}",
            })

    # Komposit RGB hanya mungkin kalau VV dan VH dua-duanya berhasil dibaca.
    # Berlaku di kedua level: BRONZE S1 sudah terkalibrasi, jadi selisih
    # VV-VH-nya tetap punya arti fisik yang sama, cuma belum di-despeckle.
    composite_entries: list[dict] = []
    if composite_dir is not None and "s1_vv" in layers and "s1_vh" in layers:
        rgb_path = composite_dir / fm.dated_filename(date_key, f"{S1_RGB_KEY}.png")
        try:
            if _render_s1_rgb(layers["s1_vv"], layers["s1_vh"], rgb_path) is not None:
                composite_entries.append({
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

    # Overlay MODIS/GPM di atas Sentinel-1: hanya bermakna kalau ada S1 sebagai
    # peta dasar (dan otomatis begitu, karena lapisan aux baru punya grid yang
    # sama dengan S1 saat S1 ada).
    if composite_dir is not None and grid is not None and grid_source in layers:
        base_layer = layers[grid_source]
        for spec in PREVIEW_SPECS:
            layer = layers.get(spec.key)
            if spec.source == "sentinel1" or layer is None:
                continue
            out_path = composite_dir / fm.dated_filename(date_key, overlay_filename(spec.key))
            try:
                if _render_overlay_on_s1(base_layer, layer, spec, out_path) is None:
                    continue
                entry = {
                    "key": f"{spec.key}_on_s1",
                    "source": spec.source,
                    "band": spec.band,
                    "label": f"{spec.label} di atas Sentinel-1",
                    "units": spec.units,
                    "file": out_path.name,
                    "colormap": f"{spec.cmap} di atas grayscale {grid_source}",
                    "range_method": spec.scale,
                    "basemap": grid_source,
                    "width": layer.width,
                    "height": layer.height,
                    "interpretation": (
                        f"{spec.interpretation} Latar abu-abu adalah citra "
                        f"{grid_source} (garis pantai, daratan, laut) supaya "
                        "posisi piksel terbaca secara geografis."
                    ),
                    "size_bytes": out_path.stat().st_size,
                }
                if spec.categorical:
                    entry["legend"] = [
                        {"value": v, "color": c, "alpha": a, "label": lbl}
                        for v, c, a, lbl in spec.categories
                    ]
                composite_entries.append(entry)
                written.append(out_path)
            except Exception as exc:
                logger.exception("[M10] gagal render overlay %s", spec.key)
                skipped.append({
                    "key": f"{spec.key}_on_s1", "source": spec.source,
                    "band": spec.band, "reason": f"gagal render: {exc}",
                })

    # PNG render lama untuk lapisan yang kali ini dilewati (mis. NDVI yang
    # sekarang seluruhnya awan) harus hilang: API mendaftar PNG lewat glob
    # folder, jadi berkas basi akan tetap tampil seolah hasil render terbaru.
    if overwrite:
        # Dibatasi ke tanggal yang sedang dirender: satu folder kind sekarang
        # memuat PNG SEMUA tanggal (tidak ada lagi folder tanggal), jadi
        # mencocokkan nama tanpa prefiks akan menghapus render tanggal lain
        # yang justru masih sahih.
        known = {
            fm.dated_filename(date_key, f"{spec.key}.png") for spec in PREVIEW_SPECS
        } | {fm.dated_filename(date_key, f"{S1_RGB_KEY}.png")} | {
            fm.dated_filename(date_key, overlay_filename(spec.key))
            for spec in PREVIEW_SPECS if spec.source != "sentinel1"
        }
        for kind_dir, entries in (
            (gray_dir, gray_entries), (color_dir, color_entries),
            (composite_dir, composite_entries),
        ):
            if kind_dir is None:
                continue
            current = {e["file"] for e in entries}
            for stale in kind_dir.glob("*.png"):
                if stale.name in known and stale.name not in current:
                    stale.unlink()
                    logger.info("[M10] hapus preview basi %s", stale)

    kinds: dict[str, dict] = {}
    for kind, kind_dir, entries, info_fn in (
        ("grayscale", gray_dir, gray_entries, _grayscale_info),
        ("colored", color_dir, color_entries, _colored_info),
        ("composite", composite_dir, composite_entries, _composite_info),
    ):
        if kind_dir is None:
            continue
        # Berprefiks tanggal, persis seperti PNG-nya. Satu folder kind memuat
        # PNG SEMUA tanggal dataset (tidak ada lagi folder tanggal), jadi
        # sidecar tanpa prefiks akan ditulis ulang penuh oleh tiap scene dan
        # yang tersisa cuma milik scene yang selesai terakhir -- galeri lalu
        # menampilkan gambar tanggal itu untuk SEMUA tanggal.
        info_name = fm.dated_filename(date_key, f"{kind}_info.json")
        written.append(_write_json(kind_dir / info_name, info_fn(entries)))
        kinds[kind] = {
            "dir": kind,
            "info": info_name,
            "files": [e["file"] for e in entries],
        }

    sources_present = sorted({e["source"] for e in gray_entries + color_entries})
    metadata = {
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "acquisition_date": date_key,
        "s1_scene_key": s1_scene_key,
        # Scene lain di tanggal yang sama yang preview-nya baru saja ditimpa;
        # None kalau tidak ada penimpaan lintas scene.
        "replaced_s1_scene_key": replaced_scene_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator": MODULE,
        "tier": "PREVIEW",
        "processing_level": level,
        # Tier asalnya, bukan konstanta "COG": preview level RAW dirender
        # dari tier ALIGNED, dan menuliskan "COG" di situ akan membuat sidecar
        # berbohong soal provenance-nya.
        "derived_from": tier.upper(),
        "options": sorted(wanted),
        "max_side_px": max_side,
        "grid": (
            {"aligned_to": grid_source, "width": grid.width, "height": grid.height}
            if grid is not None else None
        ),
        "png_compress_level": PNG_COMPRESS_LEVEL,
        "sources_present": sources_present,
        "counts": {
            "grayscale": len(gray_entries),
            "colored": len(color_entries),
            "composite": len(composite_entries),
            "total_png": len(gray_entries) + len(color_entries) + len(composite_entries),
            "skipped": len(skipped),
        },
        "kinds": kinds,
        "skipped": skipped,
        "usage": {
            "grayscale": "Pembacaan ilmiah satu berkas; kontras dioptimalkan per berkas.",
            "colored": "Publikasi dan perbandingan lintas tanggal; skala terpatok.",
            "not_for": "Analisis kuantitatif — pakai gold/*.tif atau fusion/*.h5.",
        },
    }
    # Sidecar per level DAN per tanggal. Level karena dua level menulis ke
    # folder yang sama; tanggal karena folder preview dipakai bersama seluruh
    # tanggal dataset (lihat komentar info_name di atas).
    metadata_name = fm.dated_filename(date_key, "preview_metadata.json")
    level_dir.mkdir(parents=True, exist_ok=True)
    written.append(_write_json(level_dir / metadata_name, metadata))
    # Salinan di akar preview/ untuk pembaca yang belum menyebut level.
    written.append(_write_json(preview_dir / metadata_name, metadata))

    total_mb = sum(p.stat().st_size for p in written if p.exists()) / (1024 ** 2)
    logger.info(
        "[M10] PREVIEW %s level=%s dari %s: %d grayscale + %d colored + %d komposit "
        "PNG (%d dilewati, %.2f MB)",
        date_key, level, tier, len(gray_entries), len(color_entries),
        len(composite_entries), len(skipped), total_mb,
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
