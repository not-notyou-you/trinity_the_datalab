# etl/land_mask.py
"""
Layer jarak ke garis pantai — informasi tambahan untuk konsumen dataset,
BUKAN penyaring yang mengubah data.

Air memantulkan gelombang radar menjauh dari sensor (specular), jadi di SAR
laut tampak gelap persis seperti genangan banjir. Model apa pun yang dilatih
tanpa tahu mana laut akan belajar "kenali laut" — mudah, konsisten, dan
menghasilkan metrik tinggi yang palsu. Di AOI Jabodetabek 16,78% piksel adalah
laut; di AOI JAWA 60,34%.

Yang TIDAK dilakukan modul ini, dan itu disengaja:

    - tidak menyentuh raw maupun fusion; tidak ada piksel yang dibuang
    - tidak memutuskan buffer, ambang, atau apa pun yang termasuk pemodelan
    - tidak membedakan sungai/danau dari daratan — luapan sungai justru
      sinyal yang dicari, jadi air tawar sengaja dibiarkan "darat"

Modul ini menjawab satu pertanyaan faktual saja: piksel ini berjarak berapa
meter dari garis pantai? Keputusan memakainya ada di hilir.

Nilainya JARAK BERTANDA, bukan biner, dan itu pilihan sadar:

    positif  di darat       negatif  di laut       nol  di garis pantai

Dari satu layer itu konsumen menurunkan sembarang aturan sendiri tanpa perlu
regenerasi apa pun — `> 0` untuk garis pantai ketat, `> -500` untuk melonggarkan
muara (rob dan luapan sungai bertemu di sana), `> 2000` untuk menjauhi pesisir —
atau memakainya langsung sebagai fitur kontinu. Mask biner mengunci buffer pada
angka yang dipilih produsen, dan tiap eksperimen baru berarti minta file baru.

Sumbernya OSM land polygons di tabel PostGIS `reference_land_polygons`
(lihat database/migrations/023_reference_land_polygons.sql).
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
from rasterio.transform import Affine

from etl.atomic_write import atomic_path

logger = logging.getLogger(__name__)

MODULE = "LAND_MASK"

LAND_TABLE = "reference_land_polygons"
MASKS_DIRNAME = "masks"
LAND_DISTANCE_STEM = "land_distance"

# Jarak dipotong di +/-32 km supaya muat di int16 (batas 32767) tanpa naik ke
# int32 yang menggandakan ukuran file. Tidak ada informasi yang hilang: tidak
# ada buffer pantai realistis sebesar 32 km, jadi "32 km" dan "120 km" sama-sama
# berarti "jauh dari laut". AOI pedalaman penuh berakhir seluruhnya +32000.
CLAMP_M = 32_000
NODATA = -32768

# Derajat -> meter. Lintang memakai panjang derajat meridian, bujur menyusut
# dengan cos(lintang). Dipakai sebagai `sampling` EDT supaya jaraknya meter,
# bukan piksel, dan supaya piksel yang tidak persegi di EPSG:4326 tidak bias.
M_PER_DEG_LAT = 110_574.0
M_PER_DEG_LON_EQ = 111_320.0

# Anggaran piksel untuk transformasi jarak. EDT mengembalikan float64, jadi satu
# grid 3,3 miliar piksel (AOI JAWA) butuh ~26 GB. Di atas anggaran ini jarak
# dihitung pada grid yang didesimasi lalu dinaikkan lagi; medan jaraknya mulus
# sehingga galatnya seorde ukuran sel desimasi, dan itu diakui di metadata.
EDT_PIXEL_BUDGET = 200_000_000


def get_masks_dir(dataset_root: Path) -> Path:
    """Folder masks/ milik satu dataset. Sejajar fusion/, bukan di dalamnya:
    mask adalah properti AOI + grid, berlaku untuk SEMUA tanggal, sementara
    isi fusion/ adalah per tanggal."""
    return Path(dataset_root) / MASKS_DIRNAME


def grid_from_fusion_h5(h5_path: Path) -> tuple[tuple[float, ...], Affine, tuple[int, int]]:
    """Baca grid referensi dari satu stack fusion.

    Grid diambil dari berkas nyata, bukan dihitung ulang dari bbox dan
    S1_RESOLUTION_DEG. Resolusi fusion mengikuti raster S1 dataset itu
    (module9._dataset_fusion_grid), bukan konstanta: AOI Jabodetabek menghasilkan
    8801x8801 pada 9,0904e-05 derajat, sedangkan rumus konstanta memberi 8906.
    Menghitung ulang berarti mask meleset satu setengah ratus piksel dari stack
    yang seharusnya ia dampingi.
    """
    import h5py

    with h5py.File(h5_path, "r") as h:
        bbox = tuple(float(v) for v in h.attrs["aoi_bbox"])
        t = [float(v) for v in h.attrs["transform"]]
        shape = (int(h.attrs["height"]), int(h.attrs["width"]))
    return bbox, Affine(t[0], t[1], t[2], t[3], t[4], t[5]), shape


def fetch_land_geometries(bbox: tuple[float, float, float, float], database_url: str):
    """Poligon daratan yang memotong bbox, sudah dipotong ke bbox itu.

    Pemotongan dilakukan di sisi PostGIS (ST_Intersection) supaya yang berpindah
    ke Python hanya bagian yang benar-benar jatuh di AOI — poligon daratan Jawa
    utuh berukuran jauh lebih besar daripada AOI mana pun yang memotongnya.
    """
    from shapely import wkb
    from sqlalchemy import create_engine, text

    min_lon, min_lat, max_lon, max_lat = bbox
    sql = text(
        f"""
        SELECT ST_AsBinary(ST_Intersection(
                 geom, ST_MakeEnvelope(:a, :b, :c, :d, 4326))) AS g
        FROM {LAND_TABLE}
        WHERE geom && ST_MakeEnvelope(:a, :b, :c, :d, 4326)
        """
    )
    params = {"a": min_lon, "b": min_lat, "c": max_lon, "d": max_lat}

    geoms = []
    engine = create_engine(database_url)
    try:
        with engine.connect() as con:
            for (raw,) in con.execute(sql, params):
                if raw is None:
                    continue
                geom = wkb.loads(bytes(raw))
                if not geom.is_empty:
                    geoms.append(geom)
    finally:
        engine.dispose()
    return geoms


def rasterize_land(geoms, transform: Affine, shape: tuple[int, int]) -> np.ndarray:
    """Poligon -> bool, True di darat.

    all_touched=False dipakai sengaja: piksel dihitung darat kalau PUSATNYA di
    dalam poligon. Dengan all_touched=True setiap piksel yang sekadar
    tersenggol garis pantai jadi darat, dan garis pantainya melar setengah
    piksel ke laut di seluruh AOI.
    """
    from rasterio.features import rasterize

    if not geoms:
        return np.zeros(shape, dtype=bool)
    out = rasterize(
        ((g, 1) for g in geoms),
        out_shape=shape,
        transform=transform,
        fill=0,
        dtype=np.uint8,
        all_touched=False,
    )
    return out.astype(bool)


def _sampling_m(transform: Affine, bbox: tuple[float, float, float, float]) -> tuple[float, float]:
    """(meter per baris, meter per kolom) di lintang tengah AOI.

    Satu faktor cos untuk seluruh AOI, bukan per baris: pada rentang lintang
    AOI terbesar yang dipakai (JAWA, 2,9 derajat di sekitar 7 LS) selisih
    cos antar tepi di bawah 1%, jauh di bawah ketelitian yang berguna untuk
    keputusan buffer ratusan meter.
    """
    mean_lat = (bbox[1] + bbox[3]) / 2.0
    res_y = abs(float(transform.e))
    res_x = abs(float(transform.a))
    m_row = res_y * M_PER_DEG_LAT
    m_col = res_x * M_PER_DEG_LON_EQ * float(np.cos(np.radians(mean_lat)))
    return m_row, m_col


def _signed_distance(land: np.ndarray, sampling: tuple[float, float]) -> np.ndarray:
    """Jarak bertanda ke garis pantai, dalam meter, float32.

    distance_transform_edt mengukur jarak ke nol terdekat, jadi dipanggil dua
    kali dengan masukan saling berkebalikan: sekali dari darat mencari laut,
    sekali dari laut mencari darat. Hasilnya dijahit dengan tanda berlawanan
    sehingga nol jatuh tepat di garis pantai.
    """
    from scipy.ndimage import distance_transform_edt

    # Dihitung bergantian dan langsung diturunkan ke float32: EDT mengembalikan
    # float64, dan menahan dua array float64 seukuran AOI sekaligus melipatduakan
    # puncak memori tanpa menambah ketelitian yang berarti.
    d_land = distance_transform_edt(land, sampling=sampling).astype(np.float32)
    d_sea = distance_transform_edt(~land, sampling=sampling).astype(np.float32)
    signed = np.where(land, d_land, -d_sea).astype(np.float32)
    del d_land, d_sea
    return signed


def _decimation_for(shape: tuple[int, int]) -> int:
    """Faktor desimasi supaya EDT muat di anggaran memori. 1 = resolusi penuh."""
    total = shape[0] * shape[1]
    if total <= EDT_PIXEL_BUDGET:
        return 1
    return int(np.ceil(np.sqrt(total / EDT_PIXEL_BUDGET)))


def build_land_distance(
    land: np.ndarray,
    transform: Affine,
    bbox: tuple[float, float, float, float],
    clamp_m: int = CLAMP_M,
) -> tuple[np.ndarray, dict]:
    """Mask darat -> jarak bertanda int16, plus catatan cara ia dihitung.

    Catatannya ikut ke metadata berkas: kalau jaraknya dihitung pada grid
    desimasi, konsumen harus tahu bahwa nol-nya berketelitian seorde sel
    desimasi, bukan seorde piksel.
    """
    m_row, m_col = _sampling_m(transform, bbox)
    step = _decimation_for(land.shape)

    if step == 1:
        signed = _signed_distance(land, (m_row, m_col))
        note = {"decimation": 1, "distance_accuracy_m": round(max(m_row, m_col), 2)}
    else:
        # Medan jarak mulus dan bergradasi, jadi menghitungnya kasar lalu
        # menaikkannya kembali jauh lebih murah daripada EDT resolusi penuh,
        # dengan galat yang terbatas di sekitar garis pantai saja.
        small = land[::step, ::step]
        signed_small = _signed_distance(small, (m_row * step, m_col * step))
        signed = _upsample(signed_small, land.shape)
        note = {
            "decimation": step,
            "distance_accuracy_m": round(max(m_row, m_col) * step, 2),
        }
        del small, signed_small

    np.clip(signed, -clamp_m, clamp_m, out=signed)
    out = signed.astype(np.int16)
    del signed

    total = int(out.size)
    n_land = int(np.count_nonzero(land))
    note.update(
        {
            "clamp_m": int(clamp_m),
            "pixels_total": total,
            "pixels_land": n_land,
            "pixels_sea": total - n_land,
            "pct_land": round(n_land / total * 100.0, 4),
            "pct_sea": round((total - n_land) / total * 100.0, 4),
            "m_per_pixel_row": round(m_row, 4),
            "m_per_pixel_col": round(m_col, 4),
        }
    )
    return out, note


def _upsample(small: np.ndarray, shape: tuple[int, int]) -> np.ndarray:
    """Naikkan grid desimasi ke bentuk penuh dengan interpolasi bilinear."""
    from scipy.ndimage import zoom

    factors = (shape[0] / small.shape[0], shape[1] / small.shape[1])
    out = zoom(small, factors, order=1, mode="nearest").astype(np.float32)
    # zoom membulatkan ukuran keluaran, jadi selisih satu-dua piksel dirapikan
    # dengan memotong atau menyalin baris/kolom tepi.
    return _fit_shape(out, shape)


def _fit_shape(arr: np.ndarray, shape: tuple[int, int]) -> np.ndarray:
    out = arr[: shape[0], : shape[1]]
    if out.shape == shape:
        return out
    pad_r = shape[0] - out.shape[0]
    pad_c = shape[1] - out.shape[1]
    return np.pad(out, ((0, max(0, pad_r)), (0, max(0, pad_c))), mode="edge")


def write_manifest(path: Path, note: dict, extra: dict | None = None) -> Path:
    """Catatan asal-usul di samping mask, dalam bentuk yang bisa dibaca mesin.

    Metadata yang sama sudah tertanam sebagai tag GeoTIFF, tapi tag hanya
    terbaca oleh yang membuka berkasnya. Manifest terpisah membuat asal-usul
    dan statistiknya terbaca tanpa GDAL — termasuk oleh konsumen hilir yang
    cuma ingin tahu berapa persen AOI ini laut sebelum memutuskan memakainya.
    """
    import json

    payload = {
        "layer": LAND_DISTANCE_STEM,
        "semantics": (
            "signed distance to coastline in metres; >0 land, <0 sea, 0 coastline"
        ),
        "applies_to": "all dates in this dataset (same grid as every fusion stack)",
        "not_a_filter": (
            "raw and fusion data are untouched; consuming this layer is the "
            "downstream model's decision"
        ),
        "rivers_and_lakes": (
            "NOT excluded — inland water stays positive because river and lake "
            "overflow is the signal being studied"
        ),
        "source": {
            "name": "OSM land polygons",
            "url": "https://osmdata.openstreetmap.de/download/land-polygons-complete-4326.zip",
            "license": "ODbL, OpenStreetMap contributors",
            "table": LAND_TABLE,
        },
        "statistics": note,
    }
    payload.update(extra or {})
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def render_preview(data: np.ndarray, path: Path, max_side: int = 1024) -> Path:
    """PNG pemeriksaan cepat: laut biru, darat oranye, nol di batasnya.

    Colormap divergen dipakai justru karena besarannya memang dua-kutub —
    tandanya yang membawa arti, dan titik nolnya adalah garis pantai. Dengan
    sequential, batas darat/laut jadi gradasi samar dan kesalahan penempatan
    garis pantai sulit terlihat; dengan divergen, batas itu tegas dan salah
    sedikit pun langsung kentara.
    """
    from PIL import Image

    step = max(1, int(np.ceil(max(data.shape) / max_side)))
    d = data[::step, ::step].astype(np.float32)

    sea = d < 0
    land = ~sea
    rgb = np.zeros((*d.shape, 3), np.uint8)
    deep = np.clip(-d, 0, 12_000) / 12_000.0
    rgb[..., 2] = np.where(sea, 120 + deep * 135, 0).astype(np.uint8)
    inland = np.clip(d, 0, float(CLAMP_M)) / float(CLAMP_M)
    rgb[..., 0] = np.where(land, 60 + inland * 195, 0).astype(np.uint8)
    rgb[..., 1] = np.where(land, 40 + inland * 140, 0).astype(np.uint8)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(rgb).save(path)
    return path


def write_cog(
    data: np.ndarray,
    path: Path,
    transform: Affine,
    crs: str = "EPSG:4326",
    tags: dict | None = None,
    nodata: float | None = None,
    description: str = "signed distance to coastline (m)",
) -> Path:
    """Tulis int16 sebagai GeoTIFF ber-tile, terkompresi, dengan overview.

    Ber-tile dan bukan per baris supaya konsumen bisa membaca sepotong wilayah
    tanpa memuat seluruh berkas — pada AOI JAWA selisihnya antara membaca
    beberapa megabita dan 6,6 GB. Predictor 2 dipakai karena medan jarak
    berubah perlahan antar piksel bertetangga, jadi selisihnya jauh lebih
    mampat daripada nilainya.

    dtype diambil dari array, bukan dipaku: penulis ini dipakai bersama oleh
    land_distance (int16 bertanda) dan water_occurrence (uint8 0-100), dan
    kedua layer referensi sebaiknya punya bentuk berkas yang sama persis.
    """
    import rasterio

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": data.shape[0],
        "width": data.shape[1],
        "count": 1,
        "dtype": data.dtype.name,
        "crs": crs,
        "transform": transform,
        "nodata": NODATA if nodata is None else nodata,
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "compress": "DEFLATE",
        "predictor": 2,
        "zlevel": 6,
        "BIGTIFF": "IF_SAFER",
    }
    with atomic_path(path) as tmp_out:
        with rasterio.open(tmp_out, "w", **profile) as dst:
            dst.write(data, 1)
            dst.update_tags(**{k: str(v) for k, v in (tags or {}).items()})
            dst.set_band_description(1, description)
            dst.build_overviews([2, 4, 8, 16, 32], rasterio.enums.Resampling.average)
    return path
