# etl/geo_utils.py
"""
Utilitas bbox WGS84 yang dipakai bersama oleh API (validasi request lokasi) dan
ETL (resolusi lokasi + geocoding). Aturan validasi tinggal di satu tempat ini
supaya UI, API, dan pipeline tidak pernah berbeda pendapat soal bbox yang sah.
"""

from __future__ import annotations

import re

# Batas span AOI. Swath Sentinel-1 IW ~250 km (~2.25 derajat), jadi 10 derajat
# (~1100 km) sudah sangat longgar; di atas itu hampir pasti salah input dan akan
# menarik ratusan scene tanpa disengaja.
MAX_SPAN_DEG = 10.0
# Di bawah ini bbox lebih kecil dari satu piksel GRD (~10 m) dan tidak berguna.
MIN_SPAN_DEG = 0.001


class BBoxError(ValueError):
    """Bbox tidak valid. Pesannya sudah ramah untuk ditampilkan ke pengguna."""


def validate_bbox(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> tuple[float, float, float, float]:
    """Validasi bbox WGS84 dan kembalikan tuple float yang sudah bersih.

    Raises BBoxError dengan pesan berbahasa Indonesia bila tidak valid.
    """
    try:
        min_lon, min_lat = float(min_lon), float(min_lat)
        max_lon, max_lat = float(max_lon), float(max_lat)
    except (TypeError, ValueError):
        raise BBoxError("Koordinat harus berupa angka")

    for label, value in (("Longitude", min_lon), ("Longitude", max_lon)):
        if not -180.0 <= value <= 180.0:
            raise BBoxError(f"{label} harus di rentang -180 sampai 180 (dapat {value})")
    for label, value in (("Latitude", min_lat), ("Latitude", max_lat)):
        if not -90.0 <= value <= 90.0:
            raise BBoxError(f"{label} harus di rentang -90 sampai 90 (dapat {value})")

    if min_lon >= max_lon:
        raise BBoxError("Longitude minimum harus lebih kecil dari longitude maksimum")
    if min_lat >= max_lat:
        raise BBoxError("Latitude minimum harus lebih kecil dari latitude maksimum")

    span_lon, span_lat = max_lon - min_lon, max_lat - min_lat
    if span_lon < MIN_SPAN_DEG or span_lat < MIN_SPAN_DEG:
        raise BBoxError(
            f"Area terlalu kecil, minimum {MIN_SPAN_DEG} derajat (~110 m) per sisi"
        )
    if span_lon > MAX_SPAN_DEG or span_lat > MAX_SPAN_DEG:
        raise BBoxError(
            f"Area terlalu besar, maksimum {MAX_SPAN_DEG} derajat (~1100 km) per sisi"
        )

    return min_lon, min_lat, max_lon, max_lat


def bbox_to_wkt(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> str:
    """Bbox -> POLYGON WKT, urutan titik searah seperti seed data yang ada."""
    return (
        f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, "
        f"{max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
    )


def parse_bbox_string(raw: str) -> tuple[float, float, float, float]:
    """Parse bbox yang di-paste pengguna.

    Menerima 4 angka dipisah koma/spasi/titik-koma dengan urutan
    ``min_lon, min_lat, max_lon, max_lat`` (urutan bbox standar GDAL/GeoJSON).
    """
    numbers = re.findall(r"-?\d+(?:\.\d+)?", raw or "")
    if len(numbers) != 4:
        raise BBoxError(
            "Format bbox harus 4 angka: min_lon, min_lat, max_lon, max_lat"
        )
    return validate_bbox(*[float(n) for n in numbers])


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------
# Geocoding sengaja tinggal di layer ETL, bukan di route API: dataset_manager
# (lewat location_resolver) dan endpoint pencarian lokasi memakai fungsi yang
# sama, sehingga penggantian provider cukup dilakukan di berkas ini.
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "the-trinity-flood-pipeline/1.0"
GEOCODE_TIMEOUT_SEC = 15


def geocode_search(query: str, limit: int = 5, country_codes: str = "id") -> list[dict]:
    """Cari lokasi lewat Nominatim, kembalikan kandidat dengan bbox siap pakai.

    Setiap item: ``{name, display_name, bbox, bbox_wkt, type}`` dengan ``bbox``
    dalam urutan ``[min_lon, min_lat, max_lon, max_lat]``.

    Kandidat dengan bbox yang tidak lolos :func:`validate_bbox` (mis. seluruh
    negara, atau satu titik alamat) dibuang dari hasil, bukan bikin error —
    supaya pengguna tetap melihat kandidat lain yang bisa dipakai.
    """
    import requests

    query = (query or "").strip()
    if not query:
        return []

    params = {
        "q": query,
        "format": "json",
        "limit": max(1, min(int(limit), 20)),
        "polygon_geojson": 0,
    }
    if country_codes:
        params["countrycodes"] = country_codes

    resp = requests.get(
        NOMINATIM_URL,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=GEOCODE_TIMEOUT_SEC,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Geocoding gagal ({resp.status_code}) untuk lokasi: {query}")

    results: list[dict] = []
    for item in resp.json():
        try:
            south, north, west, east = [float(x) for x in item["boundingbox"]]
            bbox = validate_bbox(west, south, east, north)
        except (BBoxError, KeyError, TypeError, ValueError):
            continue
        display = item.get("display_name", query)
        results.append({
            "name": display.split(",")[0].strip() or query,
            "display_name": display,
            "bbox": list(bbox),
            "bbox_wkt": bbox_to_wkt(*bbox),
            "type": item.get("type"),
        })
    return results
