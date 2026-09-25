# etl/live_interpret.py
"""Kalimat kondisi Live Monitoring (LIVE_MONITORING.md 4.2), rule-based.

Format wajib:  Menampilkan [apa] dalam kondisi [kategori] karena [angka].

SATU-SATUNYA tempat ambang: THRESHOLDS di bawah. Ubah angkanya di sini; kalimat,
status daerah, garis ambang grafik, dan metrik (live_metrics) membacanya dari
dict ini.

Sumber nilai awal:
  s1_vh_water_db  -20 dB. Ambang air terbuka VH yang lazim untuk Sentinel-1
                  GRD (rentang literatur -20..-24 dB; mis. UN-SPIDER
                  Recommended Practice Flood Mapping, Twele et al. 2016).
                  -20 dipilih yang lebih longgar: lebih cepat memberi tanda,
                  dengan konsekuensi lebih banyak permukaan halus (jalan,
                  lahan terbuka basah) ikut terhitung. Karena itu kategori
                  diputuskan dari PERUBAHAN terhadap scene sebelumnya, bukan
                  dari persen absolut -- badan air permanen di AOI (laut,
                  sungai) konstan dan tidak memicu tanda.
  s1_vh_*_delta   +2 / +5 poin persen antar scene: di bawah 2 poin masih
                  dalam variasi normal antar-akuisisi (sudut, angin di
                  permukaan air); >= 5 poin adalah pertambahan area air yang
                  jelas.
  s1_vv_drop_db   1,5 dB. Penurunan rata-rata VV sebesar ini untuk seluruh AOI
                  jauh di atas derau kalibrasi S1 (~0,5 dB) dan menandakan
                  permukaan lebih basah/halus.
  modis_flood_*   1% / 5% area teramati berkelas "banjir" MCDWD.
  modis_min_valid 10% piksel teramati; di bawah itu tutupan awan terlalu
                  besar untuk dibaca.
  ndwi_water      0,0 (ambang McFeeters 1996 untuk air terbuka).
  ndvi_drop       0,1 turun antar komposit: tekanan vegetasi yang jelas
                  (variasi musiman antar komposit 8 hari umumnya < 0,05).
  rain_24h        20 / 50 mm: kategori BMKG hujan lebat (20-50 mm/hari) dan
                  sangat lebat (50-100 mm/hari).
  rain_72h        50 / 100 mm akumulasi 3 hari (100 mm = contoh di dokumen
                  spesifikasi, ~ dua hari berturut-turut hujan sangat lebat).
  rain_7d         75 / 150 mm akumulasi sepekan (tanah praktis jenuh).
"""
from __future__ import annotations

THRESHOLDS: dict[str, float] = {
    "s1_vh_water_db": -20.0,
    "s1_vh_warn_delta_pct": 2.0,
    "s1_vh_flood_delta_pct": 5.0,
    # Tanpa scene pembanding: persen absolut yang sudah patut diwaspadai.
    "s1_vh_first_scene_warn_pct": 30.0,
    "s1_vv_drop_db": 1.5,
    "modis_min_valid_pct": 10.0,
    "modis_flood_warn_pct": 1.0,
    "modis_flood_high_pct": 5.0,
    "ndwi_water": 0.0,
    "ndwi_warn_delta_pct": 2.0,
    "ndwi_flood_delta_pct": 5.0,
    "ndvi_drop": 0.10,
    "rain_24h_warn_mm": 20.0,
    "rain_24h_high_mm": 50.0,
    "rain_72h_warn_mm": 50.0,
    "rain_72h_high_mm": 100.0,
    "rain_7d_warn_mm": 75.0,
    "rain_7d_high_mm": 150.0,
}

NORMAL = "normal"
WASPADA = "waspada"
TINGGI = "tinggi"
GENANGAN = "terindikasi genangan"
NA = "tidak tersedia"

# Tingkat untuk warna UI dan status daerah: -1 tidak tersedia, 0..2.
LEVEL = {NA: -1, NORMAL: 0, WASPADA: 1, TINGGI: 2, GENANGAN: 2}

PREVIEW_KEYS = (
    "s1_vv", "s1_vh",
    "modis_flood", "modis_ndvi", "modis_ndwi",
    "gpm_rain_24h", "gpm_rain_72h", "gpm_rain_7d",
)


def fmt(v, nd: int = 1, signed: bool = False) -> str:
    """Angka gaya Indonesia: koma desimal, minus tipografis."""
    if v is None:
        return "-"
    s = f"{abs(v):.{nd}f}"
    if "." in s:  # 142,0 -> 142 ; 0,50 -> 0,5
        s = s.rstrip("0").rstrip(".")
    s = s.replace(".", ",")
    if v < 0:
        return "−" + s
    return ("+" + s) if signed and v > 0 else s


def _sentence(what: str, category: str, because: str) -> dict:
    return {
        "category": category,
        "level": LEVEL[category],
        "what": what,
        "because": because,
        "text": f"Menampilkan {what} dalam kondisi {category} karena {because}.",
    }


def _g(d: dict | None, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _delta_category(delta, warn, high, hi_cat=GENANGAN):
    if delta is None:
        return NORMAL
    if delta >= high:
        return hi_cat
    if delta >= warn:
        return WASPADA
    return NORMAL


def _change_phrase(cur, prev, unit: str, nd: int = 1, what="scene sebelumnya") -> str:
    if prev is None:
        return "belum ada scene pembanding"
    d = cur - prev
    if abs(d) < 10 ** -nd:
        return f"sama dengan {what} ({fmt(prev, nd)}{unit})"
    arah = "naik" if d > 0 else "turun"
    return f"{arah} dari {fmt(prev, nd)}{unit} pada {what}"


# ---------------------------------------------------------------------------
# Per preview
# ---------------------------------------------------------------------------

def _s1_vv(m, p, status):
    what = "kondisi permukaan umum (radar VV)"
    v = _g(m, "sentinel1", "vv_mean_db")
    if v is None:
        return _sentence(what, NA, "citra Sentinel-1 tidak berhasil diproses")
    prev = _g(p, "sentinel1", "vv_mean_db")
    t = THRESHOLDS["s1_vv_drop_db"]
    if prev is not None and v - prev <= -t:
        return _sentence(what, WASPADA,
                         f"rata-rata VV {fmt(v)} dB, turun {fmt(prev - v)} dB dari scene "
                         f"sebelumnya (ambang {fmt(t)} dB) sehingga permukaan tampak lebih basah")
    return _sentence(what, NORMAL, f"rata-rata VV {fmt(v)} dB, "
                     + _change_phrase(v, prev, " dB"))


def _s1_vh(m, p, status):
    what = "permukaan tanah (radar VH)"
    pct = _g(m, "sentinel1", "vh_water_pct")
    if pct is None:
        return _sentence(what, NA, "citra Sentinel-1 tidak berhasil diproses")
    thr = fmt(THRESHOLDS["s1_vh_water_db"], 0)
    prev = _g(p, "sentinel1", "vh_water_pct")
    base = f"{fmt(pct)}% area memiliki VH < {thr} dB"
    if prev is None:
        first = THRESHOLDS["s1_vh_first_scene_warn_pct"]
        cat = WASPADA if pct >= first else NORMAL
        return _sentence(what, cat, f"{base} (belum ada scene pembanding; ambang "
                         f"waspada {fmt(first, 0)}%)")
    cat = _delta_category(pct - prev, THRESHOLDS["s1_vh_warn_delta_pct"],
                          THRESHOLDS["s1_vh_flood_delta_pct"])
    return _sentence(what, cat, f"{base}, " + _change_phrase(pct, prev, "%"))


def _cloud_na(what, entry):
    cloud = _g(entry, "cloud_pct")
    reason = ("tidak ada citra valid"
              + (f" (tutupan awan {fmt(cloud, 0)}%)" if cloud is not None else ""))
    return _sentence(what, NA, reason)


def _modis_flood(m, p, status):
    what = "peta banjir optik MODIS"
    e = _g(m, "modis", "flood")
    if not e:
        return _sentence(what, NA, _missing_reason(status, "modis"))
    if (e.get("valid_pct") or 0) < THRESHOLDS["modis_min_valid_pct"]:
        return _cloud_na(what, e)
    f = e.get("flood_pct") or 0.0
    warn, high = THRESHOLDS["modis_flood_warn_pct"], THRESHOLDS["modis_flood_high_pct"]
    cat = GENANGAN if f >= high else WASPADA if f >= warn else NORMAL
    return _sentence(what, cat, f"{fmt(f)}% area teramati berkelas banjir (ambang waspada "
                     f"{fmt(warn, 0)}%), dengan {fmt(e.get('cloud_pct'), 0)}% area tertutup awan"
                     + _nearest_note(e))


def _modis_ndvi(m, p, status):
    what = "kesehatan vegetasi (NDVI)"
    e = _g(m, "modis", "ndvi")
    if not e:
        return _sentence(what, NA, _missing_reason(status, "modis"))
    if (e.get("valid_pct") or 0) < THRESHOLDS["modis_min_valid_pct"] or e.get("mean") is None:
        return _cloud_na(what, e)
    v, prev = e["mean"], _g(p, "modis", "ndvi", "mean")
    drop = THRESHOLDS["ndvi_drop"]
    if prev is not None and prev - v >= drop:
        cat, why = WASPADA, (f"NDVI rata-rata {fmt(v, 2)}, turun {fmt(prev - v, 2)} dari "
                             f"scene sebelumnya (ambang {fmt(drop, 2)})")
    else:
        cat, why = NORMAL, f"NDVI rata-rata {fmt(v, 2)}, " + _change_phrase(v, prev, "", 2)
    return _sentence(what, cat, why + _composite_note(e))


def _modis_ndwi(m, p, status):
    what = "indikasi air permukaan (NDWI)"
    e = _g(m, "modis", "ndwi")
    if not e:
        return _sentence(what, NA, _missing_reason(status, "modis"))
    if (e.get("valid_pct") or 0) < THRESHOLDS["modis_min_valid_pct"] or e.get("water_pct") is None:
        return _cloud_na(what, e)
    v, prev = e["water_pct"], _g(p, "modis", "ndwi", "water_pct")
    cat = _delta_category(None if prev is None else v - prev,
                          THRESHOLDS["ndwi_warn_delta_pct"], THRESHOLDS["ndwi_flood_delta_pct"])
    return _sentence(what, cat, f"{fmt(v)}% area teramati ber-NDWI > "
                     f"{fmt(THRESHOLDS['ndwi_water'], 0)}, " + _change_phrase(v, prev, "%")
                     + _composite_note(e))


def _rain(window: str, label: str):
    def _fn(m, p, status):
        what = f"curah hujan {label}"
        e = _g(m, "gpm", f"rain_{window}")
        if not e or e.get("mean_mm") is None:
            return _sentence(what, NA, _missing_reason(status, "gpm"))
        v = e["mean_mm"]
        warn = THRESHOLDS[f"rain_{window}_warn_mm"]
        high = THRESHOLDS[f"rain_{window}_high_mm"]
        if v >= high:
            cat, ref = TINGGI, f"di atas ambang {fmt(high, 0)} mm"
        elif v >= warn:
            cat, ref = WASPADA, f"di atas ambang waspada {fmt(warn, 0)} mm"
        else:
            cat, ref = NORMAL, f"di bawah ambang waspada {fmt(warn, 0)} mm"
        peak = e.get("max_mm")
        extra = f" (puncak {fmt(peak)} mm)" if peak is not None and peak > v else ""
        note = "" if e.get("imerg_runs") in (None, "F") else " — data IMERG awal, belum terkalibrasi penakar"
        return _sentence(what, cat, f"akumulasi rata-rata {fmt(v)} mm{extra}, {ref}{note}")
    return _fn


def _missing_reason(status: dict | None, source: str) -> str:
    st = _g(status, source, "status")
    if st == "FAILED":
        return "data gagal diunduh atau diproses (akan dicoba ulang)"
    return "data belum tersedia untuk tanggal ini"


def _nearest_note(e: dict) -> str:
    obs = e.get("observation_date") or e.get("matched_date")
    return f"; memakai data terdekat {obs}" if e.get("nearest") and obs else ""


def _composite_note(e: dict) -> str:
    age = e.get("age_days_median")
    per = e.get("composite_period")
    if age is None or age < 1:
        return ""
    return (f"; komposit 8 hari (periode {per}), observasi rata-rata {fmt(age, 0)} hari "
            f"sebelum scene" if per else f"; observasi rata-rata {fmt(age, 0)} hari sebelum scene")


_RULES = {
    "s1_vv": _s1_vv,
    "s1_vh": _s1_vh,
    "modis_flood": _modis_flood,
    "modis_ndvi": _modis_ndvi,
    "modis_ndwi": _modis_ndwi,
    "gpm_rain_24h": _rain("24h", "24 jam"),
    "gpm_rain_72h": _rain("72h", "72 jam"),
    "gpm_rain_7d": _rain("7d", "7 hari"),
}


def interpret_scene(metrics: dict, prev_metrics: dict | None,
                    source_status: dict | None = None) -> dict:
    """{preview_key: {category, level, text, ...}} untuk 8 preview."""
    out = {}
    for key in PREVIEW_KEYS:
        try:
            out[key] = _RULES[key](metrics or {}, prev_metrics, source_status)
        except Exception as exc:  # satu aturan rusak tidak boleh menjatuhkan kartu
            out[key] = _sentence(key, NA, f"interpretasi gagal ({exc.__class__.__name__})")
    return out


# ---------------------------------------------------------------------------
# Status singkat daerah
# ---------------------------------------------------------------------------

def area_status(interp: dict) -> dict:
    """Gabungan S1 VH + indeks air MODIS (NDWI, cadangan peta banjir) + GPM 72 jam."""
    vh = interp.get("s1_vh", {}).get("category", NA)
    optic = interp.get("modis_ndwi", {}).get("category", NA)
    if optic == NA:
        optic = interp.get("modis_flood", {}).get("category", NA)
    rain = interp.get("gpm_rain_72h", {}).get("category", NA)

    reasons = []
    if rain == TINGGI:
        reasons.append("hujan tinggi")
    elif rain == WASPADA:
        reasons.append("hujan cukup lebat")
    if vh == GENANGAN:
        reasons.append("area genangan meningkat")
    elif vh == WASPADA:
        reasons.append("area basah radar bertambah")
    if optic == GENANGAN:
        reasons.append("citra optik menunjukkan air bertambah")
    elif optic == WASPADA:
        reasons.append("indikasi air optik sedikit naik")

    strong = (vh == GENANGAN and (rain == TINGGI or optic == GENANGAN))
    if strong:
        label, level = "Tinggi", 2
    elif reasons:
        label, level = "Waspada", 1
    elif all(c == NA for c in (vh, optic, rain)):
        label, level = "Tidak tersedia", -1
        reasons = ["data scene ini belum lengkap"]
    else:
        label, level = "Normal", 0
        reasons = ["tidak ada tanda genangan atau hujan lebat"]
    return {
        "label": label,
        "level": level,
        "text": f"{label} — " + " dan ".join(reasons),
        "inputs": {"s1_vh": vh, "optic": optic, "rain_72h": rain},
    }
