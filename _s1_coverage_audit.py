"""Hitung fraksi piksel valid (cakupan AOI) tiap fusion_*_hybrid_processed.h5
Wajo, dari layer sentinel1/VV. Dipakai untuk klasifikasi full/terpotong
sedikit/terpotong banyak.
"""
import h5py
import numpy as np
from pathlib import Path

DATASETS = [
    "48_wajo_jan_apr_2023",
    "49_wajo_mei_agt_2023",
    "50_wajo_sep_dec_2023",
    "40_wajo_jan_apr_2024",
    "41_wajo_mei_agt_2024",
    "51_wajo_sep_dec_2024",
    "37_wajo_jan_apr_2025",
    "38_wajo_mei_agt_2025",
    "39_wajo_sep_dec_2025",
]

ROOT = Path("data/datasets")
STEP = 4  # decimasi baca supaya cepat, cukup untuk estimasi fraksi

rows = []
for dir_name in DATASETS:
    hybrid_dir = ROOT / dir_name / "fusion" / "hybrid"
    for f in sorted(hybrid_dir.glob("fusion_*_hybrid_processed.h5")):
        date_key = f.name.split("_")[1]
        try:
            with h5py.File(f, "r") as h5:
                vv = h5["sentinel1/VV"]
                arr = vv[::STEP, ::STEP]
                valid = np.isfinite(arr)
                frac = float(valid.mean())
        except Exception as exc:
            frac = None
            print(f"GAGAL {f}: {exc}")
        rows.append((dir_name, date_key, frac))

print(f"{'dataset':<24}{'tanggal':>10}{'valid_frac':>12}")
for dir_name, date_key, frac in rows:
    print(f"{dir_name:<24}{date_key:>10}{'' if frac is None else f'{frac*100:6.2f}%':>12}")

fracs = [f for _, _, f in rows if f is not None]
print("\nTotal tanggal:", len(rows))
import numpy as np
fracs = np.array(fracs)
for lo, hi, label in [(0.98, 1.01, "FULL (>=98%)"), (0.80, 0.98, "TERPOTONG SEDIKIT (80-98%)"), (0.0, 0.80, "TERPOTONG BANYAK (<80%)")]:
    n = int(((fracs >= lo) & (fracs < hi)).sum())
    print(f"{label}: {n} tanggal")
print("min:", fracs.min()*100, "% max:", fracs.max()*100, "% mean:", fracs.mean()*100, "%")

# simpan detail ke csv untuk dicek manual
import csv
with open("_s1_coverage_detail.csv", "w", newline="") as fp:
    w = csv.writer(fp)
    w.writerow(["dataset", "tanggal", "valid_fraction_percent"])
    for dir_name, date_key, frac in rows:
        w.writerow([dir_name, date_key, "" if frac is None else round(frac*100, 2)])
