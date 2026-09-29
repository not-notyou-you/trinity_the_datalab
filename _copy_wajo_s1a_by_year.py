"""Salin fusion_<tanggal>_hybrid_processed.h5 Wajo yang murni Sentinel-1A
ke data/datasets/wajo_processed_hybrid_only/<tahun>/ (h5 saja, tanpa raw/metadata)."""
import shutil
from collections import defaultdict
from pathlib import Path

from sqlalchemy import text

from etl.database_client import DatabaseClient

DATASET_YEAR = {
    48: ("48_wajo_jan_apr_2023", "2023"),
    49: ("49_wajo_mei_agt_2023", "2023"),
    40: ("40_wajo_jan_apr_2024", "2024"),
    41: ("41_wajo_mei_agt_2024", "2024"),
    37: ("37_wajo_jan_apr_2025", "2025"),
    38: ("38_wajo_mei_agt_2025", "2025"),
    39: ("39_wajo_sep_dec_2025", "2025"),
}

ROOT = Path("data/datasets")
OUT_ROOT = ROOT / "wajo_processed_hybrid_only"

db = DatabaseClient.from_env()
with db.session() as sess:
    rows = sess.execute(text(
        """
        SELECT dj.dataset_id, ss.product_identifier
        FROM scene_job_state ss
        JOIN dataset_jobs dj ON dj.job_id = ss.job_id
        WHERE dj.dataset_id IN (37,38,39,40,41,48,49)
        """
    )).all()

by_date = defaultdict(set)
for ds, pid in rows:
    parts = pid.split("_")
    date_key = parts[4][:8]
    sat = pid[:3]
    by_date[(ds, date_key)].add(sat)

summary = {}
for dataset_id, (dir_name, year) in DATASET_YEAR.items():
    hybrid_dir = ROOT / dir_name / "fusion" / "hybrid"
    out_dir = OUT_ROOT / year
    out_dir.mkdir(parents=True, exist_ok=True)

    s1c_dates = {d for (ds, d), sats in by_date.items() if ds == dataset_id and "S1C" in sats}

    copied, skipped = 0, 0
    for f in sorted(hybrid_dir.glob("fusion_*_hybrid_processed.h5")):
        date_key = f.name.split("_")[1]
        if date_key in s1c_dates:
            skipped += 1
            continue
        shutil.copy2(f, out_dir / f.name)
        copied += 1

    summary[dataset_id] = (dir_name, year, copied, skipped)

print(f"{'dataset':<28}{'tahun':>7}{'disalin':>10}{'dilewati(S1C)':>16}")
for ds, (dir_name, year, c, s) in summary.items():
    print(f"{dir_name:<28}{year:>7}{c:>10}{s:>16}")
