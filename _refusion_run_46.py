import logging
from etl.database_client import DatabaseClient
from etl.refusion import refuse_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATES = [
    "20240901", "20240913", "20240925", "20241007", "20241019", "20241031",
    "20241112", "20241124", "20241206", "20241214", "20241218", "20241230",
]

db = DatabaseClient.from_env()
job_id = 44

results = {}
for d in DATES:
    try:
        ok = refuse_date(db, job_id, d)
        results[d] = "OK" if ok else "SKIPPED (no members)"
    except Exception as exc:
        results[d] = f"ERROR: {exc}"

print("\n=== SUMMARY dataset 46, job 44 ===")
for d, r in results.items():
    print(f"{d}: {r}")
