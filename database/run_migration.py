#!/usr/bin/env python
"""
Jalankan satu berkas migrasi SQL memakai kredensial dari .env.

Kenapa ada: `psql "$DATABASE_URL"` adalah sintaks bash. Di PowerShell
`$DATABASE_URL` adalah variabel PowerShell yang kosong, jadi psql menerima
string kosong lalu diam-diam jatuh ke default (user OS, database bernama sama)
dan gagal autentikasi -- yang terbaca seolah passwordnya salah padahal .env
tidak pernah ikut terbaca sama sekali. .env dimuat oleh python-dotenv saat
runtime, bukan oleh shell.

Skrip ini memakai jalur yang sama dengan pipeline (etl.config -> load_dotenv),
jadi kredensialnya dijamin sama dengan yang dipakai ETL dan API.

AUTOCOMMIT dipakai karena `ALTER TYPE ... ADD VALUE` (migrasi 020) tidak boleh
berada di dalam blok transaksi. Berkas migrasi yang butuh transaksi menuliskan
BEGIN/COMMIT-nya sendiri, dan itu tetap berjalan benar di mode ini.

Pakai:
    python database/run_migration.py database/migrations/019_....sql
    python database/run_migration.py database/migrations/*.sql
    python database/run_migration.py --check        # cek koneksi saja
    python database/run_migration.py --list         # migrasi yang tersedia

Pola glob dikembangkan DI DALAM skrip ini, bukan diserahkan ke shell:
PowerShell tidak mengembangkan wildcard untuk argumen program (berbeda dari
bash), jadi `*.sql` akan sampai ke sini sebagai teks apa adanya dan dibaca
sebagai nama berkas yang tidak ada.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine, text  # noqa: E402

from etl.config import DatabaseConfig  # noqa: E402  (memicu load_dotenv)


def _engine():
    cfg = DatabaseConfig()
    url = (
        f"postgresql+psycopg2://{cfg.user}:{cfg.password}"
        f"@{cfg.host}:{cfg.port}/{cfg.name}"
    )
    # Password tidak pernah ikut dicetak.
    print(f"[DB] {cfg.user}@{cfg.host}:{cfg.port}/{cfg.name}")
    return create_engine(url, isolation_level="AUTOCOMMIT")


def _print_available() -> None:
    """Daftar berkas migrasi yang ada, supaya nama yang salah ketik langsung
    terlihat alih-alih hanya dilaporkan 'tidak ada'."""
    d = Path(__file__).resolve().parent / "migrations"
    files = sorted(d.glob("*.sql"))
    if not files:
        print(f"[INFO] tidak ada berkas .sql di {d}")
        return
    print(f"[INFO] migrasi tersedia di {d}:")
    for f in files:
        print(f"         {f.name}")


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2

    if argv[0] == "--list":
        _print_available()
        return 0

    engine = _engine()
    rc_missing: list[str] = []

    if argv[0] == "--check":
        with engine.connect() as conn:
            ver = conn.scalar(text("SELECT version()"))
        print(f"[OK] tersambung — {str(ver).split(',')[0]}")
        return 0

    targets: list[Path] = []
    for arg in argv:
        path = Path(arg)
        if path.is_file():
            targets.append(path)
            continue
        # Glob dikembangkan di sini (lihat docstring): PowerShell menyerahkan
        # polanya apa adanya.
        matches = sorted(Path(path.parent or ".").glob(path.name))
        if matches:
            targets.extend(m for m in matches if m.is_file())
            continue
        print(f"[SKIP] tidak ada berkas yang cocok: {arg}")
        rc_missing.append(arg)

    if not targets:
        print("[INFO] tidak ada migrasi yang dijalankan.")
        _print_available()
        return 1 if rc_missing else 0

    rc = 1 if rc_missing else 0
    for path in targets:
        sql = path.read_text(encoding="utf-8")
        print(f"[RUN ] {path.name} ({len(sql)} bytes)")
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql(sql)
        except Exception as exc:
            print(f"[FAIL] {path.name}: {exc}")
            rc = 1
            continue
        print(f"[DONE] {path.name}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
