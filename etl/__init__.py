# etl/__init__.py
import os

# Muat .env di sini, bukan di tiap entry point. `DatabaseClient.from_env()`
# membaca DB_* lewat os.getenv, tapi sebelumnya cuma api/main.py, etl/config.py,
# dan tests/conftest.py yang memanggil load_dotenv() -- sehingga setiap CLI
# `python -m etl.<modul>` (mis. migrate_data_structure, seed_data) berjalan
# dengan DB_PASSWORD kosong dan gagal dengan "fe_sendauth: no password supplied".
# Semua modul etl lewat sini, jadi satu pemanggilan di sini menutup semuanya.
#
# override=False: variabel yang sudah ada di environment tetap menang atas .env,
# jadi menjalankan dengan DB_NAME=... di depan perintah tetap berfungsi.
try:
    from dotenv import load_dotenv

    load_dotenv(override=False)
except ImportError:  # python-dotenv opsional saat runtime minimal
    pass

# This machine has three mutually-incompatible PROJ installations fighting over
# the same env vars: PostgreSQL/PostGIS sets PROJ_LIB/GDAL_DATA system-wide
# (PROJ 8.2.1, DB layout 1.2), a stray .env sets PROJ_LIB/PROJ_DATA to a path
# that doesn't even exist on disk, and the venv's rasterio (PROJ 9.5.0, layout
# 1.4) and pyproj (PROJ 9.4.1, layout 1.3) wheels each bundle their own
# matching proj.db. Whichever of PROJ_LIB/PROJ_DATA/GDAL_DATA is already set
# in the process env wins over each wheel's own bundled data, which is how you
# get "Cannot find proj.db" / "CRSError: unknown EPSG code" during CALIBRATE,
# CROP, etc. Every rasterio/pyproj-touching module in this codebase imports
# through the `etl` package, so stripping these here — before any of them are
# imported — forces rasterio and pyproj to fall back to their own bundled,
# version-matched PROJ data instead of an external, incompatible one.
#
# Urutannya penting: strip ini berjalan SETELAH load_dotenv di atas, jadi kalau
# suatu saat PROJ_LIB/PROJ_DATA/GDAL_DATA muncul lagi di .env, dia tetap ikut
# dibuang dan tidak bisa menghidupkan kembali bug itu lewat pintu belakang.
for _var in ("PROJ_LIB", "PROJ_DATA", "GDAL_DATA"):
    os.environ.pop(_var, None)
del _var
