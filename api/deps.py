# api/deps.py
"""Dependency bersama untuk seluruh route API.

Modul ini sengaja terpisah dari ``api/main.py``. ``api.main`` meng-import setiap
route, jadi route tidak bisa meng-import ``api.main`` di level modul tanpa
circular import. Sebelumnya tiap route menyiasatinya dengan shim lokal::

    async def _get_db():
        from api.main import get_db      # import di dalam fungsi
        return get_db()

Shim itu bikin ``app.dependency_overrides`` tidak pernah berlaku: FastAPI
mencocokkan override berdasarkan callable yang persis dipakai di ``Depends(...)``
(yaitu ``_get_db`` milik masing-masing modul), dan ``_get_db`` memanggil
``get_db()`` langsung sehingga melewati mekanisme override. Akibatnya TestClient
di tests/conftest.py selalu mendapat DatabaseClient milik produksi.

Dengan satu callable bersama di sini, semua route memakai ``Depends(get_db)``
yang sama, dan ``app.dependency_overrides[get_db]`` berlaku untuk semuanya.
"""

from __future__ import annotations

from etl.database_client import DatabaseClient

_db_client: DatabaseClient | None = None


def set_db(client: DatabaseClient | None) -> None:
    """Dipasang oleh lifespan ``api.main`` saat startup/shutdown."""
    global _db_client
    _db_client = client


def get_db() -> DatabaseClient:
    """Dependency FastAPI: DatabaseClient yang dipakai bersama seluruh route."""
    if _db_client is None:
        raise RuntimeError("DatabaseClient not initialized. App startup failed.")
    return _db_client
