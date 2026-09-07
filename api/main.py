# api/main.py
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
import logging
import mimetypes
import time

mimetypes.add_type("image/webp", ".webp")
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from etl.database_client import DatabaseClient
# get_db di-re-export supaya `from api.main import get_db` tetap jalan; objeknya
# sama persis dengan api.deps.get_db, jadi dependency_overrides lewat jalur mana
# pun mengenai callable yang sama.
from api.deps import get_db, set_db
from api.routes import datasets, health, lineage, live, pipeline, preview, products, quality, regions, scenes, storage

logger = logging.getLogger(__name__)
_db_client: DatabaseClient | None = None
_live_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global _db_client, _live_scheduler
    logger.info("[API] startup: initializing DatabaseClient")
    _db_client = DatabaseClient.from_env()
    set_db(_db_client)
    health_info = _db_client.check_health()
    if not health_info.get("connected"):
        logger.error("[API] DB health check FAILED: %s", health_info)
    else:
        logger.info("[API] DB connected. Pool: %s", health_info)

    try:
        from etl.live_scheduler import LiveScheduler
        _live_scheduler = LiveScheduler(_db_client)
        _live_scheduler.start()
        logger.info("[API] LiveScheduler started")
    except Exception:
        logger.exception(
            "[API] LiveScheduler gagal dimulai (cek instalasi rasterio/apscheduler). "
            "API tetap jalan, tapi live dataset tidak akan auto-check harian."
        )
        _live_scheduler = None

    yield

    logger.info("[API] shutdown: stopping LiveScheduler")
    if _live_scheduler:
        _live_scheduler.shutdown()

    logger.info("[API] shutdown: disposing DatabaseClient")
    set_db(None)
    if _db_client:
        _db_client.dispose()


app = FastAPI(
    title="Sentinel-1 Flood Detection Data Pipeline API",
    description=(
        "REST API for querying Sentinel-1 SAR scenes, data products, "
        "quality metrics, transformation lineage, dataset management, "
        "and live monitoring for flood detection research."
    ),
    version="1.2.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    start = time.perf_counter()
    response: Response = await call_next(request)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    logger.info("%s %s -> %d (%dms)", request.method, request.url.path, response.status_code, elapsed_ms)
    return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Ratakan error validasi Pydantic jadi satu kalimat.

    Bentuk bawaan FastAPI (`detail` berisi list of dict) tidak bisa ditampilkan
    langsung di UI; front-end di web/app.js membaca `detail` sebagai teks.
    """
    messages = []
    for err in exc.errors():
        msg = str(err.get("msg", "")).removeprefix("Value error, ").strip()
        loc = [str(p) for p in err.get("loc", []) if p not in ("body", "query", "path")]
        messages.append(f"{'.'.join(loc)}: {msg}" if loc else msg)
    detail = " | ".join(dict.fromkeys(m for m in messages if m)) or "Permintaan tidak valid"
    return JSONResponse(status_code=422, content={"detail": detail})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "path": str(request.url.path)},
    )


# Info API. Dulu ada di "/", lalu tergeser waktu web UI di-mount di "/" (commit
# 76bdcc3). Sekarang tinggal di "/api" bersama route lainnya, sedangkan "/"
# memang milik UI.
@app.get("/api", include_in_schema=False)
async def api_info() -> dict:
    return {"message": app.title, "docs": app.docs_url, "version": app.version}


app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(scenes.router, prefix="/api/scenes", tags=["Scenes"])
app.include_router(products.router, prefix="/api/products", tags=["Products"])
app.include_router(quality.router, prefix="/api/quality", tags=["Quality"])
app.include_router(lineage.router, prefix="/api/metadata", tags=["Lineage"])
app.include_router(preview.router, prefix="/api/preview", tags=["Preview"])
app.include_router(storage.router, prefix="/api/storage", tags=["Storage"])
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["Pipeline"])
app.include_router(datasets.router, prefix="/api/datasets", tags=["Datasets"])
app.include_router(live.router, prefix="/api/live", tags=["Live"])
app.include_router(regions.router, prefix="/api/regions", tags=["Regions"])
app.mount("/", StaticFiles(directory="web", html=True), name="web")