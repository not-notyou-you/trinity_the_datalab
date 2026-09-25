# api/routes/report.py
"""GET /api/datasets/{id}/report - laporan PDF komprehensif (report.md).

Generasinya sinkron, bukan job/job_id + polling: laporan hanya membaca
statistik dan chart agregat yang sudah murah dihitung (query DB + PNG kecil
lewat matplotlib), beda dari job ETL yang bisa berjam-jam. Pola ini konsisten
dengan download_dataset() di datasets.py (ZIP dataset), bukan pola job
background di dataset_manager.py yang dipakai untuk pipeline ingesti."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse

from api.deps import get_db
from etl.database_client import DatabaseClient
from etl.dataset_manager import DatasetManager
from etl.report_generator import ReportGenerationError, ReportGenerator

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/{dataset_id}/report", summary="Laporan PDF komprehensif dataset")
async def get_dataset_report(
    dataset_id: int,
    force: bool = Query(False, description="Lewati cache, buat ulang laporan"),
    db: DatabaseClient = Depends(get_db),
) -> FileResponse:
    info = DatasetManager(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    try:
        pdf_path = ReportGenerator(dataset_id, db).generate(force=force)
    except ReportGenerationError as exc:
        raise HTTPException(400, str(exc)) from exc

    from etl import folder_manager as fm
    filename = f"{fm.slugify(info['name'])}_report.pdf"
    return FileResponse(str(pdf_path), filename=filename, media_type="application/pdf")


@router.get("/{dataset_id}/report/json", summary="Ringkasan laporan dalam format JSON (Section 10)")
async def get_dataset_report_json(
    dataset_id: int,
    force: bool = Query(False, description="Lewati cache, buat ulang laporan"),
    db: DatabaseClient = Depends(get_db),
) -> FileResponse:
    """JSON ditulis di samping PDF (stem sama) oleh ReportGenerator, jadi
    endpoint ini memakai cache yang sama dengan endpoint PDF."""
    info = DatasetManager(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    try:
        pdf_path = ReportGenerator(dataset_id, db).generate(force=force)
        json_path = pdf_path.with_suffix(".json")
        if not json_path.exists():  # laporan cache lama (v1) tanpa JSON
            json_path = ReportGenerator(dataset_id, db).generate(force=True).with_suffix(".json")
    except ReportGenerationError as exc:
        raise HTTPException(400, str(exc)) from exc

    from etl import folder_manager as fm
    filename = f"{fm.slugify(info['name'])}_report.json"
    return FileResponse(str(json_path), filename=filename, media_type="application/json")
