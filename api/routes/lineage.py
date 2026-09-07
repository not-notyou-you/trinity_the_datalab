# api/routes/lineage.py
"""
GET /api/metadata/lineage/{product_id} — transformation provenance chain.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from api.schemas import LineageResponse, LineageStep
from api.deps import get_db
from etl.database_client import DataProduct, DatabaseClient
from etl.lineage_tracker import LineageTracker

router = APIRouter()


@router.get(
    "/lineage/{product_id}",
    response_model=LineageResponse,
    summary="Transformation lineage (ancestors)",
    description=(
        "Trace the full provenance chain from a product back to its RAW source. "
        "Returns ordered list of transformation steps: CROP → LEE_FILTER → GOLD_EXPORT. "
        "Each step carries `source` plus `parent_tier`/`child_tier`, so a dataset's "
        "parallel per-sensor chains (S1 RAW→GOLD, MODIS/GPM SILVER→GOLD) stay "
        "distinguishable. "
        "Use direction=descendants to trace forward to derived products."
    ),
)
async def get_lineage(
    product_id: int,
    db:         DatabaseClient = Depends(get_db),
    direction:  str            = Query(
        "ancestors",
        pattern="^(ancestors|descendants)$",
        description="ancestors = trace back to source | descendants = trace forward",
    ),
) -> LineageResponse:
    """
    Retrieve the full data lineage chain for a given product.

    - **ancestors**: starts from `product_id` and walks backwards to the RAW source.
    - **descendants**: starts from `product_id` and walks forward to all derived products.
    """
    with db.session() as sess:
        p = sess.get(DataProduct, product_id)
        if not p:
            raise HTTPException(status_code=404, detail=f"Product {product_id} not found")

    tracker = LineageTracker(db)
    chain   = tracker.get_lineage_chain(product_id, direction=direction)

    return LineageResponse(
        product_id  = product_id,
        direction   = direction,
        chain       = [LineageStep(**step) for step in chain],
        total_steps = len(chain),
    )
