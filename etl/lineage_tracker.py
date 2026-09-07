# etl/lineage_tracker.py
"""
Data lineage tracking: SHA-256 hashing, parent→child product linking,
and provenance query for audit trail.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from etl.database_client import (
    DataLineage,
    DataProduct,
    DatabaseClient,
    ProcessingJob,
    ProcessingStage,
)

logger = logging.getLogger(__name__)

# Chunk size for streaming SHA-256 computation (8 MB)
_HASH_CHUNK_BYTES = 8 * 1024 * 1024


class LineageTracker:
    """
    Tracks data provenance across ETL pipeline stages.

    Responsibilities:
        - Compute SHA-256 hashes for all output files
        - Record parent→child product relationships in data_lineage
        - Query full transformation history for any product

    The transformation DAG maps pipeline stages to lineage links:
        RAW (download) ──CROP────────► BRONZE
        BRONZE         ──LEE_FILTER──► SILVER
        SILVER         ──GOLD_EXPORT─► GOLD  (per-source analysis-ready COG)
        GOLD           ──FUSION──────► FUSION (multi-modal HDF5 stack)

    Args:
        db: Initialized DatabaseClient instance
    """

    # Maps transformation_type → expected stage_name.
    # GOLD_EXPORT was added to processing_stages by migration 013 (SILVER →
    # GOLD per-source COG, for Sentinel-1 as well as MODIS/GPM) but never
    # registered here, so every record_transformation() call from
    # module5_orchestrator / module9_fusion raised "Unknown
    # transformation_type" and failed the whole GOLD stage. COG_EXPORT stays:
    # it is still referenced by pre-013 historical lineage rows.
    _TRANSFORM_STAGE_MAP: dict[str, str] = {
        "CROP":        "CROP",
        "LEE_FILTER":  "LEE_FILTER",
        "COG_EXPORT":  "COG_EXPORT",
        "GOLD_EXPORT": "GOLD_EXPORT",
        "FUSION":      "FUSION",
        "ANALYTICS":   "QUALITY_ANALYTICS",
    }

    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # HASHING
    # ------------------------------------------------------------------

    @staticmethod
    def compute_sha256(file_path: str | Path) -> str:
        """
        Compute SHA-256 hash of a file using streaming reads.
        Handles large GeoTIFF files (multiple GB) without loading into RAM.

        Args:
            file_path : Absolute path to the file

        Returns:
            Lowercase hex digest string (64 characters)

        Raises:
            FileNotFoundError : If file does not exist
            IOError           : On read error
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found for hashing: {path}")

        h = hashlib.sha256()
        file_size = path.stat().st_size

        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(_HASH_CHUNK_BYTES), b""):
                h.update(chunk)

        digest = h.hexdigest()
        logger.debug("[HASH] %s → %s (%.2f MB)", path.name, digest[:12] + "...",
                     file_size / (1024 ** 2))
        return digest

    @staticmethod
    def compute_sha256_from_bytes(data: bytes) -> str:
        """
        Compute SHA-256 from an in-memory bytes object.

        Args:
            data : Raw bytes to hash

        Returns:
            Lowercase hex digest string
        """
        return hashlib.sha256(data).hexdigest()

    # ------------------------------------------------------------------
    # LINEAGE RECORDING
    # ------------------------------------------------------------------

    def record_transformation(
        self,
        parent_product_id: int,
        child_product_id: int,
        transformation_type: str,
        job_id: int,
        params: dict | None = None,
    ) -> int:
        """
        Record a parent→child product transformation in data_lineage.

        Args:
            parent_product_id   : Source product (input to transformation)
            child_product_id    : Result product (output of transformation)
            transformation_type : 'CROP' | 'LEE_FILTER' | 'GOLD_EXPORT' |
                                  'FUSION' | 'ANALYTICS' | 'COG_EXPORT' (legacy)
            job_id              : ProcessingJob that performed the transformation
            params              : Transformation parameters dict (bbox, window_size, etc.)

        Returns:
            lineage_id (int)

        Raises:
            ValueError    : On unknown transformation_type or missing stage
            IntegrityError: If identical lineage link already exists
        """
        stage_name = self._TRANSFORM_STAGE_MAP.get(transformation_type)
        if not stage_name:
            raise ValueError(
                f"Unknown transformation_type: '{transformation_type}'. "
                f"Valid: {list(self._TRANSFORM_STAGE_MAP.keys())}"
            )

        with self._db.session() as sess:
            # Resolve stage_id from stage_name
            stage_id = sess.scalar(
                select(ProcessingStage.stage_id).where(
                    ProcessingStage.stage_name == stage_name
                )
            )
            if not stage_id:
                raise ValueError(f"Stage '{stage_name}' not found in processing_stages")

            # Retrieve checksums from products
            parent = sess.get(DataProduct, parent_product_id)
            child  = sess.get(DataProduct, child_product_id)
            if not parent:
                raise ValueError(f"parent_product_id={parent_product_id} not found")
            if not child:
                raise ValueError(f"child_product_id={child_product_id} not found")

            lineage = DataLineage(
                parent_product_id    = parent_product_id,
                child_product_id     = child_product_id,
                transformation_type  = transformation_type,
                stage_id             = stage_id,
                job_id               = job_id,
                transformation_params = params or {},
                input_checksum       = parent.data_hash_sha256,
                output_checksum      = child.data_hash_sha256,
            )
            sess.add(lineage)
            sess.flush()
            lineage_id = lineage.lineage_id

        logger.info("[LINEAGE] lineage_id=%d parent=%d →[%s]→ child=%d",
                    lineage_id, parent_product_id, transformation_type, child_product_id)
        return lineage_id

    def record_full_pipeline(
        self,
        scene_id: int,
        raw_product_id: int,
        bronze_product_id: int,
        silver_product_id: int,
        gold_product_id: int,
        job_ids: dict[str, int],
        crop_params: dict | None = None,
        lee_params: dict | None = None,
        cog_params: dict | None = None,
    ) -> list[int]:
        """
        Convenience method: record all three lineage links for a complete
        pipeline run (RAW → BRONZE → SILVER → GOLD).

        Args:
            scene_id          : Parent scene (for logging)
            raw_product_id    : RAW tier product (original download)
            bronze_product_id : BRONZE tier product (after Module 2 crop)
            silver_product_id : SILVER tier product (after Module 3 Lee filter)
            gold_product_id   : GOLD tier product (after Module 4 COG export)
            job_ids           : Dict mapping stage_name → job_id
                                e.g. {'CROP': 10, 'LEE_FILTER': 11, 'COG_EXPORT': 12}
            crop_params       : Module 2 params (bbox, resolution, etc.)
            lee_params        : Module 3 params (window_size, looks, etc.)
            cog_params        : Module 4 params (compression, blocksize, etc.)

        Returns:
            List of 3 lineage_ids [crop_lineage, lee_lineage, cog_lineage]
        """
        lineage_ids = []

        # RAW → BRONZE (crop)
        lid = self.record_transformation(
            parent_product_id  = raw_product_id,
            child_product_id   = bronze_product_id,
            transformation_type = "CROP",
            job_id             = job_ids["CROP"],
            params             = crop_params or {"region": "Jabodetabek"},
        )
        lineage_ids.append(lid)

        # BRONZE → SILVER (Lee filter)
        lid = self.record_transformation(
            parent_product_id  = bronze_product_id,
            child_product_id   = silver_product_id,
            transformation_type = "LEE_FILTER",
            job_id             = job_ids["LEE_FILTER"],
            params             = lee_params or {"window_size": 7, "looks": 1},
        )
        lineage_ids.append(lid)

        # SILVER → GOLD (COG export)
        lid = self.record_transformation(
            parent_product_id  = silver_product_id,
            child_product_id   = gold_product_id,
            transformation_type = "COG_EXPORT",
            job_id             = job_ids["COG_EXPORT"],
            params             = cog_params or {"compression": "LZW", "blocksize": 512},
        )
        lineage_ids.append(lid)

        logger.info("[LINEAGE] Full pipeline recorded for scene=%d: %s",
                    scene_id, " → ".join(map(str, lineage_ids)))
        return lineage_ids

    # ------------------------------------------------------------------
    # PROVENANCE QUERIES
    # ------------------------------------------------------------------

    def get_lineage_chain(self, product_id: int, direction: str = "ancestors") -> list[dict]:
        """
        Recursively trace the transformation chain for a product.

        Args:
            product_id : Starting product
            direction  : 'ancestors' (trace back to source) or
                         'descendants' (trace forward to derived products)

        Returns:
            Ordered list of lineage step dicts:
                lineage_id, parent_product_id, child_product_id,
                transformation_type, stage_name, job_id,
                transformation_params, input_checksum, output_checksum, created_at
        """
        chain = []
        visited = set()

        with self._db.session() as sess:
            self._trace_recursive(sess, product_id, direction, chain, visited)

        logger.info("[LINEAGE] Chain for product=%d direction=%s → %d steps",
                    product_id, direction, len(chain))
        return chain

    def _trace_recursive(
        self,
        sess,
        product_id: int,
        direction: str,
        chain: list,
        visited: set,
    ) -> None:
        """Internal recursive lineage traversal."""
        if product_id in visited:
            return
        visited.add(product_id)

        if direction == "ancestors":
            # Find the lineage record where this product is the CHILD
            lineages = sess.scalars(
                select(DataLineage).where(DataLineage.child_product_id == product_id)
            ).all()
            for lin in lineages:
                chain.insert(0, self._lineage_to_dict(lin, sess))
                self._trace_recursive(sess, lin.parent_product_id, direction, chain, visited)
        else:
            # Find lineage records where this product is the PARENT
            lineages = sess.scalars(
                select(DataLineage).where(DataLineage.parent_product_id == product_id)
            ).all()
            for lin in lineages:
                chain.append(self._lineage_to_dict(lin, sess))
                self._trace_recursive(sess, lin.child_product_id, direction, chain, visited)

    def _lineage_to_dict(self, lin: DataLineage, sess=None) -> dict:
        """Serialize a DataLineage ORM object to dict.

        `source`/`product_tier` tiap ujung langkah ikut dibawa: sejak layout
        tier-source, satu dataset punya rantai paralel per sensor (S1
        RAW->BRONZE->SILVER->GOLD, MODIS/GPM SILVER->GOLD), dan tanpa kolom
        ini pembaca rantai tidak bisa tahu langkah mana milik sensor mana
        tanpa menarik tiap product satu per satu."""
        out = {
            "lineage_id":             lin.lineage_id,
            "parent_product_id":      lin.parent_product_id,
            "child_product_id":       lin.child_product_id,
            "transformation_type":    lin.transformation_type,
            "stage_id":               lin.stage_id,
            "job_id":                 lin.job_id,
            "transformation_params":  lin.transformation_params,
            "input_checksum":         lin.input_checksum,
            "output_checksum":        lin.output_checksum,
            "created_at":             lin.created_at.isoformat(),
            "source":                 None,
            "parent_tier":            None,
            "child_tier":             None,
        }
        if sess is not None:
            parent = sess.get(DataProduct, lin.parent_product_id)
            child = sess.get(DataProduct, lin.child_product_id)
            if child is not None:
                out["source"] = child.source
                out["child_tier"] = child.product_tier.value
            if parent is not None:
                out["parent_tier"] = parent.product_tier.value
                if out["source"] is None:
                    out["source"] = parent.source
        return out

    def verify_integrity(self, product_id: int, file_path: str) -> dict:
        """
        Verify a file's current SHA-256 against the stored hash.

        Args:
            product_id : DataProduct to verify
            file_path  : Current file location on disk

        Returns:
            dict with keys: product_id, stored_hash, computed_hash,
                            integrity_ok (bool), file_size_mb
        """
        with self._db.session() as sess:
            product = sess.get(DataProduct, product_id)
            if not product:
                raise ValueError(f"product_id={product_id} not found")
            stored_hash = product.data_hash_sha256

        computed_hash = self.compute_sha256(file_path)
        file_size_mb  = os.path.getsize(file_path) / (1024 ** 2)
        integrity_ok  = stored_hash == computed_hash

        result = {
            "product_id":   product_id,
            "file_path":    file_path,
            "stored_hash":  stored_hash,
            "computed_hash": computed_hash,
            "integrity_ok": integrity_ok,
            "file_size_mb": round(file_size_mb, 3),
        }

        if integrity_ok:
            logger.info("[INTEGRITY] product_id=%d ✓ hash match", product_id)
        else:
            logger.error("[INTEGRITY] product_id=%d ✗ HASH MISMATCH stored=%s computed=%s",
                         product_id, stored_hash[:12], computed_hash[:12])

        return result
