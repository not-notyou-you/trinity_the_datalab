# etl/metadata_manager.py
from __future__ import annotations
import logging
import socket
from datetime import datetime, timedelta, timezone
from typing import Any
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from etl.database_client import (
    AlertEvent,
    AlertEventTypeEnum,
    AlertSeverityEnum,
    DataProduct,
    DatabaseClient,
    JobStatusEnum,
    NasaScene,
    ProcessingJob,
    ProcessingStage,
    ProductSourceEnum,
    ProductTierEnum,
    QualityMetric,
    SatelliteScene,
    StorageLocationEnum,
)

logger = logging.getLogger(__name__)


class MetadataManager:
    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    def insert_processing_job(
        self,
        scene_id: int,
        stage_name: str,
        attempt_number: int = 1,
        parameters: dict | None = None,
    ) -> int:
        with self._db.session() as sess:
            stage = sess.scalar(
                select(ProcessingStage).where(ProcessingStage.stage_name == stage_name)
            )
            if not stage:
                raise ValueError(f"Unknown stage_name: '{stage_name}'. Check processing_stages table.")

            existing_job_id = sess.scalar(
                select(ProcessingJob.job_id).where(
                    ProcessingJob.scene_id == scene_id,
                    ProcessingJob.stage_id == stage.stage_id,
                    ProcessingJob.attempt_number == attempt_number,
                )
            )
            if existing_job_id:
                logger.warning(
                    "[JOB] Duplicate job skipped: scene=%d stage=%s attempt=%d (job_id=%d)",
                    scene_id, stage_name, attempt_number, existing_job_id,
                )
                return existing_job_id

            job = ProcessingJob(
                scene_id=scene_id,
                stage_id=stage.stage_id,
                attempt_number=attempt_number,
                status=JobStatusEnum.QUEUED,
                worker_hostname=socket.gethostname(),
                parameters_json=parameters or {},
            )
            sess.add(job)
            sess.flush()
            job_id = job.job_id

        logger.info("[JOB] Created job_id=%d scene=%d stage=%s attempt=%d",
                    job_id, scene_id, stage_name, attempt_number)
        return job_id

    def start_job(self, job_id: int) -> None:
        with self._db.session() as sess:
            job = sess.get(ProcessingJob, job_id)
            if not job:
                raise ValueError(f"job_id={job_id} not found")
            job.status = JobStatusEnum.RUNNING
            job.started_at = datetime.now(tz=timezone.utc)
        logger.info("[JOB] job_id=%d -> RUNNING", job_id)

    def complete_job(
        self,
        job_id: int,
        status: JobStatusEnum = JobStatusEnum.SUCCESS,
        output_size_mb: float | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        cpu_usage_percent: float | None = None,
        memory_usage_mb: float | None = None,
    ) -> None:
        with self._db.session() as sess:
            job = sess.get(ProcessingJob, job_id)
            if not job:
                raise ValueError(f"job_id={job_id} not found")
            job.status = status
            job.completed_at = datetime.now(tz=timezone.utc)
            job.output_size_mb = output_size_mb
            job.error_code = error_code
            job.error_message = error_message
            if cpu_usage_percent is not None:
                job.cpu_usage_percent = cpu_usage_percent
            if memory_usage_mb is not None:
                job.memory_usage_mb = memory_usage_mb
        logger.info("[JOB] job_id=%d -> %s", job_id, status.value)

    def insert_satellite_scene(
        self,
        product_identifier: str,
        acquisition_datetime: datetime,
        region_id: int,
        bbox_wkt: str,
        orbit_direction: str = "ASCENDING",
        polarization_vv: bool = True,
        polarization_vh: bool = True,
        orbit_number: int | None = None,
        relative_orbit: int | None = None,
        cloud_cover_percent: float | None = None,
        incidence_angle_near: float | None = None,
        incidence_angle_far: float | None = None,
        resolution_m: int = 10,
        raw_file_path: str | None = None,
        raw_file_size_mb: float | None = None,
        download_url: str | None = None,
        checksum_md5: str | None = None,
        instrument_mode: str = "IW",
    ) -> int:
        with self._db.session() as sess:
            existing = sess.scalar(
                select(SatelliteScene.scene_id).where(
                    SatelliteScene.product_identifier == product_identifier
                )
            )
            if existing:
                logger.warning("[SCENE] Duplicate scene skipped: %s (scene_id=%d)",
                               product_identifier, existing)
                return existing

            scene = SatelliteScene(
                product_identifier=product_identifier,
                acquisition_datetime=acquisition_datetime,
                region_id=region_id,
                bbox=f"SRID=4326;{bbox_wkt}",
                orbit_direction=orbit_direction,
                polarization_vv=polarization_vv,
                polarization_vh=polarization_vh,
                orbit_number=orbit_number,
                relative_orbit=relative_orbit,
                cloud_cover_percent=cloud_cover_percent,
                incidence_angle_near=incidence_angle_near,
                incidence_angle_far=incidence_angle_far,
                resolution_m=resolution_m,
                raw_file_path=raw_file_path,
                raw_file_size_mb=raw_file_size_mb,
                download_url=download_url,
                checksum_md5=checksum_md5,
                instrument_mode=instrument_mode,
                is_available=True,
            )
            sess.add(scene)
            sess.flush()
            scene_id = scene.scene_id

        logger.info("[SCENE] Registered scene_id=%d pid=%s acq=%s",
                    scene_id, product_identifier, acquisition_datetime.isoformat())
        return scene_id

    def insert_data_product(
        self,
        scene_id: int,
        job_id: int,
        product_tier: str,
        source: str,
        product_type: str,
        band_name: str,
        file_path: str,
        file_name: str,
        file_size_mb: float,
        data_hash_sha256: str,
        file_format: str = "TIFF",
        crs: str = "EPSG:4326",
        pixel_size_m: float | None = None,
        nodata_value: float | None = None,
        rows: int | None = None,
        cols: int | None = None,
        band_count: int = 1,
        storage_location: str = "LOCAL",
        dataset_id: int | None = None,
    ) -> int:
        with self._db.session() as sess:
            sess.query(DataProduct).filter(
                and_(
                    DataProduct.scene_id == scene_id,
                    DataProduct.band_name == band_name,
                    DataProduct.product_tier == product_tier,
                    DataProduct.dataset_id == dataset_id,
                    DataProduct.is_latest == True,
                )
            ).update({"is_latest": False})

            product = DataProduct(
                scene_id=scene_id,
                job_id=job_id,
                dataset_id=dataset_id,
                product_tier=ProductTierEnum(product_tier),
                source=ProductSourceEnum(source).value,
                product_type=product_type,
                band_name=band_name,
                file_name=file_name,
                file_path=file_path,
                file_size_mb=file_size_mb,
                data_hash_sha256=data_hash_sha256,
                file_format=file_format,
                crs=crs,
                pixel_size_m=pixel_size_m,
                nodata_value=nodata_value,
                rows=rows,
                cols=cols,
                band_count=band_count,
                storage_location=StorageLocationEnum(storage_location),
                is_valid=True,
                is_latest=True,
            )
            sess.add(product)
            sess.flush()
            product_id = product.product_id

        logger.info("[PRODUCT] Registered product_id=%d scene=%d band=%s tier=%s source=%s dataset=%s file=%s",
                    product_id, scene_id, band_name, product_tier, source, dataset_id, file_name)
        return product_id

    def mark_products_invalid_by_paths(self, dataset_id: int | None, paths: list[str]) -> int:
        """Tandai is_valid=False untuk produk yang file-nya baru dihapus,
        dicocokkan lewat file_path.

        Dipakai cleanup tier parsial. mark_products_invalid() di bawah
        mencocokkan lewat scene_id, yang tidak cukup untuk produk aux
        MODIS/GPM: file-nya ditulis saat memproses satu scene Sentinel-1,
        tapi barisnya menempel ke SatelliteScene placeholder per-tanggal
        (module9_fusion._resolve_aux_scene), bukan ke scene S1 itu."""
        if not paths:
            return 0
        with self._db.session() as sess:
            stmt = select(DataProduct).where(DataProduct.file_path.in_(paths))
            if dataset_id is not None:
                stmt = stmt.where(DataProduct.dataset_id == dataset_id)
            rows = sess.scalars(stmt).all()
            for r in rows:
                r.is_valid = False
            count = len(rows)
        logger.info("[PRODUCT] %d produk dataset=%s ditandai is_valid=False (file dihapus)",
                    count, dataset_id)
        return count

    def mark_products_invalid(self, scene_id: int, dataset_id: int | None, tier: str) -> None:
        with self._db.session() as sess:
            stmt = select(DataProduct).where(
                DataProduct.scene_id == scene_id,
                DataProduct.product_tier == ProductTierEnum(tier),
            )
            if dataset_id is not None:
                stmt = stmt.where(DataProduct.dataset_id == dataset_id)
            rows = sess.scalars(stmt).all()
            for r in rows:
                r.is_valid = False
        logger.info("[PRODUCT] scene=%d tier=%s dataset=%s ditandai is_valid=False (file dihapus)",
                    scene_id, tier, dataset_id)

    def insert_quality_metrics(
        self,
        scene_id: int,
        product_id: int,
        band_name: str,
        total_pixels: int,
        valid_pixels: int,
        nodata_pixels: int,
        quality_score: float,
        backscatter_mean_db: float | None = None,
        backscatter_std_db: float | None = None,
        backscatter_min_db: float | None = None,
        backscatter_max_db: float | None = None,
        cloud_threshold_percent: float = 20.0,
        radiometric_consistency: bool | None = None,
        speckle_index: float | None = None,
        quality_flag: str = "UNCHECKED",
        notes: str | None = None,
    ) -> int:
        with self._db.session() as sess:
            metric = QualityMetric(
                scene_id=scene_id,
                product_id=product_id,
                band_name=band_name,
                total_pixels=total_pixels,
                valid_pixels=valid_pixels,
                nodata_pixels=nodata_pixels,
                quality_score=round(quality_score, 2),
                backscatter_mean_db=backscatter_mean_db,
                backscatter_std_db=backscatter_std_db,
                backscatter_min_db=backscatter_min_db,
                backscatter_max_db=backscatter_max_db,
                cloud_threshold_percent=cloud_threshold_percent,
                radiometric_consistency=radiometric_consistency,
                speckle_index=speckle_index,
                quality_flag=quality_flag,
                notes=notes,
            )
            sess.add(metric)
            sess.flush()
            metric_id = metric.metric_id

        logger.info("[QUALITY] metric_id=%d scene=%d band=%s score=%.2f flag=%s",
                    metric_id, scene_id, band_name, quality_score, quality_flag)

        if quality_flag == "FAIL":
            self.insert_alert_event(
                event_type=AlertEventTypeEnum.QUALITY_WARNING,
                severity=AlertSeverityEnum.WARNING,
                title=f"Quality FAIL: scene={scene_id} band={band_name}",
                message=(f"Quality score {quality_score:.1f}/100 below threshold. "
                         f"NoData={nodata_pixels}/{total_pixels} pixels."),
                scene_id=scene_id,
                metadata={"quality_score": quality_score, "band": band_name,
                          "product_id": product_id},
            )

        return metric_id

    def insert_alert_event(
        self,
        event_type: AlertEventTypeEnum,
        title: str,
        message: str,
        severity: AlertSeverityEnum = AlertSeverityEnum.INFO,
        scene_id: int | None = None,
        job_id: int | None = None,
        product_id: int | None = None,
        metadata: dict | None = None,
    ) -> int:
        with self._db.session() as sess:
            alert = AlertEvent(
                event_type=event_type,
                severity=severity,
                scene_id=scene_id,
                job_id=job_id,
                product_id=product_id,
                title=title,
                message=message,
                metadata_json=metadata or {},
                is_resolved=False,
            )
            sess.add(alert)
            sess.flush()
            alert_id = alert.alert_id

        logger.log(
            logging.WARNING if severity != AlertSeverityEnum.INFO else logging.INFO,
            "[ALERT] alert_id=%d [%s] %s: %s", alert_id, severity.value, title, message
        )
        return alert_id

    def query_latest_scenes(
        self,
        n_days: int = 30,
        region_id: int | None = None,
        only_unprocessed: bool = False,
    ) -> list[dict]:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=n_days)

        with self._db.session() as sess:
            stmt = (
                select(
                    SatelliteScene.scene_id,
                    SatelliteScene.product_identifier,
                    SatelliteScene.acquisition_datetime,
                    SatelliteScene.orbit_direction,
                    SatelliteScene.polarization_vv,
                    SatelliteScene.polarization_vh,
                )
                .where(
                    and_(
                        SatelliteScene.acquisition_datetime >= cutoff,
                        SatelliteScene.is_available == True,
                    )
                )
                .order_by(SatelliteScene.acquisition_datetime.desc())
            )

            if region_id:
                stmt = stmt.where(SatelliteScene.region_id == region_id)

            rows = sess.execute(stmt).fetchall()

            results = []
            for row in rows:
                # Deliverable terakhir pipeline sekarang tier FUSION, bukan
                # GOLD (GOLD kini produk per-source, bukan ujung pipeline).
                has_final = sess.scalar(
                    select(func.count(DataProduct.product_id)).where(
                        and_(
                            DataProduct.scene_id == row.scene_id,
                            DataProduct.product_tier == ProductTierEnum.FUSION,
                            DataProduct.is_latest == True,
                            DataProduct.is_valid == True,
                        )
                    )
                ) > 0

                if only_unprocessed and has_final:
                    continue

                results.append({
                    "scene_id": row.scene_id,
                    "product_identifier": row.product_identifier,
                    "acquisition_datetime": row.acquisition_datetime.isoformat(),
                    "orbit_direction": row.orbit_direction,
                    "polarization_vv": row.polarization_vv,
                    "polarization_vh": row.polarization_vh,
                    "has_gold_product": has_gold,
                })

        logger.info("[QUERY] latest_scenes n_days=%d region=%s results=%d",
                    n_days, region_id, len(results))
        return results

    def get_scene_by_id(self, scene_id: int) -> dict | None:
        with self._db.session() as sess:
            scene = sess.get(SatelliteScene, scene_id)
            if not scene:
                return None
            return {
                "scene_id": scene.scene_id,
                "scene_uuid": str(scene.scene_uuid),
                "product_identifier": scene.product_identifier,
                "platform": scene.platform,
                "instrument_mode": scene.instrument_mode,
                "polarization_vv": scene.polarization_vv,
                "polarization_vh": scene.polarization_vh,
                "acquisition_datetime": scene.acquisition_datetime.isoformat(),
                "orbit_direction": scene.orbit_direction,
                "orbit_number": scene.orbit_number,
                "relative_orbit": scene.relative_orbit,
                "cloud_cover_percent": float(scene.cloud_cover_percent) if scene.cloud_cover_percent else None,
                "resolution_m": scene.resolution_m,
                "region_id": scene.region_id,
                "raw_file_path": scene.raw_file_path,
                "raw_file_size_mb": float(scene.raw_file_size_mb) if scene.raw_file_size_mb else None,
                "is_available": scene.is_available,
                "created_at": scene.created_at.isoformat(),
            }

    def get_quality_by_scene(self, scene_id: int) -> list[dict]:
        with self._db.session() as sess:
            metrics = sess.scalars(
                select(QualityMetric).where(QualityMetric.scene_id == scene_id)
            ).all()

            return [
                {
                    "metric_id": m.metric_id,
                    "band_name": m.band_name,
                    "quality_score": float(m.quality_score),
                    "quality_flag": m.quality_flag,
                    "total_pixels": m.total_pixels,
                    "valid_pixels": m.valid_pixels,
                    "nodata_pixels": m.nodata_pixels,
                    "backscatter_mean_db": float(m.backscatter_mean_db) if m.backscatter_mean_db else None,
                    "backscatter_std_db": float(m.backscatter_std_db) if m.backscatter_std_db else None,
                    "radiometric_consistency": m.radiometric_consistency,
                    "speckle_index": float(m.speckle_index) if m.speckle_index else None,
                    "assessed_at": m.assessed_at.isoformat(),
                }
                for m in metrics
            ]

    def get_products_by_scene(self, scene_id: int, tier: str | None = None,
                               latest_only: bool = True) -> list[dict]:
        with self._db.session() as sess:
            stmt = select(DataProduct).where(DataProduct.scene_id == scene_id)

            if tier:
                stmt = stmt.where(DataProduct.product_tier == ProductTierEnum(tier))
            if latest_only:
                stmt = stmt.where(DataProduct.is_latest == True)

            products = sess.scalars(stmt).all()

            return [
                {
                    "product_id": p.product_id,
                    "product_uuid": str(p.product_uuid),
                    "job_id": p.job_id,
                    "dataset_id": p.dataset_id,
                    "product_tier": p.product_tier.value,
                    "product_type": p.product_type,
                    "band_name": p.band_name,
                    "file_path": p.file_path,
                    "file_name": p.file_name,
                    "file_size_mb": float(p.file_size_mb),
                    "file_format": p.file_format,
                    "data_hash_sha256": p.data_hash_sha256,
                    "crs": p.crs,
                    "rows": p.rows,
                    "cols": p.cols,
                    "is_valid": p.is_valid,
                    "is_latest": p.is_latest,
                    "created_at": p.created_at.isoformat(),
                }
                for p in products
            ]

    def get_scene_by_pid(self, product_identifier: str) -> dict | None:
        with self._db.session() as sess:
            scene = sess.scalar(
                select(SatelliteScene).where(
                    SatelliteScene.product_identifier == product_identifier
                )
            )
            if not scene:
                return None

            has_gold = sess.scalar(
                select(func.count(DataProduct.product_id)).where(
                    and_(
                        DataProduct.scene_id == scene.scene_id,
                        DataProduct.product_tier == ProductTierEnum.GOLD,
                        DataProduct.is_latest == True,
                        DataProduct.is_valid == True,
                    )
                )
            ) > 0

            return {
                "scene_id": scene.scene_id,
                "product_identifier": scene.product_identifier,
                "acquisition_datetime": scene.acquisition_datetime.isoformat(),
                "is_available": scene.is_available,
                "has_gold": has_gold,
            }

    def get_pipeline_status(self, scene_id: int) -> list[dict]:
        with self._db.session() as sess:
            jobs = sess.scalars(
                select(ProcessingJob)
                .join(ProcessingStage)
                .where(ProcessingJob.scene_id == scene_id)
                .order_by(ProcessingStage.stage_order)
            ).all()

            return [
                {
                    "job_id": j.job_id,
                    "stage_name": j.stage.stage_name,
                    "stage_order": j.stage.stage_order,
                    "attempt_number": j.attempt_number,
                    "status": j.status.value,
                    "queued_at": j.queued_at.isoformat(),
                    "started_at": j.started_at.isoformat() if j.started_at else None,
                    "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                    "error_message": j.error_message,
                }
                for j in jobs
            ]

    def insert_nasa_scene(
        self,
        source: str,
        tile_id: str,
        product_short_name: str,
        acquisition_date,
        region_id: int,
        raw_file_path: str | None = None,
        download_url: str | None = None,
    ) -> int:
        with self._db.session() as sess:
            existing = sess.scalar(
                select(NasaScene.nasa_scene_id).where(
                    NasaScene.source == source,
                    NasaScene.tile_id == tile_id,
                    NasaScene.product_short_name == product_short_name,
                    NasaScene.acquisition_date == acquisition_date,
                )
            )
            if existing:
                return existing
            scene = NasaScene(
                source=source,
                tile_id=tile_id,
                product_short_name=product_short_name,
                acquisition_date=acquisition_date,
                region_id=region_id,
                raw_file_path=raw_file_path,
                download_url=download_url,
                is_available=True,
            )
            sess.add(scene)
            sess.flush()
            nasa_scene_id = scene.nasa_scene_id
        logger.info("[NASA] Registered nasa_scene_id=%d source=%s tile=%s date=%s",
                    nasa_scene_id, source, tile_id, acquisition_date)
        return nasa_scene_id

    def get_nasa_scene(
        self,
        source: str,
        tile_id: str,
        product_short_name: str,
        acquisition_date,
    ) -> int | None:
        with self._db.session() as sess:
            return sess.scalar(
                select(NasaScene.nasa_scene_id).where(
                    NasaScene.source == source,
                    NasaScene.tile_id == tile_id,
                    NasaScene.product_short_name == product_short_name,
                    NasaScene.acquisition_date == acquisition_date,
                )
            )