# tests/test_fusion_product_identity.py
"""
Identitas produk FUSION adalah BERKASNYA, bukan scene primary-nya.

Regresi nyata, terukur di dua dataset sekaligus:

    dataset 26  fusion_20251201_cooccurrence_processed.h5
                dua baris is_latest, satu mengaku 32040x103630 padahal
                berkasnya 31922x103248
    dataset 25  fusion_20250204_hybrid_processed.h5
                dua baris is_latest, tidak kelihatan karena bentuknya sama

Mekanismenya: nama berkas fusion cuma memuat tanggal
(`fusion_{tanggal}_{strategi}_{level}.h5`), tapi produknya didaftarkan atas
nama scene "primary" — anggota pertama tanggal itu setelah diurutkan per pid.
Kalau job terputus lalu dilanjutkan, himpunan anggota yang sudah selesai
berbeda, primary-nya berganti, dan dedup `insert_data_product` yang berkunci
pada scene_id tidak mengenali keduanya sebagai artefak yang sama. Berkasnya
tertimpa, barisnya menumpuk.

Yang dijaga: mendaftarkan stack kedua ke path yang sama harus memadamkan
baris pertama, apa pun scene-nya.
"""
from __future__ import annotations

import os
import uuid

import pytest

from etl import tier_names as tn


@pytest.fixture
def meta():
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("tanpa DATABASE_URL: tes ini butuh database sungguhan")
    from etl.database_client import DatabaseClient
    from etl.metadata_manager import MetadataManager

    return MetadataManager(DatabaseClient(url))


@pytest.fixture
def anchors(meta):
    """Dataset, job, dan dua scene yang SUDAH ADA.

    dataset_id, job_id, dan scene_id ketiganya berkunci asing, jadi tes tidak
    bisa mengarang nilainya. Yang dibuat unik justru band_name dan file_path —
    keduanya masuk ke filter "tandai usang", jadi nilai yang tidak dipakai
    baris mana pun menjamin tes ini tidak bisa memadamkan produk sungguhan.
    """
    from etl.database_client import Dataset, ProcessingJob, SatelliteScene

    with meta._db.session() as sess:
        ds = sess.query(Dataset.dataset_id).limit(1).scalar()
        job = sess.query(ProcessingJob.job_id).limit(1).scalar()
        scenes = [r[0] for r in sess.query(SatelliteScene.scene_id).limit(2).all()]
    if ds is None or job is None or len(scenes) < 2:
        pytest.skip("database uji belum punya dataset/job/scene untuk ditumpangi")
    tag = uuid.uuid4().hex[:8]
    return {
        "dataset_id": ds,
        "job_id": job,
        "scenes": scenes,
        "band": f"UJI_{tag}",
        "path": f"data/_uji/{tag}/fusion_19990101_processed.h5",
    }


def _cleanup(meta, dataset_id, band):
    from etl.database_client import DataProduct

    with meta._db.session() as sess:
        sess.query(DataProduct).filter(
            DataProduct.dataset_id == dataset_id,
            DataProduct.band_name == band,
        ).delete(synchronize_session=False)
        sess.commit()


def _rows(meta, dataset_id, band):
    from etl.database_client import DataProduct

    with meta._db.session() as sess:
        return [
            (r.product_id, r.scene_id, r.is_latest)
            for r in sess.query(DataProduct).filter(
                DataProduct.dataset_id == dataset_id,
                DataProduct.band_name == band,
            ).all()
        ]


def _insert(meta, a, scene_id, **kw):
    return meta.insert_data_product(
        scene_id=scene_id, job_id=a["job_id"], dataset_id=a["dataset_id"],
        product_tier=tn.FUSED, source="FUSION", product_type="FUSION_H5",
        band_name=a["band"], file_path=a["path"],
        file_name="fusion_19990101_processed.h5",
        file_size_mb=1.0, data_hash_sha256="x" * 64,
        file_format="HDF5", rows=10, cols=20, processing_level="PROCESSED",
        **kw,
    )


class TestSupersedeByPath:
    def test_same_path_from_another_scene_supersedes(self, meta, anchors):
        """Inti perbaikannya: scene berbeda, berkas sama -> yang lama padam."""
        try:
            ids = [
                _insert(meta, anchors, sc, supersede_same_path=True)
                for sc in anchors["scenes"]
            ]
            latest = [r for r in _rows(meta, anchors["dataset_id"], anchors["band"])
                      if r[2]]
            assert len(latest) == 1, "hanya stack terbaru yang boleh is_latest"
            assert latest[0][0] == ids[-1]
        finally:
            _cleanup(meta, anchors["dataset_id"], anchors["band"])

    def test_without_the_flag_the_stale_row_survives(self, meta, anchors):
        """Bentuk bug-nya sebelum diperbaiki, dikunci sebagai pembanding:
        tanpa bendera itu kedua baris tetap is_latest, dan yang pertama
        terus mengklaim isi berkas yang sudah ditimpa."""
        try:
            for sc in anchors["scenes"]:
                _insert(meta, anchors, sc)
            latest = [r for r in _rows(meta, anchors["dataset_id"], anchors["band"])
                      if r[2]]
            assert len(latest) == 2
        finally:
            _cleanup(meta, anchors["dataset_id"], anchors["band"])

    def test_reregistering_the_same_scene_still_supersedes(self, meta, anchors):
        """Jalur lama tidak boleh rusak: scene yang sama didaftarkan ulang
        tetap memadamkan barisnya sendiri."""
        try:
            sc = anchors["scenes"][0]
            _insert(meta, anchors, sc, supersede_same_path=True)
            second = _insert(meta, anchors, sc, supersede_same_path=True)
            latest = [r for r in _rows(meta, anchors["dataset_id"], anchors["band"])
                      if r[2]]
            assert len(latest) == 1
            assert latest[0][0] == second
        finally:
            _cleanup(meta, anchors["dataset_id"], anchors["band"])
