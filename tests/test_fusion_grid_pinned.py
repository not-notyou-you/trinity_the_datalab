"""
Grid fusion dipaku sekali per dataset (etl/module9_fusion).

Regresi yang dijaga di sini nyata dan sudah pernah terjadi: dataset 26 (JAWA)
menghasilkan dua stack pada grid yang tidak berhimpit --

    fusion_20251201   32040 x 103630
    fusion_20251204   31922 x 103248

karena grid diturunkan ULANG tiap jalan dari "raster S1 pertama yang filenya
masih ada". Tiap scene S1 direproyeksi dengan calculate_default_transform,
jadi tiap scene punya ukuran piksel sendiri (dataset 26 punya 14 bentuk);
begitu raster acuan berganti -- berkas hilang, is_latest bergeser -- gridnya
ikut berganti, dan deret waktunya tidak bisa ditumpuk lagi.

Yang diuji: sesudah dipaku, JAWABANNYA TIDAK BERUBAH walau acuannya berubah.
"""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from etl import module9_fusion as m9

AOI = (106.4, -6.7, 107.2, -5.9)
DATASET_ID = 26


class _FakeSession:
    """Sesi seadanya: menyimpan satu nilai fusion_grid di dict milik klien."""

    def __init__(self, store: dict) -> None:
        self._store = store

    def scalar(self, _stmt):
        return self._store.get("fusion_grid")

    def execute(self, _stmt):
        # update() di sini selalu "pin kalau masih kosong", jadi cukup tiru
        # syarat itu tanpa membedah statement SQLAlchemy-nya.
        if self._store.get("fusion_grid") is None:
            self._store["fusion_grid"] = self._store.pop("_pending", None)

    def commit(self):
        pass


class _FakeDB:
    def __init__(self) -> None:
        self.store: dict = {"fusion_grid": None}

    @contextmanager
    def session(self):
        yield _FakeSession(self.store)


@pytest.fixture
def db():
    return _FakeDB()


@pytest.fixture
def reference(monkeypatch):
    """Ganti-ganti raster S1 acuan, seperti yang terjadi di dunia nyata."""
    state = {"res": 9.0904e-05}

    def fake_reference(_db, _dataset_id, _tier):
        from rasterio.crs import CRS
        from rasterio.transform import from_origin

        res = state["res"]
        return from_origin(106.4, -5.9, res, res), CRS.from_epsg(4326), (10, 10)

    monkeypatch.setattr(m9, "_dataset_s1_reference_grid", fake_reference)
    return state


@pytest.fixture(autouse=True)
def capture_pin(monkeypatch):
    """Sambungkan _pin_grid ke penyimpanan palsu."""
    real_pin = m9._pin_grid

    def pin(db, dataset_id, transform, crs, shape, origin):
        if db is None:
            return
        db.store["_pending"] = {
            "transform": [transform.a, transform.b, transform.c,
                          transform.d, transform.e, transform.f],
            "height": int(shape[0]), "width": int(shape[1]),
            "crs": str(crs), "origin": origin,
        }
        with db.session() as sess:
            sess.execute(None)
            sess.commit()

    monkeypatch.setattr(m9, "_pin_grid", pin)
    return real_pin


def _grid(db):
    return m9._dataset_fusion_grid(db, DATASET_ID, "COG", AOI)


class TestPinning:
    def test_grid_is_stored_on_first_run(self, db, reference):
        assert db.store["fusion_grid"] is None
        _grid(db)
        assert db.store["fusion_grid"] is not None

    def test_same_answer_when_reference_raster_changes(self, db, reference):
        """Inti perbaikannya: acuan boleh berubah, grid tidak boleh ikut."""
        first = _grid(db)

        # Raster acuan berganti ke resolusi lain -- persis skenario yang
        # memecah dataset 26 menjadi dua grid.
        reference["res"] = 8.9827e-05
        second = _grid(db)

        assert second[2] == first[2], "bentuk stack berubah setelah dipaku"
        assert tuple(second[0])[:6] == pytest.approx(tuple(first[0])[:6])

    def test_shape_still_spans_the_whole_aoi(self, db, reference):
        transform, _, (height, width) = _grid(db)
        west, north = transform * (0, 0)
        east, south = transform * (width, height)
        assert (west, north) == pytest.approx((AOI[0], AOI[3]))
        assert east == pytest.approx(AOI[2], abs=1e-4)
        assert south == pytest.approx(AOI[1], abs=1e-4)

    def test_without_database_nothing_is_pinned(self, reference):
        """Pemanggil tanpa database (uji rumus grid) tetap harus jalan."""
        transform, _, shape = m9._dataset_fusion_grid(None, DATASET_ID, "COG", AOI)
        assert shape[0] > 0 and shape[1] > 0

    def test_corrupt_pin_is_recomputed_not_fatal(self, db, reference):
        db.store["fusion_grid"] = {"transform": "bukan angka"}
        transform, _, shape = _grid(db)
        assert shape[0] > 0 and shape[1] > 0


class TestAudit:
    """audit_dataset_grids: membuat percabangan grid TERLIHAT.

    Memaku grid mencegah percabangan baru, tapi stack yang sudah terlanjur
    lahir di grid lain tetap diam. Dataset 26 (JAWA) berjalan berbulan-bulan
    dengan dua stack yang tidak berhimpit tanpa satu pun peringatan.
    """

    def _write_stack(self, root, name, height, width):
        import h5py

        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with h5py.File(path, "w") as h:
            h.attrs["height"] = height
            h.attrs["width"] = width
        return path

    def test_matching_stacks_report_clean(self, tmp_path, db, reference, monkeypatch):
        from etl import folder_manager as fm

        monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
        _grid(db)  # paku dulu
        _t, _c, shape = m9._load_pinned_grid(db, DATASET_ID)

        root = fm.get_dataset_root(DATASET_ID, "Audit Test")
        self._write_stack(root, "fusion/a.h5", shape[0], shape[1])
        self._write_stack(root, "fusion/b.h5", shape[0], shape[1])

        audit = m9.audit_dataset_grids(db, DATASET_ID, "Audit Test")
        assert len(audit["matching"]) == 2
        assert audit["mismatched"] == []

    def test_divergent_stack_is_reported(self, tmp_path, db, reference, monkeypatch):
        """Persis bentuk kerusakan dataset 26."""
        from etl import folder_manager as fm

        monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
        _grid(db)
        _t, _c, shape = m9._load_pinned_grid(db, DATASET_ID)

        root = fm.get_dataset_root(DATASET_ID, "Audit Test")
        self._write_stack(root, "fusion/baik.h5", shape[0], shape[1])
        self._write_stack(root, "fusion/menyimpang.h5", shape[0] + 118, shape[1] + 382)

        audit = m9.audit_dataset_grids(db, DATASET_ID, "Audit Test")
        assert len(audit["matching"]) == 1
        assert len(audit["mismatched"]) == 1
        assert audit["mismatched"][0]["file"] == "menyimpang.h5"

    def test_no_pinned_grid_means_nothing_to_audit(self, tmp_path, db, monkeypatch):
        from etl import folder_manager as fm

        monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
        audit = m9.audit_dataset_grids(db, DATASET_ID, "Audit Test")
        assert audit["pinned"] is None
        assert audit["mismatched"] == []

    def test_corrupt_h5_does_not_break_the_audit(
        self, tmp_path, db, reference, monkeypatch
    ):
        from etl import folder_manager as fm

        monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
        _grid(db)
        root = fm.get_dataset_root(DATASET_ID, "Audit Test")
        root.mkdir(parents=True, exist_ok=True)
        (root / "rusak.h5").write_bytes(b"bukan hdf5 sama sekali")

        audit = m9.audit_dataset_grids(db, DATASET_ID, "Audit Test")
        assert audit["mismatched"] == []
