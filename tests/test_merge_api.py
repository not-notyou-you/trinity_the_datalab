"""Tes endpoint /api/merge.

Yang dijaga di sini terutama adalah pagar izinnya: penggabungan menulis berkas
besar dan lama, jadi tidak boleh ada jalur yang menjalankannya tanpa pemanggil
menyebut dengan tegas apa yang digabung.
"""
from __future__ import annotations

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from tests.test_dataset_merge import RES, WEST0, NORTH0, write_stack


@pytest.fixture
def merge_env(tmp_path, monkeypatch):
    """Dua dataset dengan stack sejajar di tanggal yang sama."""
    from etl import folder_manager as fm
    from api.routes import merge as merge_route

    roots = {}
    for did, name in ((27, "JAWA_A"), (28, "JAWA_B")):
        root = tmp_path / "datasets" / f"{did}_{name}"
        (root / "fusion").mkdir(parents=True)
        roots[did] = root

    write_stack(roots[27] / "fusion" / "fusion_20251201_x.h5",
                west=WEST0, north=NORTH0, height=10, width=10,
                layers=("s1_vv",), fill=1.0)
    write_stack(roots[28] / "fusion" / "fusion_20251201_x.h5",
                west=WEST0 + 10 * RES, north=NORTH0, height=10, width=10,
                layers=("s1_vv",), fill=2.0)

    monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: roots[did])
    monkeypatch.setattr(merge_route, "_merged_root", lambda: tmp_path / "merged")
    monkeypatch.setattr(
        merge_route, "_all_datasets",
        lambda db: [{"dataset_id": 27, "name": "JAWA_A"},
                    {"dataset_id": 28, "name": "JAWA_B"}],
    )
    return tmp_path


@pytest.fixture
def client(merge_env):
    from api.main import app
    from api.deps import get_db

    app.dependency_overrides[get_db] = lambda: object()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_candidates_menemukan_tanggal_yang_bisa_digabung(client):
    r = client.get("/api/merge/candidates")
    assert r.status_code == 200
    body = r.json()
    assert body["mergeable_count"] == 1
    cand = body["candidates"][0]
    assert cand["date"] == "20251201"
    assert cand["mergeable"] is True
    assert cand["dataset_ids"] == [27, 28]
    assert cand["already_merged"] is False


def test_candidates_tidak_menulis_apa_pun(client, merge_env):
    client.get("/api/merge/candidates")
    assert not (merge_env / "merged").exists()


def test_run_menggabungkan_dan_tidak_mengubah_sumber(client, merge_env):
    src = merge_env / "datasets" / "27_JAWA_A" / "fusion" / "fusion_20251201_x.h5"
    before = src.read_bytes()

    r = client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27, 28]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "MERGED"
    assert body["output_shape"] == [10, 20]
    assert body["sources_untouched"] is True

    out = merge_env / "merged" / "merged_20251201.h5"
    with h5py.File(out, "r") as m:
        data = m["s1_vv"][:]
        assert np.all(data[:, :10] == 1.0)
        assert np.all(data[:, 10:] == 2.0)

    assert src.read_bytes() == before, "berkas sumber ikut berubah"


def test_run_menolak_tanpa_dataset_ids(client):
    r = client.post("/api/merge/run", json={"date": "20251201"})
    assert r.status_code == 400
    assert "minimal dua" in r.json()["detail"]


def test_run_menolak_satu_dataset(client):
    r = client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27]})
    assert r.status_code == 400


def test_run_menolak_dataset_tak_dikenal(client):
    r = client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27, 999]})
    assert r.status_code == 404


def test_run_menolak_tanggal_kosong(client):
    r = client.post("/api/merge/run", json={"dataset_ids": [27, 28]})
    assert r.status_code == 400


def test_run_menolak_tanggal_tanpa_stack(client):
    r = client.post("/api/merge/run",
                    json={"date": "20250101", "dataset_ids": [27, 28]})
    assert r.status_code == 400
    assert "stack" in r.json()["detail"].lower()


def test_run_dua_kali_butuh_overwrite(client):
    body = {"date": "20251201", "dataset_ids": [27, 28]}
    assert client.post("/api/merge/run", json=body).status_code == 200
    again = client.post("/api/merge/run", json=body)
    assert again.status_code == 409
    assert "overwrite" in again.json()["detail"]

    body["overwrite"] = True
    assert client.post("/api/merge/run", json=body).status_code == 200


def test_candidates_menandai_yang_sudah_digabung(client):
    client.post("/api/merge/run",
                json={"date": "20251201", "dataset_ids": [27, 28]})
    cand = client.get("/api/merge/candidates").json()["candidates"][0]
    assert cand["already_merged"] is True
    assert cand["output_size_bytes"] > 0


class TestPreview:
    """Penggabungan ikut menulis PNG.

    Tanpa ini hasil penggabungan cuma HDF5 belasan GB, dan pertanyaan pertama
    setelahnya -- apakah stripnya benar-benar bersambung -- tidak bisa dijawab
    tanpa menulis kode dulu.
    """

    def test_run_menghasilkan_preview_yang_bisa_diambil(self, client):
        r = client.post("/api/merge/run",
                        json={"date": "20251201", "dataset_ids": [27, 28]})
        images = r.json()["preview_images"]
        assert images, "penggabungan seharusnya ikut menulis PNG"

        png = client.get(images[0]["url"])
        assert png.status_code == 200
        assert png.headers["content-type"] == "image/png"
        assert png.content[:8] == b"\x89PNG\r\n\x1a\n"

    def test_candidates_membawa_preview_setelah_digabung(self, client):
        cand = client.get("/api/merge/candidates").json()["candidates"][0]
        assert cand["preview_images"] == [], "belum digabung, belum ada gambar"

        client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27, 28]})
        cand = client.get("/api/merge/candidates").json()["candidates"][0]
        assert len(cand["preview_images"]) >= 1

    def test_rebuild_membuat_preview_tanpa_menyentuh_hdf5(self, client, merge_env):
        """Berkas yang digabung sebelum preview ada harus bisa disusulkan
        gambarnya tanpa menggabung ulang belasan GB."""
        client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27, 28]})
        out = merge_env / "merged" / "merged_20251201.h5"
        before = out.read_bytes()

        import shutil
        shutil.rmtree(out.parent / "preview")
        assert client.get("/api/merge/candidates").json(
        )["candidates"][0]["preview_images"] == []

        r = client.post("/api/merge/preview/20251201/rebuild")
        assert r.status_code == 200, r.text
        assert r.json()["preview_images"], "rebuild seharusnya menulis PNG"
        assert out.read_bytes() == before, "HDF5 ikut berubah"

    def test_hapus_membuang_hdf5_dan_preview_tanpa_menyentuh_sumber(
            self, client, merge_env):
        client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27, 28]})
        out = merge_env / "merged" / "merged_20251201.h5"
        src = merge_env / "datasets" / "27_JAWA_A" / "fusion" / "fusion_20251201_x.h5"
        src_before = src.read_bytes()
        assert out.exists()

        r = client.delete("/api/merge/result/20251201")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["freed_bytes"] > 0
        assert not out.exists()
        assert not (out.parent / "preview" / out.stem).exists()
        assert src.read_bytes() == src_before, "stack sumber ikut terhapus"

        # Sudah hilang dari daftar, dan bisa digabung lagi dari sumber yang utuh.
        cand = client.get("/api/merge/candidates").json()["candidates"][0]
        assert cand["already_merged"] is False
        assert client.post(
            "/api/merge/run",
            json={"date": "20251201", "dataset_ids": [27, 28]}
        ).status_code == 200

    def test_hapus_preview_saja_menyisakan_hdf5(self, client, merge_env):
        client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27, 28]})
        out = merge_env / "merged" / "merged_20251201.h5"

        r = client.delete("/api/merge/result/20251201?preview_only=true")
        assert r.status_code == 200
        assert out.exists(), "preview_only seharusnya tidak menghapus HDF5"
        assert client.get("/api/merge/candidates").json(
        )["candidates"][0]["preview_images"] == []

    def test_hapus_menolak_tanggal_tak_dikenal_dan_bentuk_aneh(
            self, client, merge_env):
        assert client.delete("/api/merge/result/20250101").status_code == 404

        # Bukan delapan digit -> ditolak sebelum menyentuh disk. `..` bahkan
        # tidak sampai ke handler: klien menormalkan path-nya lebih dulu. Yang
        # dijaga di sini bukan kode status tertentu, tapi bahwa tidak satu pun
        # bentuk ini berhasil menghapus sesuatu.
        for bad in ("2025", "..", "20251201x", "%2e%2e"):
            assert client.delete(f"/api/merge/result/{bad}").status_code != 200
        assert (merge_env / "datasets" / "27_JAWA_A" / "fusion").is_dir()

    def test_rebuild_menolak_tanggal_yang_belum_digabung(self, client):
        r = client.post("/api/merge/preview/20250101/rebuild")
        assert r.status_code == 404

    def test_preview_hanya_melayani_berkas_yang_memang_ada(self, client):
        client.post("/api/merge/run",
                    json={"date": "20251201", "dataset_ids": [27, 28]})
        # Nama dicocokkan ke isi folder, jadi bentuk apa pun yang bukan PNG di
        # sana -- termasuk yang menaiki direktori -- berhenti di 404.
        for name in ("tidak_ada.png", "../../merged_20251201.h5"):
            assert client.get(
                f"/api/merge/preview/20251201/{name}"
            ).status_code == 404


class TestAllDatasetsShape:
    """`_all_datasets` membaca hasil DatasetManager.list_datasets.

    Tes lain di berkas ini men-stub `_all_datasets`, jadi tidak satu pun
    menyentuh isinya -- dan di situlah bug pertamanya bersembunyi: ia membaca
    kunci `"datasets"` sementara list_datasets mengembalikan `"items"`.
    Kesalahannya tidak melempar apa pun, cuma mengembalikan daftar kosong,
    sehingga panel penggabungan diam seolah memang tidak ada yang bisa
    digabung. Tes ini memakai pembungkus sungguhan, bukan stub.
    """

    def test_membaca_kunci_items(self, monkeypatch):
        from api.routes import merge as merge_route

        class FakeManager:
            def __init__(self, db):
                pass

            def list_datasets(self, limit=500):
                return {
                    "total": 2, "limit": limit, "offset": 0,
                    "items": [
                        {"dataset_id": 27, "name": "JAWA_A", "status": "COMPLETED"},
                        {"dataset_id": 28, "name": "JAWA_B", "status": "COMPLETED"},
                    ],
                }

        monkeypatch.setattr(merge_route, "DatasetManager", FakeManager)
        got = merge_route._all_datasets(object())
        assert got == [
            {"dataset_id": 27, "name": "JAWA_A"},
            {"dataset_id": 28, "name": "JAWA_B"},
        ]

    def test_daftar_kosong_bukan_error(self, monkeypatch):
        from api.routes import merge as merge_route

        class EmptyManager:
            def __init__(self, db):
                pass

            def list_datasets(self, limit=500):
                return {"total": 0, "limit": limit, "offset": 0, "items": []}

        monkeypatch.setattr(merge_route, "DatasetManager", EmptyManager)
        assert merge_route._all_datasets(object()) == []
