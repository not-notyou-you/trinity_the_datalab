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
