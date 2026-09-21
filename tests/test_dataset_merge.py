"""Tes penggabungan stack fusion lintas dataset (etl/dataset_merge.py).

Yang diuji di sini bukan "apakah kodenya jalan" tapi "apakah nilainya utuh":
penggabungan yang menggeser piksel setengah langkah atau meresample diam-diam
akan tetap menghasilkan berkas yang terbuka dengan rapi, dan kerusakannya baru
ketahuan setelah model dilatih. Karena itu setiap tes membandingkan nilai
piksel hasil gabungan dengan sumbernya, bukan sekadar bentuk raster.
"""
from __future__ import annotations

import h5py
import numpy as np
import pytest

from etl.dataset_merge import (
    GridMismatch,
    MergeCandidate,
    StackInfo,
    check_mergeable,
    find_candidates,
    merge_stacks,
    read_stack_info,
)

RES = 9.100297437167133e-05  # ukuran piksel grid Jawa yang dipaku (D16)
WEST0 = 105.2095467
NORTH0 = -5.8755039


def write_stack(
    path,
    *,
    west,
    north,
    height,
    width,
    layers=("s1_vv", "s1_vh"),
    fill=None,
    strategy="CO_OCCURRENCE",
    res=RES,
    crs="EPSG:4326",
    with_transform=True,
):
    """Tulis satu stack HDF5 sesuai kontrak module9_fusion."""
    with h5py.File(path, "w") as h:
        for i, name in enumerate(layers):
            if fill is None:
                # Nilai unik per piksel supaya penempatan yang salah satu
                # piksel pun langsung terdeteksi, bukan lolos karena kebetulan.
                data = np.arange(height * width, dtype=np.float32).reshape(height, width)
                data = data + i * 1000.0 + west * 10.0
            else:
                data = np.full((height, width), fill, dtype=np.float32)
            h.create_dataset(name, data=data)
        h.attrs["layers"] = list(layers)
        h.attrs["height"] = height
        h.attrs["width"] = width
        h.attrs["crs"] = crs
        h.attrs["fusion_strategy"] = strategy
        if with_transform:
            h.attrs["transform"] = [res, 0.0, west, 0.0, -res, north]
    return path


def info_for(path, dataset_id, name):
    got = read_stack_info(path, dataset_id, name)
    assert got is not None, f"{path} seharusnya terbaca"
    return got


@pytest.fixture
def two_strips(tmp_path):
    """Dua strip bersebelahan di grid yang sama, seperti JAWA_A dan JAWA_B."""
    a = write_stack(
        tmp_path / "fusion_20251201_a.h5", west=WEST0, north=NORTH0, height=40, width=30
    )
    b = write_stack(
        tmp_path / "fusion_20251201_b.h5",
        west=WEST0 + 30 * RES,  # persis bersambung, nol tumpang tindih
        north=NORTH0,
        height=40,
        width=25,
    )
    return info_for(a, 27, "JAWA_A"), info_for(b, 28, "JAWA_B")


# ---------------------------------------------------------------------------
# Kelayakan
# ---------------------------------------------------------------------------

def test_strip_bersebelahan_layak_digabung(two_strips):
    cand = check_mergeable(list(two_strips))
    assert cand.mergeable, cand.blocked_reason
    assert cand.out_height == 40
    assert cand.out_width == 55  # 30 + 25, tanpa celah dan tanpa tumpang tindih


def test_satu_stack_saja_bukan_kandidat(two_strips):
    cand = check_mergeable([two_strips[0]])
    assert not cand.mergeable
    assert "minimal dua" in cand.blocked_reason


def test_grid_tidak_sejajar_ditolak(tmp_path):
    """Origin yang bergeser SETENGAH piksel tidak boleh diam-diam diresample."""
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0, height=10, width=10)
    b = write_stack(
        tmp_path / "b.h5",
        west=WEST0 + 10 * RES + RES / 2,  # geser setengah piksel
        north=NORTH0,
        height=10,
        width=10,
    )
    cand = check_mergeable([info_for(a, 1, "A"), info_for(b, 2, "B")])
    assert not cand.mergeable
    assert "tidak sejajar" in cand.blocked_reason
    assert "resample" in cand.blocked_reason


def test_ukuran_piksel_beda_ditolak(tmp_path):
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0, height=10, width=10)
    b = write_stack(
        tmp_path / "b.h5", west=WEST0 + 10 * RES, north=NORTH0,
        height=10, width=10, res=RES * 2,
    )
    cand = check_mergeable([info_for(a, 1, "A"), info_for(b, 2, "B")])
    assert not cand.mergeable


def test_crs_beda_ditolak(tmp_path):
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0, height=10, width=10)
    b = write_stack(
        tmp_path / "b.h5", west=WEST0 + 10 * RES, north=NORTH0,
        height=10, width=10, crs="EPSG:32748",
    )
    cand = check_mergeable([info_for(a, 1, "A"), info_for(b, 2, "B")])
    assert not cand.mergeable


def test_lapisan_beda_ditolak_bukan_diisi_nodata(tmp_path):
    """Lapisan yang cuma ada di separuh peta adalah jebakan diam."""
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0, height=10, width=10,
                    layers=("s1_vv", "s1_vh"))
    b = write_stack(tmp_path / "b.h5", west=WEST0 + 10 * RES, north=NORTH0,
                    height=10, width=10, layers=("s1_vv",))
    cand = check_mergeable([info_for(a, 1, "A"), info_for(b, 2, "B")])
    assert not cand.mergeable
    assert "lapisan" in cand.blocked_reason.lower()


def test_strategi_fusion_beda_ditolak(tmp_path):
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0, height=10, width=10,
                    strategy="CO_OCCURRENCE")
    b = write_stack(tmp_path / "b.h5", west=WEST0 + 10 * RES, north=NORTH0,
                    height=10, width=10, strategy="FULL_COVERAGE")
    cand = check_mergeable([info_for(a, 1, "A"), info_for(b, 2, "B")])
    assert not cand.mergeable
    assert "strategi" in cand.blocked_reason.lower()


def test_stack_lama_tanpa_transform_dilewati(tmp_path):
    """Stack versi lama tidak bisa ditempatkan di peta, jadi bukan kandidat."""
    p = write_stack(tmp_path / "old.h5", west=WEST0, north=NORTH0,
                    height=10, width=10, with_transform=False)
    assert read_stack_info(p, 1, "A") is None


# ---------------------------------------------------------------------------
# Nilai piksel -- inti dari semuanya
# ---------------------------------------------------------------------------

def test_nilai_piksel_utuh_setelah_digabung(two_strips, tmp_path):
    a, b = two_strips
    out = merge_stacks([a, b], tmp_path / "merged.h5")

    with h5py.File(out, "r") as m, h5py.File(a.path, "r") as fa, h5py.File(b.path, "r") as fb:
        for layer in ("s1_vv", "s1_vh"):
            merged = m[layer][:]
            # Strip A menempati kolom 0..29, strip B kolom 30..54.
            np.testing.assert_array_equal(merged[:, :30], fa[layer][:])
            np.testing.assert_array_equal(merged[:, 30:], fb[layer][:])


def test_grid_keluaran_menutup_gabungan_kedua_strip(two_strips, tmp_path):
    a, b = two_strips
    out = merge_stacks([a, b], tmp_path / "merged.h5")
    with h5py.File(out, "r") as m:
        assert (int(m.attrs["height"]), int(m.attrs["width"])) == (40, 55)
        t = list(m.attrs["transform"])
        assert t[2] == pytest.approx(WEST0)      # origin = paling barat
        assert t[5] == pytest.approx(NORTH0)     # origin = paling utara
        assert t[0] == pytest.approx(RES)        # ukuran piksel tidak berubah
        assert m[list(m.attrs["layers"])[0]].shape == (40, 55)


def test_celah_antar_strip_jadi_nodata(tmp_path):
    """Dua strip yang tidak bersentuhan menyisakan lubang, dan lubang itu
    harus NaN -- bukan nol, yang di dB adalah nilai yang sah."""
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0, height=10, width=10,
                    layers=("s1_vv",), fill=1.0)
    b = write_stack(tmp_path / "b.h5", west=WEST0 + 20 * RES, north=NORTH0,
                    height=10, width=10, layers=("s1_vv",), fill=2.0)
    out = merge_stacks([info_for(a, 1, "A"), info_for(b, 2, "B")], tmp_path / "m.h5")
    with h5py.File(out, "r") as m:
        data = m["s1_vv"][:]
        assert data.shape == (10, 30)
        assert np.all(data[:, :10] == 1.0)
        assert np.all(np.isnan(data[:, 10:20])), "celah harus NaN, bukan nol"
        assert np.all(data[:, 20:] == 2.0)


def test_nodata_sumber_tidak_menghapus_piksel_valid_tetangga(tmp_path):
    """Di daerah tumpang tindih, strip yang bolong tidak boleh melubangi
    tetangganya yang datanya utuh."""
    full = write_stack(tmp_path / "full.h5", west=WEST0, north=NORTH0,
                       height=10, width=10, layers=("s1_vv",), fill=5.0)
    holed_path = tmp_path / "holed.h5"
    write_stack(holed_path, west=WEST0, north=NORTH0, height=10, width=10,
                layers=("s1_vv",), fill=np.nan)
    out = merge_stacks(
        [info_for(full, 1, "FULL"), info_for(holed_path, 2, "HOLED")],
        tmp_path / "m.h5",
    )
    with h5py.File(out, "r") as m:
        assert np.all(m["s1_vv"][:] == 5.0), "NaN menimpa nilai valid"


def test_tiga_strip_berurutan(tmp_path):
    """Kasus nyata A-B-C: penempatan harus benar untuk lebih dari dua."""
    paths = []
    for i, fill in enumerate((1.0, 2.0, 3.0)):
        p = write_stack(
            tmp_path / f"s{i}.h5", west=WEST0 + i * 10 * RES, north=NORTH0,
            height=8, width=10, layers=("s1_vv",), fill=fill,
        )
        paths.append(info_for(p, i + 1, f"S{i}"))
    out = merge_stacks(paths, tmp_path / "m.h5")
    with h5py.File(out, "r") as m:
        data = m["s1_vv"][:]
        assert data.shape == (8, 30)
        assert np.all(data[:, :10] == 1.0)
        assert np.all(data[:, 10:20] == 2.0)
        assert np.all(data[:, 20:] == 3.0)


def test_strip_beda_tinggi_disejajarkan_di_baris_yang_benar(tmp_path):
    """Strip Jawa punya rentang lintang berbeda-beda (D16), jadi tinggi dan
    posisi vertikalnya tidak sama."""
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0,
                    height=20, width=10, layers=("s1_vv",), fill=1.0)
    # B mulai 5 piksel lebih ke selatan dan lebih pendek.
    b = write_stack(tmp_path / "b.h5", west=WEST0 + 10 * RES, north=NORTH0 - 5 * RES,
                    height=10, width=10, layers=("s1_vv",), fill=2.0)
    out = merge_stacks([info_for(a, 1, "A"), info_for(b, 2, "B")], tmp_path / "m.h5")
    with h5py.File(out, "r") as m:
        data = m["s1_vv"][:]
        assert data.shape == (20, 20)
        assert np.all(data[:, :10] == 1.0)
        assert np.all(np.isnan(data[:5, 10:])), "5 baris teratas kolom B harus kosong"
        assert np.all(data[5:15, 10:] == 2.0)
        assert np.all(np.isnan(data[15:, 10:])), "baris bawah kolom B harus kosong"


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def test_hasil_gabungan_menjelaskan_asalnya(two_strips, tmp_path):
    """Tanpa jejak ini, berkas gabungan tidak bisa ditelusuri balik ke strip
    pembentuknya dan D16 jadi tidak terbaca dari data."""
    a, b = two_strips
    out = merge_stacks([a, b], tmp_path / "merged.h5")
    with h5py.File(out, "r") as m:
        assert bool(m.attrs["merged"]) is True
        assert list(m.attrs["merged_from_dataset_ids"]) == [27, 28]
        names = [x.decode() if isinstance(x, bytes) else x
                 for x in m.attrs["merged_from_datasets"]]
        assert names == ["JAWA_A", "JAWA_B"]
        offsets = [list(o) for o in m.attrs["merged_offsets_row_col"]]
        assert offsets == [[0, 0], [0, 30]]
        assert m.attrs["merged_date"] == "20251201"


def test_merge_menolak_kandidat_terhalang(tmp_path):
    a = write_stack(tmp_path / "a.h5", west=WEST0, north=NORTH0, height=10, width=10)
    b = write_stack(tmp_path / "b.h5", west=WEST0 + 10 * RES + RES / 2,
                    north=NORTH0, height=10, width=10)
    with pytest.raises(ValueError, match="tidak sejajar"):
        merge_stacks([info_for(a, 1, "A"), info_for(b, 2, "B")], tmp_path / "m.h5")


# ---------------------------------------------------------------------------
# Penemuan kandidat
# ---------------------------------------------------------------------------

def test_kandidat_dikelompokkan_per_tanggal(tmp_path, monkeypatch):
    """Tanggal yang cuma dipunyai satu dataset bukan kandidat."""
    from etl import folder_manager as fm

    roots = {}
    for did, name in ((27, "JAWA_A"), (28, "JAWA_B")):
        root = tmp_path / f"{did}_{name}"
        (root / "fusion").mkdir(parents=True)
        roots[did] = root

    # 20251201 ada di keduanya; 20251204 cuma di A.
    write_stack(roots[27] / "fusion" / "fusion_20251201_x.h5",
                west=WEST0, north=NORTH0, height=10, width=10)
    write_stack(roots[28] / "fusion" / "fusion_20251201_x.h5",
                west=WEST0 + 10 * RES, north=NORTH0, height=10, width=10)
    write_stack(roots[27] / "fusion" / "fusion_20251204_x.h5",
                west=WEST0, north=NORTH0, height=10, width=10)

    monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: roots[did])

    cands = find_candidates([
        {"dataset_id": 27, "name": "JAWA_A"},
        {"dataset_id": 28, "name": "JAWA_B"},
    ])
    assert [c.date_key for c in cands] == ["20251201"]
    assert cands[0].mergeable
    assert cands[0].dataset_ids == [27, 28]
