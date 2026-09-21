"""Layer referensi harus dibangun ulang saat grid dataset berubah.

Regresi nyata (D19/D20): keempat strip Jawa dipakukan ke satu grid bersama
SETELAH masks-nya terlanjur dibuat. Penjaga idempoten di
`reference_layers` waktu itu hanya memeriksa "berkasnya ada", jadi
land_distance.tif dan water_occurrence.tif JAWA_A dan JAWA_B tetap di grid lama
(25805x19018) sementara stack-nya pindah ke 25907x19093.

Diamnya yang berbahaya: berkas lama tetap terbuka normal, tidak ada error di
mana pun, tapi mask[r,c] tidak lagi menunjuk piksel yang sama dengan
stack[r,c]. Deep learning engineer yang mengindeks keduanya berdampingan
membaca lokasi yang salah tanpa pernah diberi tahu.
"""
from __future__ import annotations

import numpy as np
import pytest
import rasterio
from rasterio.transform import Affine

from etl.reference_layers import _grid_matches, _reuse_or_rebuild

RES = 9.06676818684e-05
WEST = 105.2095467
NORTH = -5.8755039


def write_tif(path, *, width, height, res=RES, west=WEST, north=NORTH):
    transform = Affine(res, 0.0, west, 0.0, -res, north)
    # Tanpa CRS: `_grid_matches` hanya membandingkan ukuran dan transform,
    # dan meminta EPSG di sini akan menabrak proj.db PostgreSQL yang dicatat
    # di .env sebagai sumber galat "Cannot find proj.db" di mesin ini.
    with rasterio.open(
        path, "w", driver="GTiff", width=width, height=height, count=1,
        dtype="int16", transform=transform,
    ) as dst:
        dst.write(np.zeros((height, width), dtype="int16"), 1)
    return path


def grid(width, height, res=RES, west=WEST, north=NORTH):
    return Affine(res, 0.0, west, 0.0, -res, north), (height, width)


class TestGridMatches:
    def test_grid_yang_sama_cocok(self, tmp_path):
        p = write_tif(tmp_path / "a.tif", width=64, height=48)
        t, shape = grid(64, 48)
        assert _grid_matches(p, t, shape) is True

    def test_ukuran_beda_tidak_cocok(self, tmp_path):
        """Persis kasus JAWA_C: resolusi benar, lebar meleset satu kolom."""
        p = write_tif(tmp_path / "a.tif", width=65, height=48)
        t, shape = grid(64, 48)
        assert _grid_matches(p, t, shape) is False

    def test_resolusi_beda_tidak_cocok(self, tmp_path):
        """Persis kasus JAWA_A dan JAWA_B: seluruh grid dari pemakuan lama."""
        p = write_tif(tmp_path / "a.tif", width=64, height=48, res=9.10306849989e-05)
        t, shape = grid(64, 48)
        assert _grid_matches(p, t, shape) is False

    def test_origin_bergeser_satu_piksel_tidak_cocok(self, tmp_path):
        p = write_tif(tmp_path / "a.tif", width=64, height=48, west=WEST + RES)
        t, shape = grid(64, 48)
        assert _grid_matches(p, t, shape) is False

    def test_geser_jauh_di_bawah_setengah_piksel_masih_cocok(self, tmp_path):
        """Galat pembulatan lewat JSON tidak boleh memicu bangun ulang."""
        p = write_tif(tmp_path / "a.tif", width=64, height=48, west=WEST + RES * 0.01)
        t, shape = grid(64, 48)
        assert _grid_matches(p, t, shape) is True

    def test_berkas_rusak_dianggap_tidak_cocok(self, tmp_path):
        p = tmp_path / "rusak.tif"
        p.write_bytes(b"bukan geotiff")
        t, shape = grid(64, 48)
        assert _grid_matches(p, t, shape) is False


class TestReuseOrRebuild:
    def test_berkas_belum_ada_dibangun(self, tmp_path):
        t, shape = grid(64, 48)
        assert _reuse_or_rebuild(tmp_path / "hilang.tif", t, shape, False) is None

    def test_grid_cocok_dipakai_ulang(self, tmp_path):
        p = write_tif(tmp_path / "a.tif", width=64, height=48)
        t, shape = grid(64, 48)
        assert _reuse_or_rebuild(p, t, shape, False) == "exists"

    def test_grid_basi_dibangun_ulang_tanpa_perlu_force(self, tmp_path):
        """Inti perbaikannya: pemulihan terjadi sendiri, tidak menunggu
        seseorang ingat memanggil dengan force=True."""
        p = write_tif(tmp_path / "a.tif", width=64, height=48,
                      res=9.10306849989e-05)
        t, shape = grid(64, 48)
        assert _reuse_or_rebuild(p, t, shape, False) is None

    def test_force_selalu_membangun_ulang(self, tmp_path):
        p = write_tif(tmp_path / "a.tif", width=64, height=48)
        t, shape = grid(64, 48)
        assert _reuse_or_rebuild(p, t, shape, True) is None

    def test_grid_basi_dicatat_di_log(self, tmp_path, caplog):
        p = write_tif(tmp_path / "land_distance.tif", width=64, height=48,
                      res=9.10306849989e-05)
        t, shape = grid(64, 48)
        with caplog.at_level("WARNING"):
            _reuse_or_rebuild(p, t, shape, False)
        assert "tidak cocok" in caplog.text
        assert "land_distance.tif" in caplog.text
