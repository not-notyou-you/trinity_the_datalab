"""Unit test untuk etl/geo_utils.py — validasi bbox dan parsing.

Tidak butuh database maupun jaringan, jadi bisa jalan di mana saja:
    pytest tests/test_geo_utils.py
"""

import pytest

from etl.geo_utils import (
    MAX_SPAN_DEG,
    BBoxError,
    bbox_to_wkt,
    parse_bbox_string,
    validate_bbox,
)

# bbox area uji DKI Jakarta yang dipakai seeder
JKT_TEST = (106.78, -6.22, 106.87, -6.07)


class TestValidateBBox:
    def test_bbox_valid_dikembalikan_sebagai_float(self):
        assert validate_bbox(*JKT_TEST) == JKT_TEST

    def test_string_angka_diterima(self):
        assert validate_bbox("106.78", "-6.22", "106.87", "-6.07") == JKT_TEST

    def test_bukan_angka_ditolak(self):
        with pytest.raises(BBoxError, match="angka"):
            validate_bbox("utara", -6.22, 106.87, -6.07)

    @pytest.mark.parametrize("bad_lon", [-180.1, 180.1, 999])
    def test_longitude_di_luar_rentang_ditolak(self, bad_lon):
        with pytest.raises(BBoxError, match="Longitude"):
            validate_bbox(bad_lon, -6.22, bad_lon + 0.1, -6.07)

    @pytest.mark.parametrize("bad_lat", [-90.1, 90.1])
    def test_latitude_di_luar_rentang_ditolak(self, bad_lat):
        with pytest.raises(BBoxError, match="Latitude"):
            validate_bbox(106.78, bad_lat, 106.87, bad_lat + 0.1)

    def test_lon_min_harus_lebih_kecil_dari_lon_maks(self):
        with pytest.raises(BBoxError, match="Longitude minimum"):
            validate_bbox(106.87, -6.22, 106.78, -6.07)

    def test_lat_min_harus_lebih_kecil_dari_lat_maks(self):
        with pytest.raises(BBoxError, match="Latitude minimum"):
            validate_bbox(106.78, -6.07, 106.87, -6.22)

    def test_bbox_degenerate_ditolak(self):
        with pytest.raises(BBoxError, match="terlalu kecil"):
            validate_bbox(106.78, -6.22, 106.780001, -6.219999)

    def test_bbox_terlalu_besar_ditolak(self):
        span = MAX_SPAN_DEG + 1
        with pytest.raises(BBoxError, match="terlalu besar"):
            validate_bbox(100.0, -6.0, 100.0 + span, -6.0 + span)


class TestBBoxToWKT:
    def test_wkt_tertutup_dan_urutannya_benar(self):
        wkt = bbox_to_wkt(*JKT_TEST)
        assert wkt == (
            "POLYGON((106.78 -6.22, 106.87 -6.22, 106.87 -6.07, "
            "106.78 -6.07, 106.78 -6.22))"
        )

    def test_titik_awal_sama_dengan_titik_akhir(self):
        coords = bbox_to_wkt(*JKT_TEST).removeprefix("POLYGON((").removesuffix("))").split(", ")
        assert len(coords) == 5 and coords[0] == coords[-1]


class TestParseBBoxString:
    @pytest.mark.parametrize("raw", [
        "106.78, -6.22, 106.87, -6.07",
        "106.78 -6.22 106.87 -6.07",
        "106.78;-6.22;106.87;-6.07",
        "  106.78 , -6.22 , 106.87 , -6.07  ",
    ])
    def test_pemisah_bebas(self, raw):
        assert parse_bbox_string(raw) == JKT_TEST

    @pytest.mark.parametrize("raw", ["", "106.78, -6.22", "1, 2, 3, 4, 5", None])
    def test_jumlah_angka_harus_empat(self, raw):
        with pytest.raises(BBoxError, match="4 angka"):
            parse_bbox_string(raw)

    def test_hasil_parse_ikut_divalidasi(self):
        with pytest.raises(BBoxError, match="Longitude minimum"):
            parse_bbox_string("106.87, -6.22, 106.78, -6.07")
