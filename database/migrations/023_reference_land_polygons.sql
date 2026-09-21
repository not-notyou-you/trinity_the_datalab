-- 023_reference_land_polygons.sql
--
-- Tabel referensi garis pantai: poligon DARATAN dari OSM land polygons.
--
-- Dipakai etl/land_mask.py untuk menghasilkan masks/land_distance.tif per
-- dataset. Perannya murni referensi — tidak ada tier, tidak ada lineage, tidak
-- terikat dataset mana pun, dan tidak pernah mengubah raw maupun fusion.
--
-- Isinya poligon daratan, bukan garis pantai: pertanyaan yang dijawab adalah
-- "titik ini di dalam daratan atau tidak", dan poligon menjawabnya langsung
-- sementara garis perlu uji sisi yang rapuh di pulau kecil dan muara.
--
-- Sungai dan danau TIDAK dilubangi dari poligon ini, dan itu disengaja:
-- luapan sungai justru kejadian yang ingin ditangkap, jadi air tawar harus
-- tetap berada di sisi "darat" dan tidak boleh ikut tersingkir bersama laut.
--
-- Sumber : https://osmdata.openstreetmap.de/download/land-polygons-complete-4326.zip
-- Lisensi: ODbL (OpenStreetMap contributors)
-- Muat   : ogr2ogr -f PGDUMP ... -spat 94 -12 142 7   (dipotong ke Indonesia;
--          untuk AOI di luar itu, muat ulang dengan -spat yang lebih luas)
--
-- Tabelnya diisi oleh dump ogr2ogr, bukan oleh migration ini. Migration ini
-- hanya menjamin bentuk dan indeksnya konsisten bila tabel sudah ada, dan
-- mendokumentasikan asal-usulnya supaya angka di penelitian bisa direproduksi.

CREATE TABLE IF NOT EXISTS reference_land_polygons (
    gid  SERIAL PRIMARY KEY,
    fid  BIGINT,
    geom GEOMETRY(MultiPolygon, 4326)
);

-- Indeks spasial wajib: tanpa ini setiap pembuatan mask memindai seluruh
-- 18.252 poligon, padahal AOI seukuran Jabodetabek hanya memotong 22.
CREATE INDEX IF NOT EXISTS reference_land_polygons_geom_idx
    ON reference_land_polygons USING GIST (geom);

COMMENT ON TABLE reference_land_polygons IS
    'OSM land polygons (ODbL), dipotong ke Indonesia. Referensi darat/laut '
    'untuk etl/land_mask.py. Sungai dan danau sengaja tidak dilubangi.';
