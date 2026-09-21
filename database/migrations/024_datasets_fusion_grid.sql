-- 024_datasets_fusion_grid.sql
--
-- Paku grid fusion satu dataset, sekali, selamanya.
--
-- MASALAH YANG DIPERBAIKI
--
-- _dataset_fusion_grid() menurunkan ulang grid dataset SETIAP KALI fusion
-- berjalan, dengan bertanya ke _dataset_s1_reference_grid(): "raster S1 dataset
-- ini, urut product_id, ambil yang pertama filenya masih ada".
--
-- Jawaban pertanyaan itu tidak stabil. Berkas bisa hilang atau dipindah,
-- is_latest/is_valid bisa bergeser saat produk baru terdaftar, dan begitu
-- raster pertama berganti, RESOLUSINYA ikut berganti — module1b mereproyeksi
-- tiap scene dengan calculate_default_transform, jadi tiap scene punya ukuran
-- piksel sendiri (dataset 26 punya 14 bentuk raster S1 berbeda).
--
-- Akibatnya dua kali jalan pada dataset yang sama menghasilkan grid berbeda.
-- Terukur di dataset 26 (JAWA):
--
--     fusion_20251201_cooccurrence_processed.h5   32040 x 103630
--     fusion_20251204_cooccurrence_processed.h5   31922 x 103248
--
-- Tidak satu pun pikselnya berhimpit, jadi deret waktunya tidak bisa ditumpuk
-- jadi satu array — padahal itu satu-satunya alasan tier FUSION ada.
--
-- PERBAIKANNYA
--
-- Grid dihitung sekali pada fusion pertama, disimpan di sini, lalu dibaca
-- ulang oleh semua jalan berikutnya. Sumber kebenarannya pindah dari "apa yang
-- kebetulan ada di disk saat ini" menjadi "apa yang sudah diputuskan dataset
-- ini", sehingga jawabannya tidak lagi bergantung pada keadaan disk.
--
-- CATATAN untuk dataset yang sudah terlanjur bercabang grid-nya: migration ini
-- tidak memperbaiki berkas lama. Dataset 26 tetap punya dua stack pada grid
-- berbeda; salah satunya harus dirakit ulang setelah grid dipaku.

ALTER TABLE datasets
    ADD COLUMN IF NOT EXISTS fusion_grid JSONB;

COMMENT ON COLUMN datasets.fusion_grid IS
    'Grid fusion yang dipaku untuk dataset ini: {transform, width, height, '
    'crs, source_product_id, pinned_at}. Ditulis sekali oleh module9 pada '
    'fusion pertama dan dibaca ulang seterusnya, supaya grid tidak berpindah '
    'ketika raster S1 acuan berubah ketersediaannya. NULL = belum pernah '
    'fusion.';
