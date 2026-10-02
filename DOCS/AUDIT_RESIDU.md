# Audit Residu Kode — The DataLab

Tanggal audit: 2026-10-02 · Branch: `feat/reference-layers-and-fusion-grid` · Status: **belum ada yang dihapus**

Dokumen ini berisi daftar residu (kode mati, kode/fitur warisan, file sisa eksperimen, dependency tak terpakai) beserta analisis dampaknya. Tujuannya untuk di-review dulu sebelum ada keputusan penghapusan.

---

## 0. Ringkasan

| Kategori | Jumlah temuan | Total risiko |
|---|---|---|
| A. Temuan prioritas sebelum rilis publik (bukan sekadar residu) | 6 | — |
| B. File & folder sisa di repo | 22 | sebagian besar Low |
| C. Kode Python mati (tanpa referensi di app maupun test) | 22 | Low |
| D. Kode yang hanya dipakai test | 13 | Medium |
| E. Fitur warisan "dataset LIVE tunggal" | 1 rantai (6 lokasi) | Medium–High |
| F. API: router tak di-mount & endpoint tak dipakai UI | 1 file + 9 grup endpoint | Low–Medium |
| G. Unused import / variabel | 54 import + 11 variabel | Low |
| H. Frontend (JS/CSS) | 4 JS + ~55 class CSS | Low |
| I. Dependency tak terpakai | 7 paket | Low |

### Metodologi

- **pyflakes 4.0.1**: unused import, undefined name, unused local variable.
- **vulture 2.16** (confidence ≥ 60%): fungsi/kelas/konstanta tanpa pemanggil. Hasilnya diverifikasi manual karena route FastAPI, field Pydantic/ORM, dan event listener SQLAlchemy selalu terdeteksi sebagai false positive.
- **grep referensi silang** untuk setiap kandidat di `etl/`, `api/`, `database/`, `tests/`, `web/`, dan skrip root. Hitungan dibedakan antara pemakaian oleh aplikasi dan pemakaian oleh test saja.
- **Peta impor modul**: memastikan modul mana yang tidak pernah di-import.
- **Pemetaan endpoint**: semua `@router.*` dibandingkan dengan pemanggilan `api(...)`/`fetch(...)`/`href` di `web/app.js`.
- **CSS**: class di `style.css`/`landing.css` dicocokkan dengan HTML/JS, termasuk pengecekan manual untuk class yang dirakit dinamis (`'lm-row-' + n`, `'src-' + source`).
- **Dependency**: setiap paket di `requirements.txt` dicocokkan dengan pernyataan `import` aktual.

### Tingkat risiko

| Level | Arti |
|---|---|
| **Low** | Tidak ada referensi di kode aplikasi maupun test. Penghapusan tidak mengubah perilaku. |
| **Medium** | Dipakai oleh test, skrip manual, atau dokumentasi, atau merupakan endpoint publik yang tidak dipakai UI. Penghapusan butuh penyesuaian test/docs atau keputusan produk. |
| **High** | Terikat ke skema database/ORM, data riset, atau `create_tables()`. Penghapusan butuh migrasi atau berisiko kehilangan data. |

### Verifikasi fungsi (baseline sebelum penghapusan)

| Pemeriksaan | Hasil |
|---|---|
| `pytest tests` (DB uji `*_test`, `AUTO_RESUME_JOBS=false`) | **754 passed, 0 failed** (69 detik) |
| `python -m compileall etl api database tests *.py` | OK, tidak ada syntax error |
| `node --check web/app.js web/icons.js` | OK |
| Router di `api/main.py` | 12 router ter-mount. `preview` sengaja tidak di-mount (lihat F1) |

Baseline ini menjadi pembanding setelah penghapusan: jalankan ulang ketiga perintah di atas, dan hasilnya harus tetap 754 passed (dikurangi jumlah test yang ikut dihapus bersama kode test-only di bagian D).

---

## A. Temuan prioritas sebelum rilis publik

Temuan berikut lebih dari sekadar "kode tak terpakai". Ada yang berupa bug laten, ada yang berisiko keamanan atau menyesatkan pengguna baru.

| ID | Lokasi | Masalah | Dampak | Rekomendasi |
|---|---|---|---|---|
| A1 | [tests/verify_prompt2.py:15-20](../tests/verify_prompt2.py#L15-L20) | **Password DB hardcoded** (`password="12345678"`, user `postgres`, DB `datalab_test`) ikut ter-commit. | Kredensial lokal bocor jika repo dipublikasikan. Password tetap ada di riwayat git walaupun filenya dihapus. | Hapus file (lihat B10) dan ganti password tersebut kalau masih dipakai di mesin mana pun. |
| A2 | [etl/metadata_manager.py:543](../etl/metadata_manager.py#L543) | `NameError: has_gold`. Rename `has_gold`→`has_final` tidak selesai di `query_latest_scenes`. | Saat ini tidak crash karena fungsinya tidak pernah dipanggil (C1). Akan crash kalau ada yang memanggilnya. | Hapus fungsinya (C1) atau perbaiki variabelnya. |
| A3 | [docker-compose.yml:26-28](../docker-compose.yml#L26-L28), [database/schema.sql](../database/schema.sql) | `schema.sql` **usang**: 12 tabel dari migrasi (`datasets`, `dataset_jobs`, `scene_job_state`, `fusion_products`, `live_areas`, `live_events`, `live_scenes`, `processing_logs`, `reference_land_polygons`, `nasa_scenes`, `cleanup_operations`, `live_dataset_sources`) tidak ada di sana. Padahal docker-compose memakai `schema.sql` untuk inisialisasi DB. | Siapa pun yang mengikuti jalur Docker akan mendapat DB tanpa tabel inti, dan aplikasi gagal. | Ganti init Docker dengan `database/run_migration.py database/migrations/*.sql`, atau buat ulang `schema.sql`. |
| A4 | [docker-compose.yml:36](../docker-compose.yml#L36) | Service `api` memakai `build: .`, tapi **tidak ada `Dockerfile`** di repo. | `docker-compose up` gagal di service api. | Tambahkan Dockerfile atau hapus service `api` dari compose. |
| A5 | [test_results.txt](../test_results.txt) | Output pytest lama (UTF-16) dari mesin lain: `C:\Users\hakim\...`, `D:\try\1_TRINITY_the_datalab\...`, hasil **56 errors**. | Membocorkan username/path lokal dan memberi kesan suite test gagal (padahal kini 754 passed). | Hapus (B13). |
| A6 | [api/main.py:88-94](../api/main.py#L88-L94) | `CORSMiddleware(allow_origins=["*"], allow_credentials=True)`. | Ini bukan residu, tapi catatan penting untuk rilis publik: semua origin diizinkan. | Batasi `allow_origins` ke domain produksi. |

---

## B. File & folder sisa di repo

| ID | Lokasi | Tipe residu | Alasan | Risiko | Dependency / dampak jika dihapus |
|---|---|---|---|---|---|
| B1 | [_copy_wajo_s1a_by_year.py](../_copy_wajo_s1a_by_year.py) | Skrip eksperimen sekali pakai | Dataset ID di-hardcode (37–49), berjalan di level modul tanpa `__main__` guard, menyalin h5 ke `data/datasets/wajo_processed_hybrid_only/`. | Low | Tidak di-import siapa pun. Pindahkan ke `scripts/research/` kalau perlu untuk reprodusibilitas riset. |
| B2 | [_export_s1a_split.py](../_export_s1a_split.py) | Skrip eksperimen sekali pakai | Sama seperti B1, output ke `_export_s1a_only/`. | Low | Tidak di-import. |
| B3 | [_refusion_run_46.py](../_refusion_run_46.py) | Skrip perbaikan sekali pakai | Hardcode `job_id = 44` dan 12 tanggal dataset 46. | Low | Tidak di-import. Fitur refusion tetap ada di `etl/refusion.py`. |
| B4 | [_s1_coverage_audit.py](../_s1_coverage_audit.py) + [_s1_coverage_detail.csv](../_s1_coverage_detail.csv) | Skrip analisis + output | Audit cakupan S1 Wajo. `import numpy` dua kali, CSV adalah hasil run. | Low | Tidak di-import. Simpan CSV di luar repo kalau dipakai untuk tesis. |
| B5 | `_export_s1a_only/` (**67 GB** di disk; 227 file `*_metadata.json` **ter-track git**) | Output data eksperimen | Hasil B2. `.h5`-nya di-ignore, tapi JSON-nya ikut masuk repo. | **Medium** | Data riset: pastikan sudah disalin ke tempat lain sebelum dihapus. Minimal `git rm --cached` + tambahkan ke `.gitignore`. |
| B6 | `_refusion_backup/46/` (**7,5 GB**, tidak ter-track) | Backup sementara | 12 h5 dataset 46 sebelum refusion (28 Sep). | Medium | Hapus setelah hasil refusion dataset 46 diverifikasi. |
| B7 | [debug_prompt4.py](../debug_prompt4.py) (root) | Scaffolding debug | Wrapper `pytest tests/test_dataset_sources_api.py` dari sesi "PROMPT 4". | Low | Tidak di-import. |
| B8 | [tests/debug_prompt4.py](../tests/debug_prompt4.py) | Duplikat B7 (sedikit berbeda) | Sama. | Low | Tidak dikoleksi pytest (bukan `test_*`). |
| B9 | [tests/verify_prompt4.py](../tests/verify_prompt4.py), [tests/test_prompt4.ps1](../tests/test_prompt4.ps1) | Scaffolding verifikasi sesi AI | Cek sintaks/impor sekali pakai; `.ps1` hanya membungkus `verify_prompt4.py`. | Low | Tidak dikoleksi pytest. Fungsinya sudah ditanggung `test_dataset_sources_api.py` dan `test_source_config.py`. |
| B10 | [tests/verify_prompt2.py](../tests/verify_prompt2.py) | Scaffolding + **kredensial** | Lihat A1. | Low | Tidak dikoleksi pytest. |
| B11 | [tests/preview_gold.py](../tests/preview_gold.py) | Skrip standalone lama | Render PNG dari GeoTIFF; digantikan `etl/module10_generate_preview.py`. | Low | Tidak di-import, tidak dikoleksi pytest. |
| B12 | [tests/verify_pipeline_run.py](../tests/verify_pipeline_run.py) | Skrip verifikasi manual | Cek status pipeline per `scene_id` dari model lama (per scene, bukan per dataset). | Low | Tidak dikoleksi pytest. Satu-satunya pemakai `get_quality_by_scene` di luar test (D1). |
| B13 | [test_results.txt](../test_results.txt) | Artefak output | Lihat A5. | Low | — |
| B14 | [package-lock.json](../package-lock.json) | File tooling yatim | Lockfile npm kosong (`"packages": {}`, nama `sentinel-metadata`), tanpa `package.json`. | Low | Tidak ada tooling Node di proyek. |
| B15 | [logo.webp](../logo.webp) (root) | Duplikat | Identik byte-per-byte dengan `web/logo.webp`. HTML memakai yang di `web/`. | Low | Hanya disebut di `old_ref/INTERFACE.md`. |
| B16 | [config/docker-compose.yml](../config/docker-compose.yml) | Duplikat | Identik dengan root `docker-compose.yml` (selain 2 baris author). Path volume relatif `./database/...` salah kalau dijalankan dari `config/`. | Low | Tidak dirujuk docs aktif. Lihat juga A3/A4. |
| B17 | `backup/` (ter-track): `check.txt.backup_*`, `scheduler.log.backup_*` (264 KB log), `data_structure_migration/` | Backup lama (Agustus 2026) | Dibuat saat cleanup sebelumnya (`old_ref/CLEANUP_CHANGELOG.md`) dan migrasi layout folder. | Low | Tidak dirujuk kode. Isi log scheduler sebaiknya tidak ikut dipublikasikan. |
| B18 | `old_ref/` (15 dokumen) | Dokumentasi lama | Digantikan `DOCS/`. Sebagian isinya salah untuk kode sekarang: menyebut `/api/preview` aktif, `icons.js` sudah dihapus (padahal masih dipakai), Pillow belum ada di requirements. | Medium | Ada `THESIS_CHAPTERS.md`/`PRD.md` yang mungkin masih berguna untuk tesis. Cukup dipindah ke luar repo publik. |
| B19 | `report/` (`PROMPT_fix_report_generation.md`, `report.md`, `report-detailed.md`) | Spesifikasi/prompt kerja | Prompt AI dan spesifikasi fitur report. Fiturnya sudah ada di `etl/report_generator.py`, dan `DOCS/REPORT.md` baru (belum di-commit) tampaknya menggantikannya. | Low | Tidak dirujuk kode. Catatan: `report.md` root sudah terhapus di working tree. |
| B20 | `DOCS/do.zip` (51 KB, di-ignore) | Arsip sisa | Tidak dirujuk. | Low | — |
| B21 | `analytics/`, `checkpoints_pipeline/` (hanya `.gitkeep`) | Folder warisan | Hanya dirujuk `PipelineConfig` (I-tidak terpakai, C17) dan `generate_quality_plot` (C3). Tidak ada kode aktif yang menulis ke sana. | Low | `logs_pipeline/` **tetap dipakai** oleh `etl/pipeline_logger.py:77`, jadi jangan dihapus. |
| B22 | [config/config.json](../config/config.json), [config/config_locations.json](../config/config_locations.json) | Konfigurasi warisan | Sudah ditandai `_DEPRECATED` di dalam filenya: tidak dibaca kode mana pun sejak migrasi 012 (lokasi pindah ke tabel `regions_of_interest`). | Low | Disebut di `DOCS/README.md:65` dan `DOCS/PIPELINE.md:394`; perbarui docs-nya. |

Tidak termasuk residu (sengaja tidak dihapus): `LIVE_MONITORING.md` di root dirujuk 12 komentar kode sebagai spesifikasi (sebaiknya dipindah ke `DOCS/` sekaligus memperbarui rujukannya). `data/_job_locks/*.lock` adalah file lock runtime tingkat OS (`etl/job_lock.py`); filenya tidak berbahaya dan sudah di-ignore. Jangan dihapus saat server berjalan.

---

## C. Kode Python mati (tidak dipanggil oleh aplikasi maupun test)

| ID | Lokasi | Tipe | Alasan | Risiko | Dampak |
|---|---|---|---|---|---|
| C1 | [etl/metadata_manager.py:486-548](../etl/metadata_manager.py#L486-L548) `query_latest_scenes` | Unused method + bug | Nol pemanggil. Mengandung NameError (A2). Merupakan sisa model "per scene" sebelum model dataset. | Low | Tidak ada. |
| C2 | [etl/module9_fusion.py:1226-1251](../etl/module9_fusion.py#L1226-L1251) `_aoi_reference_grid` | Unused function | Hanya disebut di komentar (baris 504, 1612, 1750) sebagai penjelasan. Jalur hari tanpa-S1 sekarang memakai grid yang dipin. | Low | Perbarui 3 komentar yang merujuknya. |
| C3 | [etl/module6_analytics.py:110-132](../etl/module6_analytics.py#L110-L132) `generate_quality_plot` | Unused function | Hanya `compute_band_metrics` yang di-import (module5:69). | Low | Satu-satunya pemakai matplotlib di modul ini. matplotlib tetap dipakai `report_generator`. |
| C4 | [etl/lineage_tracker.py:195-260](../etl/lineage_tracker.py#L195-L260) `record_full_pipeline` | Unused method | Rantai bronze→silver→gold lama (pra-D14). | Low | — |
| C5 | [etl/land_mask.py:80-96](../etl/land_mask.py#L80-L96) `grid_from_fusion_h5` | Unused function | Nol pemanggil. | Low | — |
| C6 | [etl/report_generator.py:314-322](../etl/report_generator.py#L314-L322) `_s1_orbit_counts` | Unused method | Nol pemanggil. | Low | — |
| C7 | [etl/module8_gpm_download.py:188-189](../etl/module8_gpm_download.py#L188-L189) `_daily_granule_url` | Unused function | URL granule sekarang dicari lewat `_granule_pattern`. | Low | — |
| C8 | [etl/tier_names.py:105-108](../etl/tier_names.py#L105-L108) `is_legacy`, [:132-142](../etl/tier_names.py#L132-L142) `display_tier`, [:145-154](../etl/tier_names.py#L145-L154) `tier_at_rank` | Unused helpers | Nol pemanggil. `display_tier` didokumentasikan untuk "jalur BACA" tapi tidak pernah dipakai. | Low | — |
| C9 | [etl/folder_manager.py:855-856](../etl/folder_manager.py#L855-L856) `get_fusion_scene_files` | Unused function | Nol pemanggil. | Low | — |
| C10 | [etl/dataset_merge.py:109-110](../etl/dataset_merge.py#L109-L110) `LayerMismatch` | Unused exception class | Tidak pernah di-raise atau di-catch. | Low | — |
| C11 | [etl/fusion_strategies.py:76](../etl/fusion_strategies.py#L76) `STRATEGIES` | Unused constant | Nol pemakai. | Low | — |
| C12 | [etl/module7_modis_download.py:116](../etl/module7_modis_download.py#L116) `MODIS_TILES` | Unused constant | Tile sekarang dihitung dari AOI (`modis_tiles_for_bbox`). Komentar di atasnya sudah menyatakan begitu. | Low | — |
| C13 | [etl/module10_generate_preview.py:119-120](../etl/module10_generate_preview.py#L119-L120) `MAX_WIDTH` | Alias nama lama | Komentar: "masih diekspor supaya pemanggil luar tidak patah", padahal tidak ada pemanggilnya. | Low | — |
| C14 | [etl/module9_fusion.py:185-189](../etl/module9_fusion.py#L185-L189) `FUSION_LAYERS` | Unused constant | Komentar di baris 680 sendiri menyebut konstanta ini "berbohong" pada dataset selektif. Hanya dirujuk di komentar module5:707 dan docstring module10:76. | Low | Perbarui 2 komentar tersebut. |
| C15 | [etl/database_client.py:1329-1347](../etl/database_client.py#L1329-L1347) `session_with_retry` | Unused method | Nol pemanggil. | Low | — |
| C16 | [etl/database_client.py:134-197](../etl/database_client.py#L134-L197) enum `DatasetKindEnum`, `DatasetStatusEnum`, `DatasetJobTypeEnum`, `DatasetJobStatusEnum`, `SceneJobStageStatusEnum`, `CleanupOperationTypeEnum`, `CleanupOperationStatusEnum`, `LiveSourceNameEnum` | Unused enums | Kolom terkait memakai `String` biasa; enum-enum ini tidak dirujuk. | Low–Medium | Berfungsi sebagai dokumentasi nilai yang valid. Kalau dihapus, pastikan nilai-nilainya terdokumentasi di `DOCS/`. |
| C17 | [etl/config.py:45-125](../etl/config.py#L45-L125) `APIConfig`, `PipelineConfig`, `Config`, `Config.from_json`, `PipelineConfig.ensure_dirs`, singleton `cfg` | Unused config layer | Hanya `DatabaseConfig` yang dipakai (`database/run_migration.py:40`). `jabodetabek_bbox`, `lee_window_size`, `cog_*`, dan lainnya tidak dibaca siapa pun; pipeline memakai konstanta per modul. | Low | **Pertahankan `DatabaseConfig` dan `load_dotenv()`** karena dipakai `run_migration.py`. |
| C18 | [api/schemas.py:51-59](../api/schemas.py#L51-L59) `SceneQueryParams`, [:408-427](../api/schemas.py#L408-L427) `DatasetJobItem`, [:507-520](../api/schemas.py#L507-L520) `CleanupOperationItem` | Unused Pydantic models | Tidak dipakai route mana pun. | Low | Tidak muncul di OpenAPI karena tidak direferensikan. |
| C19 | [api/schemas.py:293](../api/schemas.py#L293) `DatasetCreateRequest`, [:374](../api/schemas.py#L374) `DatasetResponse` | Alias nama lama | Komentar menyebut alias ini dipertahankan untuk "pemanggil internal (dan tes)", tapi tidak ada yang mengimpornya. | Low | Ada rujukan nama di `DOCS/INTERFACE.md`; perbarui. |
| C20 | [api/routes/storage.py:80](../api/routes/storage.py#L80) `pattern` | Unused local | Dihitung lalu tidak dipakai (filter memakai `endswith`). | Low | — |
| C21 | [etl/report_generator.py:917](../etl/report_generator.py#L917), [:1074](../etl/report_generator.py#L1074) `ds`; [:1098](../etl/report_generator.py#L1098), [:1108](../etl/report_generator.py#L1108) `n_charts`; [etl/report_stats.py:189](../etl/report_stats.py#L189) `root_path` | Unused locals/param | Hasil unpack/parameter yang tidak dibaca. | Low | `root_path` adalah parameter; cek pemanggil sebelum mengubah signature. |
| C22 | [etl/module8_gpm_download.py:45-46](../etl/module8_gpm_download.py#L45-L46) `from_origin`, `reproject` | Unused import + shadowing | `from_origin` di-import ulang lokal di baris 446, `reproject` tidak dipakai. | Low | — |

---

## D. Kode yang hanya dipakai oleh test

Fungsi-fungsi berikut tidak dipanggil aplikasi, tetapi ada test yang mengujinya. Kalau dihapus, test terkait juga harus dihapus atau diubah.

| ID | Lokasi | Dipakai oleh | Risiko | Catatan |
|---|---|---|---|---|
| D1 | [etl/metadata_manager.py:576-598](../etl/metadata_manager.py#L576-L598) `get_quality_by_scene` | `test_database.py`, `test_etl_pipeline.py`, `test_quality_metrics.py`, `verify_pipeline_run.py` | Medium | Endpoint `/api/quality/{scene_id}` menulis query-nya sendiri. |
| D2 | [etl/lineage_tracker.py:109-120](../etl/lineage_tracker.py#L109-L120) `compute_sha256_from_bytes` | `test_quality_metrics.py` | Medium | — |
| D3 | [etl/geo_utils.py:70-81](../etl/geo_utils.py#L70-L81) `parse_bbox_string` | `test_geo_utils.py` | Medium | Utilitas kecil; boleh dipertahankan. |
| D4 | [etl/module10_generate_preview.py:443-458](../etl/module10_generate_preview.py#L443-L458) `resolve_gold_inputs` | `test_module10_preview.py` | Medium | — |
| D5 | [etl/folder_manager.py:232-237](../etl/folder_manager.py#L232-L237) `ensure_source_level_dir` | `test_report_generation.py` | Medium | Dipakai sebagai fixture helper. |
| D6 | [etl/folder_manager.py:409-432](../etl/folder_manager.py#L409-L432) `list_dates` | `test_folder_manager.py` | Medium | — |
| D7 | [etl/folder_manager.py:442-447](../etl/folder_manager.py#L442-L447) `get_source_dir` | `test_folder_manager.py` | Medium | — |
| D8 | [etl/folder_manager.py:808-809](../etl/folder_manager.py#L808-L809) `list_fusion_scenes` | `test_folder_manager.py` | Medium | — |
| D9 | [etl/folder_manager.py:859-862](../etl/folder_manager.py#L859-L862) `get_preview_scene_files` | `test_folder_manager.py` | Medium | — |
| D10 | `DatasetSourceConfig.has_level` / `.to_dict` ([etl/database_client.py:1037-1041](../etl/database_client.py#L1037)) | `test_source_config.py` | Medium | — |
| D11 | `ProcessingPlan.max_tier` / `.to_dict`, `S1_BASE_STAGES` ([etl/processing_plan.py:44](../etl/processing_plan.py#L44), [:106](../etl/processing_plan.py#L106), [:214](../etl/processing_plan.py#L214)) | `test_processing_plan.py`, `test_source_config.py` | Medium | — |
| D12 | [etl/migrate_data_structure.py](../etl/migrate_data_structure.py) (500 baris, CLI `python -m`) | 1 test; dijalankan manual | Medium | Migrasi layout folder L1/L2/L3. Hapus hanya setelah **semua** instalasi sudah dimigrasi. Backup di B17 berasal dari skrip ini. |
| D13 | [etl/seed_data.py](../etl/seed_data.py) (383 baris, CLI) | 2 test | Medium | Membuat data sintetis dengan path lama `/processed/bronze/...`. Masih berguna untuk demo, tapi model datanya usang. |

`DatabaseClient.create_tables` ([etl/database_client.py:1364-1366](../etl/database_client.py#L1364-L1366)) hanya dipakai `tests/conftest.py`, tetapi **wajib dipertahankan** karena seluruh fixture DB bergantung padanya.

---

## E. Fitur warisan: "dataset LIVE tunggal"

Fitur ini sudah digantikan oleh **Daerah Live** (`/api/live/areas`, migrasi 025–026). Kodenya masih tersisa dalam satu rantai:

| ID | Lokasi | Status | Risiko |
|---|---|---|---|
| E1 | [etl/live_scheduler.py:108-285](../etl/live_scheduler.py#L108-L285): `run_daily_check`, `_check_and_ingest_source/_sentinel1/_modis/_gpm`, `_update_source_check`, `_create_live_job`, `_bbox_tuple`, `handle_backfill_request` (~175 baris) | **Tidak terjangkau.** Tidak dijadwalkan (komentar baris 57–59), dan `handle_backfill_request` tidak dipanggil siapa pun. Komentar menyebut kode ini dipertahankan "untuk endpoint lama /api/live/backfill", padahal endpoint itu langsung memanggil `DatasetManager.trigger_live_backfill`, bukan kode ini. | **Low** |
| E2 | [api/routes/live.py:31-121](../api/routes/live.py#L31-L121): `GET /api/live`, `POST /api/live/toggle`, `POST /api/live/clear`, `POST /api/live/backfill`, `GET /api/live/scenes` | Ter-mount, tetapi tidak dipanggil UI maupun test. Komentar baris 126: "dipertahankan untuk kompatibilitas". | Medium (perubahan API publik) |
| E3 | [api/schemas.py:531-586](../api/schemas.py#L531-L586): `LiveSourceItem`, `LiveStatusResponse`, `LiveToggleRequest/Response`, `LiveClearResponse`, `LiveBackfillRequest/Response`, `LiveSceneItem` | Hanya dipakai E2. | Medium (ikut E2) |
| E4 | [etl/dataset_manager.py:425-430](../etl/dataset_manager.py#L425-L430) `get_live_dataset`, [:612-666](../etl/dataset_manager.py#L612-L666) `toggle_live`, `clear_live_dataset`, `trigger_live_backfill` | Hanya dipakai E1/E2. | Medium |
| E5 | [etl/database_client.py:1155-1168](../etl/database_client.py#L1155-L1168) ORM `LiveDatasetSource` + tabel `live_dataset_sources` | Hanya dipakai E1/E2. | **High**: menghapus tabel butuh migrasi baru. |
| E6 | Kolom `datasets.live_enabled`, `live_last_checked_at`, `dataset_kind='LIVE'` | Masih diserialisasi di `dataset_manager.py:1276` dan `schemas.py:342`. | **High**: kolom DB; biarkan saja. |

Urutan yang aman: E1 dulu (tanpa dampak), lalu E2+E3+E4 sekaligus jika API lama memang tidak dijanjikan ke pengguna eksternal. E5/E6 sebaiknya ditunda.

---

## F. API: router tak di-mount & endpoint tak dipakai UI

| ID | Lokasi | Status | Risiko | Dampak |
|---|---|---|---|---|
| F1 | [api/routes/preview.py](../api/routes/preview.py) (267 baris) | **Tidak di-mount** di `api/main.py` (komentar baris 144–146: "sudah dipensiunkan"). Tidak di-import siapa pun. Kodenya tidak terjangkau sama sekali. | **Low** | Perbarui rujukan di `DOCS/DECISIONS.md:184` dan `DOCS/INTERFACE.md:286`. Pillow **tetap diperlukan** oleh module10. |

Endpoint berikut ter-mount tetapi **tidak dipanggil `web/app.js`**. Ini belum tentu residu; tergantung apakah REST API memang ditawarkan ke pihak luar (Swagger di `/docs`). Ini keputusan produk.

| Grup endpoint | File | Ada test? |
|---|---|---|
| `/api/scenes`, `/api/scenes/{id}`, `/api/scenes/{id}/status` | `api/routes/scenes.py` | Ya |
| `/api/products/*` (list, detail, download, verify) | `api/routes/products.py` | Ya |
| `/api/quality/{scene_id}`, `/api/quality/summary/stats` | `api/routes/quality.py` | Ya |
| `/api/metadata/lineage/{product_id}` | `api/routes/lineage.py` | Ya |
| `/api/storage/*` (summary, files, cleanup, cleanup/partial) | `api/routes/storage.py` | **Tidak** |
| `/api/pipeline/status/current` | `api/routes/pipeline.py` | **Tidak** |
| `/api/datasets/{id}/metadata` | `api/routes/datasets.py:288` | Ya |
| `/api/live/areas/{id}/events`, `/api/live/areas/{id}/log`, `GET /api/live/areas/{id}` | `api/routes/live.py:148,220,235` | **Tidak** |
| `GET /api/merge/preview/{date_key}` (list) | `api/routes/merge.py:138` | Ya |

Risiko: **Medium**. `/api/storage/cleanup` bisa menghapus file. Kalau endpoint ini tidak dipakai UI dan tidak punya test, pertimbangkan untuk menonaktifkannya sebelum rilis publik, karena API tidak memiliki autentikasi.

---

## G. Unused import & variabel (pyflakes)

Semua berisiko **Low** dan tidak berdampak runtime, kecuali yang ditandai.

**Aplikasi**

| Lokasi | Import tak terpakai |
|---|---|
| [etl/database_client.py:11](../etl/database_client.py#L11) | `Computed`, `Float`, `JSON` |
| [etl/lineage_tracker.py:16-21](../etl/lineage_tracker.py#L16-L21) | `Any`, `IntegrityError`, `ProcessingJob` |
| [etl/metadata_manager.py:7-9](../etl/metadata_manager.py#L7-L9) | `Any`, `Session` |
| [etl/module8_gpm_download.py:45-46](../etl/module8_gpm_download.py#L45-L46) | `from_origin`, `reproject` (C22) |
| [etl/refusion.py:41](../etl/refusion.py#L41) | `Path` |
| [etl/seed_data.py:22](../etl/seed_data.py#L22) | `JobStatusEnum`, `ProcessingStage`, `RegionOfInterest`, `SatelliteScene`, `StorageLocationEnum` |
| [api/schemas.py:5](../api/schemas.py#L5) | `UUID` |
| [api/routes/preview.py:19-23,66](../api/routes/preview.py#L19) | `StreamingResponse`, `ProductTierEnum`, `ImageFont` (file ini sendiri mati, F1) |
| [api/routes/products.py:25](../api/routes/products.py#L25) | `MetadataManager` |
| [api/routes/quality.py:22,29,118](../api/routes/quality.py#L22) | `ProductTierEnum`, `MetadataManager`, `and_` |
| [api/routes/scenes.py:11-25](../api/routes/scenes.py#L11-L25) | `timezone`, `Annotated`, `and_`, `Session`, `ProductTierEnum`, `QualityMetric` |
| [api/routes/storage.py:17-18](../api/routes/storage.py#L17-L18) | `os`, `shutil` |

**Test**: `conftest.py:18` (`MagicMock`), `test_api_endpoints.py:16`, `test_credentials.py:4`, `test_dataset_merge.py:15` (`GridMismatch`, `MergeCandidate`, `StackInfo`), `test_etl_pipeline.py:16`, `test_quality_metrics.py:15-16`, `test_reference_layer_staleness.py:17`. Ada juga variabel lokal tak terpakai di `test_etl_pipeline.py:123,145,199` dan 18 f-string tanpa placeholder di `test_credentials.py`.

**False positive (jangan dihapus):**
- `api/main.py:22` `get_db`: re-export yang disengaja (lihat komentar baris 19–21).
- `etl/module7_modis_download.py:641` `import pyhdf.SD  # noqa`: pemeriksaan ketersediaan library.
- `import etl` di `test_fusion_grid.py`, `test_gpm_native_resolution.py`, `test_modis_quality_bands.py`, `test_try8_regressions.py`: kemungkinan dipakai untuk side-effect `etl/__init__.py` (strip `PROJ_LIB`, `load_dotenv`). Jalankan ulang test setelah menghapus.

---

## H. Frontend (web/)

| ID | Lokasi | Tipe | Alasan | Risiko |
|---|---|---|---|---|
| H1 | [web/app.js:2](../web/app.js#L2) `STAGE_ORDER` | Unused const | Didefinisikan, tidak pernah dibaca. Komentar baris 1 terpotong. | Low |
| H2 | [web/app.js:3-7](../web/app.js#L3-L7) | Komentar yatim | Menjelaskan "palet tier" yang konstantanya sudah tidak ada. Komentar baris 17 juga merujuk `TIER_ORDER di atas` yang tidak ada. | Low |
| H3 | [web/app.js:37](../web/app.js#L37) `tierLabel()` | Unused function | Nol pemanggil. `TIER_LABEL` (baris 32) hanya dipakai oleh fungsi ini. | Low |
| H4 | [web/app.js:51](../web/app.js#L51) `SOURCELESS_TIERS` | Unused const | Nol pemakai. | Low |
| H5 | `web/style.css`, ±55 class tanpa pemakai | Unused CSS | Kelompok terbesar: panel "Struktur" versi lama, karena `renderStructurePanel` sekarang memakai `.struct-files`. Lihat daftar di bawah. | Low |

Daftar class CSS tanpa pemakai di HTML/JS (sudah dikurangi class dinamis):

- **Struktur lama** ([style.css:797-844](../web/style.css#L797), [:1478-1523](../web/style.css#L1478)): `.struct-legend`, `-legend-item`, `-legend-size`, `.struct-rows`, `.struct-row`, `.struct-head`, `.struct-tier`, `.struct-total`, `.struct-count`, `.struct-track`, `.struct-bar`, `.struct-seg`, `.struct-seg-mixed`, `.struct-chips`, `.struct-chip`, `.struct-chip-size`, `.struct-tree`, `.struct-source`, `-source-name`, `-source-size`, `.struct-level`, `-level-size`, `-level-name`, `.struct-leaf`, `-leaf-name`, `-leaf-size`, `-leaf-count`
- **Galeri preview lama** ([style.css:984-1027](../web/style.css#L984)): `.preview-card`, `.preview-thumb`, `.preview-zoom`, `.preview-label`, `.preview-tags`, `.preview-tag`, `.preview-note`
- **Live dataset lama** ([style.css:710-726](../web/style.css#L710)): `.live-grid`, `.live-toggle-row`, `.switch-track`, `.live-meta-row`, `.source-row`, `.source-name`, `.source-meta`
- **Mask gallery lama** ([style.css:1604-1622](../web/style.css#L1604)): `.mask-card`, `.mask-noimg`, `.mask-title`, `.mask-interp`
- **Lainnya**: `.glass` (25), `.muted` (729), `.qual-rows/.qual-row/.qual-src` (853-855), `.app-footer`, `.app-footer-slogan` (870-880), `.card-tiers` (631), `.option-icon` (1148), `.lm-sentence` (1894), `.lm-image-modal` (1931)

Bukan residu (dirakit dinamis): `.lm-row-2`, `.lm-row-3` (`'lm-row-' + row.length`), `.src-user`, `.src-geocode` (`'src-' + source`). Tidak ditemukan kode yang di-comment-out maupun `console.log`/`debugger` di `web/`.

---

## I. Dependency tak terpakai (requirements.txt)

| Paket | Baris | Alasan | Risiko | Catatan |
|---|---|---|---|---|
| `alembic==1.14.0` | 5 | Tidak ada `import alembic` dan tidak ada folder `alembic/`. Migrasi berjalan lewat `database/run_migration.py` + SQL mentah. `DOCS/ARCHITECTURE.md:26` sudah menyatakan ini. | Low | — |
| `xarray==2024.11.0` | 21 | Nol import. | Low | — |
| `dask[array]==2024.11.2` | 22 | Nol import. | Low | — |
| `pandas==2.2.3` | 23 | Nol import langsung. | Low | Ikut terpasang lagi kalau seaborn/xarray dipertahankan. |
| `seaborn==0.13.2` | 25 | Nol import. | Low | — |
| `tenacity==9.0.0` | 40 | Nol import. Retry diimplementasi manual di `download_guard.py`. | Low | — |
| `python-multipart==0.0.18` | 9 | Tidak ada endpoint dengan `Form()`/`UploadFile`. | Low | Perlu dipasang lagi kalau nanti ada endpoint upload. |

Dependency yang **tetap diperlukan** walau tidak di-import langsung: `psycopg2-binary` (driver `postgresql+psycopg2://`), `uvicorn` (server), `httpx` (`fastapi.testclient`), `pytest-asyncio` (`@pytest.mark.asyncio` di `test_mask_gallery.py`), `pytest-cov` (opsional, disebut di `conftest.py:10`), `pypdf` (test report).

Setelah requirements diubah, perbarui juga tabel stack di `DOCS/ARCHITECTURE.md:18-27`.

---

## J. Urutan penghapusan yang disarankan

Setiap langkah ditutup dengan verifikasi: `pytest tests` → harus tetap 754 passed (atau berkurang tepat sejumlah test yang sengaja dihapus), `compileall`, dan `node --check`. Setelah itu smoke test manual UI: buat dataset, buka galeri preview, buka panel Live.

1. **Keamanan dulu**: A1 (verify_prompt2 + ganti password), A5 (test_results.txt), B17 (log backup), A6 (CORS).
2. **Low tanpa dampak**: B1–B4, B7–B11, B14–B16, B19–B20, C1–C15, C17–C22, E1, F1, G (aplikasi), H1–H5, I.
3. **Untrack data**: B5 (`git rm --cached` JSON + `.gitignore`); B6 setelah verifikasi refusion.
4. **Keputusan produk**: E2–E4 (API live lama), endpoint F tanpa test (terutama `/api/storage/cleanup`), B18 (old_ref), B22 (config lama).
5. **Butuh migrasi/penyesuaian test**: D1–D13, C16, E5. Kerjakan terakhir, atau biarkan saja.
6. **Perbaikan non-residu**: A3/A4 (Docker + schema.sql) sebelum ada yang mencoba jalur Docker.

Catatan riwayat git: menghapus file tidak menghapusnya dari riwayat. Kalau repo akan dipublikasikan beserta riwayatnya, kredensial di A1 dan isi `backup/` tetap bisa dibaca dari commit lama.
