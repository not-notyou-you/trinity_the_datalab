# Laporan Penghapusan Residu — The DataLab

Tanggal: 2026-10-02 · Branch: `feat/reference-layers-and-fusion-grid` · Basis: commit `3b6beda`
Daftar temuan awal: [AUDIT_RESIDU.md](AUDIT_RESIDU.md) (ID B/C/E/F/G/H/I di bawah merujuk ke sana)

**Status: belum di-commit.** File yang dihapus sudah di-stage (`git rm`), sedangkan file yang diubah belum di-stage. Semuanya bisa di-review dengan `git diff` dan `git diff --cached`.

---

## Ringkasan

| | Jumlah |
|---|---|
| Item dieksekusi (masing-masing diverifikasi terpisah) | **67** |
| ✅ Berhasil dihapus | 67 |
| ⚠️ Di-rollback | **0** |
| ❌ Ditahan (HOLD) | 26 kelompok |
| 🔍 Residu baru ditemukan | 7 |
| File dihapus | 30 |
| File diubah | 36 (−798 / +32 baris) |

Kondisi akhir dibandingkan baseline:

| Pemeriksaan | Baseline | Akhir |
|---|---|---|
| `pytest tests` | 754 passed | **754 passed** |
| `compileall` / `node --check` | OK | OK |
| Smoke test HTTP (14 URL, termasuk `/`, `/app`, aset statis, 6 endpoint API) | semua 200 | semua 200 |
| Chrome headless: error console di `/` dan `/app` | 0 | **0** |
| Computed style setiap elemen (679 elemen, termasuk `::before`/`::after`) | — | **IDENTIK** dengan baseline |
| Endpoint OpenAPI | 70 | **70** (tidak ada yang hilang/bertambah) |
| Hash dokumen OpenAPI lengkap | `f4c364485a4909cc` | **`f4c364485a4909cc`** |
| `LiveScheduler.start()` → job terdaftar | `live_areas_check` | `live_areas_check` |

---

## Protokol yang dijalankan

**1. Analisis per item, sebelum dihapus:**
- grep rujukan di `etl/ api/ database/ tests/ web/`, skrip root, dan `DOCS/`;
- target `mock.patch`/`monkeypatch.setattr` di test, karena patch berbasis string akan rusak tanpa terlihat grep biasa;
- akses dinamis (`getattr`, `importlib`, `__import__`, `__all__`);
- pola re-export (`from modul import X` atau `modul.X` dari file lain);
- efek samping constructor dan import modul;
- untuk CSS, class yang dirakit dinamis (`'lm-row-' + n`) dan selector gabungan.

**2. Penghapusan satu item per kali.** Penghapusan Python memakai rentang AST, sehingga decorator dan komentar yang menempel ikut terhapus dengan presisi. CSS dihapus secara selector-aware: hanya selector yang memuat class target yang dibuang dari daftar selector, dan `@media` yang kosong ikut dihapus.

**3. Verifikasi setelah setiap item:**
- `compileall` dan pyflakes untuk menangkap *undefined name* baru;
- `node --check`;
- **seluruh** test suite (754 test);
- menjalankan aplikasi (uvicorn) dan smoke test 14 URL.

Item frontend mendapat dua lapis tambahan: Chrome headless untuk error console runtime, dan perbandingan `getComputedStyle` per elemen lewat Chrome DevTools Protocol. Item dependency diuji dengan **pemblokir import** yang mensimulasikan paket tidak terpasang (`ModuleNotFoundError`), karena venv yang sudah ada akan selalu lolos tes.

**Kontrol negatif** dijalankan untuk membuktikan alat-alat ini memang bisa gagal:
- Browser check menangkap `ReferenceError: tierLabel is not defined` pada halaman uji.
- Pembanding style menangkap perubahan `letter-spacing: 0.3px` di 16 elemen.
- Pemblokir benar-benar membuat `import pandas` gagal.

**4. Cascade** (import, atribut, konstanta, komentar, atau docs yang menjadi yatim *akibat* penghapusan) dibersihkan di dalam item yang sama, lalu diverifikasi ulang.

---

## ✅ DELETED

### File

| # | Item | Lokasi | Catatan |
|---|---|---|---|
| 1 | B13 | `test_results.txt` | Output pytest lama, bocorkan path `C:\Users\hakim\...` |
| 2 | B10 | `tests/verify_prompt2.py` | **Berisi password DB hardcoded.** Lihat catatan keamanan di bawah. |
| 3 | B9 | `tests/verify_prompt4.py` + `tests/test_prompt4.ps1` | Satu item karena `.ps1` hanya menjalankan `verify_prompt4.py` |
| 4 | B7 | `debug_prompt4.py` | |
| 5 | B8 | `tests/debug_prompt4.py` | |
| 6 | B11 | `tests/preview_gold.py` | Fungsi `render_preview` lain di `land_mask`/`water_occurrence` adalah definisi terpisah, bukan pemakai file ini |
| 7 | B14 | `package-lock.json` | Tidak ada `package.json` maupun `node_modules` |
| 8 | B15 | `logo.webp` (root) | Identik dengan `web/logo.webp`. Tidak dirujuk kode Python (termasuk generator PDF). `/logo.webp` tetap 200 dari `web/`. |
| 9 | B16 | `config/docker-compose.yml` | Cascade: kalimat "duplicated verbatim" di `DOCS/ARCHITECTURE.md` diperbarui |
| 10 | B17 | `backup/` (19 file) | Hanya *ditulis* oleh `migrate_data_structure.py` (`copytree` membuat folder sendiri), tidak pernah dibaca. Dataset 7 sudah tidak ada. |
| 31 | F1 | `api/routes/preview.py` (267 baris) | Router tidak di-mount. Cascade: `DOCS/DECISIONS.md:184`, `DOCS/INTERFACE.md:286`, dan komentar Pillow di `requirements.txt` diperbarui. |

### Kode Python

| # | Item | Lokasi (sebelum dihapus) | Cascade yang ikut dibersihkan |
|---|---|---|---|
| 11 | C1 | `etl/metadata_manager.py:486-548` `query_latest_scenes` | import `timedelta`. **Bug `NameError: has_gold` (A2) ikut hilang.** |
| 12 | C3 | `etl/module6_analytics.py:110-132` `generate_quality_plot` | — |
| 13 | C4 | `etl/lineage_tracker.py:195-260` `record_full_pipeline` | — |
| 14 | C5 | `etl/land_mask.py:80-96` `grid_from_fusion_h5` | — |
| 15 | C6 | `etl/report_generator.py:314-322` `_s1_orbit_counts` | import `func`, `select`, `DataProduct`, `SatelliteScene` |
| 16 | C7 | `etl/module8_gpm_download.py:188-189` `_daily_granule_url` | — (`_run_base_url`/`_daily_granule_filename` masih dipakai) |
| 17 | C22 | `etl/module8_gpm_download.py:45-46` import `from_origin`, `reproject` | — |
| 18 | C8 | `etl/tier_names.py` `is_legacy`, `display_tier`, `tier_at_rank` | — (`canonical_tier`, `LEGACY_TIERS` masih dipakai) |
| 19 | C9 | `etl/folder_manager.py:855-856` `get_fusion_scene_files` | Docstring `folder_manager.py:882` yang merujuknya diperbarui |
| 20 | C10 | `etl/dataset_merge.py:109-110` `LayerMismatch` | — |
| 21 | C11 | `etl/fusion_strategies.py:76` `STRATEGIES` | — |
| 22 | C12 | `etl/module7_modis_download.py:112-116` `MODIS_TILES` + komentarnya | — (perilaku "tile dari AOI" tetap terdokumentasi di dekat kodenya) |
| 23 | C13 | `etl/module10_generate_preview.py:119-120` `MAX_WIDTH` | — |
| 24 | C15 | `etl/database_client.py:1329-1347` `session_with_retry` | import `time`, `OperationalError`, konstanta kelas `_RETRY_ATTEMPTS`/`_RETRY_BACKOFF` |
| 25 | C17 | `etl/config.py` `APIConfig`, `PipelineConfig`, `Config`, `cfg` | import `json`, `Path`. Docstring diperbarui. **`DatabaseConfig` dan `load_dotenv()` dipertahankan.** `run_migration.py` diuji dalam mode tanpa eksekusi SQL. |
| 26 | C18 | `api/schemas.py` `SceneQueryParams`, `DatasetJobItem`, `CleanupOperationItem` | — |
| 27 | C19 | `api/schemas.py:280-282` alias `DatasetCreateRequest` | — |
| 28 | C20 | `api/routes/storage.py:80` lokal `pattern` | — (`_dir_info` diuji langsung untuk 3 cabangnya) |
| 29 | C21 | `etl/report_generator.py:906,1063` lokal `ds` | — |
| 30 | E1 | `etl/live_scheduler.py:108-285`: `run_daily_check` + 8 helper (~200 baris) | 15 import, atribut `_dsmgr`/`_plog`, konstanta `_CHECK_HOUR`/`_DEFAULT_LOOKBACK_DAYS`, `_now()`. Komentar di `start()` dan `DOCS/PIPELINE.md:340` diperbarui. Constructor `DatasetManager`/`PipelineLogger` sudah dipastikan tanpa efek samping. Scheduler diuji langsung. |

### Unused import (G)

| # | File | Nama |
|---|---|---|
| 32 | `etl/database_client.py` | `Computed`, `Float`, `JSON` |
| 33 | `etl/lineage_tracker.py` | `Any`, `IntegrityError`, `ProcessingJob` |
| 34 | `etl/metadata_manager.py` | `Any`, `Session` |
| 35 | `etl/refusion.py` | `Path` |
| 36 | `etl/seed_data.py` | `JobStatusEnum`, `ProcessingStage`, `RegionOfInterest`, `SatelliteScene`, `StorageLocationEnum` |
| 37 | `api/schemas.py` | `UUID` |
| 38 | `api/routes/products.py` | `MetadataManager` |
| 39 | `api/routes/quality.py` | `ProductTierEnum`, `MetadataManager`, `and_` (lokal) |
| 40 | `api/routes/scenes.py` | `timezone`, `Annotated`, `and_`, `Session`, `ProductTierEnum`, `QualityMetric` |
| 41 | `api/routes/storage.py` | `os`, `shutil` (penghapusan file di router ini memakai `Path.unlink`) |
| 42 | `tests/conftest.py` | `MagicMock` (tidak ada `from conftest import`) |
| 43 | `tests/test_api_endpoints.py` | `datetime`, `timezone` |
| 44 | `tests/test_credentials.py` | `json` |
| 45 | `tests/test_dataset_merge.py` | `GridMismatch`, `MergeCandidate`, `StackInfo` |
| 46 | `tests/test_etl_pipeline.py` | `datetime`, `timezone` |
| 47 | `tests/test_quality_metrics.py` | `os`, `tempfile` |
| 48 | `tests/test_reference_layer_staleness.py` | `pytest` |

### Frontend

| # | Item | Lokasi | Verifikasi tambahan |
|---|---|---|---|
| 49 | H1 | `web/app.js:1-2` `STAGE_ORDER` | browser: 0 error |
| 50 | H2 | `web/app.js:3-7` komentar palet tier yatim; komentar `STORAGE_TIER_ORDER` yang merujuk `TIER_ORDER` (tidak ada) ditulis ulang | browser: 0 error |
| 51 | H4 | `web/app.js:49-51` `SOURCELESS_TIERS` | browser: 0 error |
| 52 | H5 | CSS galeri preview lama: `.preview-card/-thumb/-zoom/-label/-tags/-tag/-note` (13 rule) | style **IDENTIK** |
| 53 | H5 | CSS UI live lama: `.live-grid` (+`@media`), `.live-toggle-row`, `.switch-track` (4 rule), `.live-meta-row`, `.source-row/-name/-meta`, `.dot.muted` | style **IDENTIK** |
| 54 | H5 | CSS mask lama: `.mask-card` (+img, figcaption), `.mask-noimg`, `.mask-title`, `.mask-interp` | style **IDENTIK** |
| 55 | H5 | `.qual-rows`, `.qual-row`, `.qual-src` | style **IDENTIK** |
| 56 | H5 | `.app-footer`, `.app-footer-slogan` + komentar yatimnya | style **IDENTIK** |
| 57 | H5 | `.card-tiers` | style **IDENTIK** |
| 58 | H5 | `.option-icon` (+svg) | style **IDENTIK** |
| 59 | H5 | `.lm-sentence` | style **IDENTIK** |
| 60 | H5 | `.lm-image-modal img` | style **IDENTIK** |

`web/style.css`: 1946 → 1837 baris. Commit `2530c3c` sudah dipastikan menggantikan markup lama (`preview-card`, `mask-card`, `lm-sentence`) dengan komponen baru (`lm-row`, `mask-grid`), jadi pencopotannya disengaja.

### Dependency (`requirements.txt`)

| # | Paket | Verifikasi |
|---|---|---|
| 61 | `alembic==1.14.0` | graf dependency: tidak dibutuhkan paket lain · pytest + app + browser lulus dengan import diblokir |
| 62 | `xarray==2024.11.0` | sama |
| 63 | `dask[array]==2024.11.2` | sama |
| 64 | `seaborn==0.13.2` | sama (dihapus sebelum pandas karena bergantung padanya) |
| 65 | `pandas==2.2.3` | sama; test laporan PDF (matplotlib + reportlab) tetap lulus |
| 66 | `tenacity==9.0.0` | sama |
| 67 | `python-multipart==0.0.18` | sama; Starlette menangani ketiadaannya (`except ModuleNotFoundError`), dan tidak ada endpoint `Form`/`UploadFile` |

Cascade: tabel stack di `DOCS/ARCHITECTURE.md` diperbarui (baris Data, Resilience, Visualization, DB driver, System). Paket-paket itu **tetap terpasang di `venv/`**; requirements hanya berpengaruh pada instalasi baru.

### Dokumentasi yang diubah sebagai cascade

`DOCS/ARCHITECTURE.md` (compose duplikat, tabel stack), `DOCS/DECISIONS.md:184`, `DOCS/INTERFACE.md:286`, `DOCS/PIPELINE.md:340`, `requirements.txt` (komentar Pillow).

---

## ❌ NOT DELETED (HOLD)

| Item | Lokasi | Alasan ditahan |
|---|---|---|
| B1, B2 | `_copy_wajo_s1a_by_year.py`, `_export_s1a_split.py` | Asal-usul (provenance) data yang masih ada: `data/datasets/wajo_processed_hybrid_only/` dan `_export_s1a_only/`. Disarankan dipindah ke `scripts/research/`, bukan dihapus. |
| B3 | `_refusion_run_46.py` | Provenance `_refusion_backup/46/` yang masih ditahan. |
| B4 | `_s1_coverage_audit.py`, `_s1_coverage_detail.csv` | Hasil analisis riset (klasifikasi cakupan). Mungkin dipakai di tesis. |
| B5 | `_export_s1a_only/` (67 GB, 227 JSON ter-track) | **Data riset.** Tidak bisa dipulihkan kalau ternyata belum dicadangkan. Minimal: `git rm --cached` untuk JSON-nya. |
| B6 | `_refusion_backup/` (7,5 GB, untracked) | Untracked, jadi tidak bisa dipulihkan dari git. Hapus setelah hasil refusion dataset 46 diverifikasi. |
| B12 | `tests/verify_pipeline_run.py` | Skrip verifikasi manual yang mungkin masih berguna. |
| B18 | `old_ref/` | Memuat `THESIS_CHAPTERS.md`/`PRD.md` yang mungkin masih dipakai untuk tesis. |
| B19 | `report/` | **Masih ada dependency**: `report/report-detailed.md` dikutip docstring `report_generator` dan `DOCS/REPORT.md` (N3). |
| B20 | `DOCS/do.zip` | Untracked, jadi tidak bisa dipulihkan. |
| B21 | `analytics/`, `checkpoints_pipeline/` | Terikat ke `.env.example` dan `.gitignore` (N4). Manfaatnya kecil. |
| B22 | `config/config.json`, `config_locations.json` | Ditandai `_DEPRECATED` tapi masih disebut sebagai rujukan koordinat asal di `DOCS/README.md`/`PIPELINE.md`. Butuh keputusan Anda. |
| C2 | `etl/module9_fusion.py` `_aoi_reference_grid` | Docstring-nya memuat alasan desain yang **dirujuk 3 komentar lain** (baris 504, 1612, 1750: "alasannya sudah ditulis di _aoi_reference_grid"). Menghapusnya akan menghilangkan alasan tersebut. |
| C14 | `etl/module9_fusion.py` `FUSION_LAYERS` | Dirujuk sebagai contoh peringatan oleh komentar di module5:707, module9:680, module10:76. |
| C16 | 8 enum di `etl/database_client.py:134-197` | Berfungsi sebagai dokumentasi nilai DB yang valid. |
| C19 | alias `DatasetResponse` | Sengaja dibuat untuk menyelaraskan dengan nama di `DOCS/INTERFACE.md`. |
| C21 | `etl/report_stats.py:189` parameter `root_path` | Mengubah signature butuh analisis pemanggil. |
| C21 | `etl/report_generator.py` `n_charts` | Penghitung yang tidak dibaca. Lebih mirip logika yang belum selesai daripada residu (N6). |
| D1–D13 | Fungsi yang hanya dipakai test | Menghapusnya berarti menghapus test juga. `create_tables` wajib ada untuk fixture. |
| E2–E4 | 5 endpoint `/api/live` lama + schema + method `DatasetManager` | API publik (terlihat di `/docs`). Butuh keputusan produk. |
| E5–E6 | ORM `LiveDatasetSource`, tabel `live_dataset_sources`, kolom `live_*` | Butuh migrasi DB. |
| F (endpoint) | `/api/storage/*`, `/api/pipeline/status/current`, dll. | Keputusan produk. **`/api/storage/cleanup` bisa menghapus file dan API tidak punya autentikasi.** |
| G | `import etl` di 4 file test | Mungkin dipakai untuk efek samping `etl/__init__.py` (strip `PROJ_LIB`, `load_dotenv`). |
| G | `api/main.py` `get_db`, `module7` `pyhdf.SD` | Disengaja (re-export dan pemeriksaan ketersediaan library). |
| H3 | `web/app.js` `tierLabel()` + `TIER_LABEL` | Pemakaiannya dicopot di commit `684602e`, tetapi `DOCS/INTERFACE.md:572-575` masih mendeskripsikan fitur itu. **Bisa jadi regresi UI** (N2). |
| H5 | CSS `struct-*` (27 class) | Satu paket dengan H3: tampilan pohon struktur yang dicopot bersamaan. |
| H5 | `.glass` | Utility design system yang terdokumentasi di `DOCS/INTERFACE.md:468`. |

---

## 🔍 NEWLY FOUND (dicatat, tidak dihapus)

| ID | Lokasi | Temuan | Ditemukan saat |
|---|---|---|---|
| N1 | `etl/module6_analytics.py:110` `run()` | Tidak dipanggil siapa pun (module5 hanya mengimpor `compute_band_metrics`). Lolos dari vulture karena nama `run` dipakai di banyak modul lain. Kemungkinan modul lain juga punya `run()` yatim. | Item 12 |
| N2 | `DOCS/INTERFACE.md:572-575` | Mendeskripsikan panel struktur bertipe pohon source→level→tier dengan label `tierLabel()`, yang sudah dicopot di `684602e`. **Perlu dicek: dokumentasinya basi, atau fiturnya hilang tanpa sengaja?** | Analisis H3 |
| N3 | `report/report-detailed.md` | Masih menjadi rujukan aktif (docstring `report_generator`, `DOCS/REPORT.md`), jadi `report/` bukan residu murni. | Analisis B19 |
| N4 | `.env.example` | `ANALYTICS_DIR`, `CHECKPOINT_DIR`, `OUTPUT_DIR`, `API_DEBUG` tidak dibaca kode mana pun lagi. Sebelumnya hanya dibaca `PipelineConfig`/`APIConfig` (item 25), dan `API_DEBUG` memang tidak pernah dipakai. Juga `AWS_*`, `GCS_*`, `GOOGLE_APPLICATION_CREDENTIALS` (tidak ada integrasi cloud). | Item 25 |
| N5 | `old_ref/SENTINEL_SENTINEL_DOCUMENTATION.md:829` | Mengklaim ada retry DB otomatis lewat `session_with_retry`. Fungsi itu (kini dihapus) **ternyata cacat**: `@contextmanager` yang melakukan `yield` lebih dari sekali akan memunculkan `RuntimeError` saat retry, jadi fitur tersebut tidak pernah benar-benar bekerja. | Item 24 |
| N6 | `etl/report_generator.py:1087-1097` | `n_charts` dihitung tetapi tidak pernah dibaca. Kemungkinan ringkasan "N chart" yang belum selesai. | Item 29 |
| N7 | `web/style.css` `.switch`, `.switch input`, `.dot`, `.dot.ok` | Sisa UI "dataset LIVE tunggal". Tidak ada elemen yang memakainya (`status-dot`/`lm-dot` adalah class lain). Lolos dari pemindai awal karena kata "dot"/"switch" muncul di komentar JS. | Item 53 |

---

## ⚠️ ROLLBACK HISTORY

**Tidak ada item yang di-rollback.** Ke-67 item lulus verifikasi pada percobaan pertama.

Ada dua masalah yang tertangkap **sebelum** menyentuh file proyek. Keduanya ada pada alat bantu, bukan pada kode aplikasi:

| Kejadian | Ditangkap oleh | Tindakan |
|---|---|---|
| Alat penghapus CSS (`css_rm.py`) versi pertama ikut merapikan baris kosong di 4 lokasi yang tidak berhubungan (baris 324, 765, 782, 1122). | Dry-run pada salinan `style.css` di scratchpad, lalu `diff` terhadap aslinya. | Logika perapian global dibuang. Dry-run ulang memastikan hanya blok target yang berubah, baru kemudian dijalankan pada file proyek. |
| Simulasi "paket tidak terpasang" versi pertama melempar `ImportError`, sehingga FastAPI tampak gagal tanpa `python-multipart`. | Pembacaan kode Starlette: yang ditangkap adalah `ModuleNotFoundError`, yang memang muncul pada paket yang benar-benar tidak ada. | Simulasi diperbaiki agar setia (`ModuleNotFoundError`). Dengan simulasi yang benar, aplikasi berjalan normal. |

---

## Yang masih terbuka dari audit awal (bukan penghapusan)

- **A1:** file berisi password sudah dihapus, tetapi password `12345678` **masih ada di riwayat git**. Ganti password itu di mesin mana pun yang memakainya.
- **A3/A4:** `schema.sql` yang usang dan Dockerfile yang tidak ada (jalur Docker rusak).
- **A6:** CORS `allow_origins=["*"]`.

## Cara membatalkan

Semua perubahan belum di-commit. Gunakan perintah ini **hanya** jika ingin membatalkan:
- Satu file: `git restore --staged --worktree <path>`
- Semuanya: `git restore --staged --worktree -- . ':!DOCS/AUDIT_PENGHAPUSAN.md'`

Skrip bantu verifikasi (verify.sh, browser check, pembanding computed-style, pemblokir import) ada di scratchpad sesi dan tidak ikut ke repo.
