# Architecture Decisions

## D1: Fusion Strategy as User Parameter (Not Hardcoded)

**Decision**: Let users choose CO_OCCURRENCE, FULL_COVERAGE, or HYBRID at dataset creation time.

**Why**: The "best" fusion strategy depends on the research question. Co-occurrence gives perfect temporal alignment (better for ML training) but fewer samples. Full coverage maximizes temporal density (better for monitoring). Making this a user parameter eliminates the need to pick one — and is the core novelty of DataLab (no competitor offers this).

**Trade-off**: More complex pipeline orchestration. Fusion stage must branch on strategy.

**Status implementasi**: strategi baru benar-benar bercabang sejak
`etl/fusion_strategies.py` ada. Sebelum itu `fusion_strategy` cuma *dicatat*
(ditulis ke `f.attrs["fusion_strategy"]` dan `fusion_products`) tapi tidak
pernah jadi percabangan, dan seluruh pipeline berjangkar pada scene S1 —
sehingga ketiga strategi menghasilkan berkas yang identik kecuali satu atribut
teks. Klaim "core novelty" di atas baru benar setelah itu.

Strategi terurai jadi **dua sumbu** — unduh (tanggal aux mana diambil) dan
rakit (tanggal mana jadi berkas HDF5). HYBRID adalah strategi ketiga yang sah
justru karena memilih sumbu berbeda dari masing-masing: unduh seperti
FULL_COVERAGE, rakit seperti CO_OCCURRENCE. Lihat DOCS/ETL.md "Strategies: two
axes, not one" untuk tabelnya.

## D2: Per-Satellite Processing Level (Not Global Toggle)

**Decision**: Each satellite has its own RAW/PROCESSED definition, configured independently. Users can request RAW, PROCESSED, or both per source.

**Why**: "Processing" means fundamentally different things for each sensor. Sentinel-1 RAW vs PROCESSED is about speckle filtering (Lee filter ablation). MODIS RAW vs PROCESSED is about whether derived indices (NDVI, NDWI) are computed. GPM RAW vs PROCESSED is about single-day rainfall vs multi-day accumulation windows. A single global toggle would force all three to the same level, preventing mixed configurations like "filtered S1 + raw MODIS flood map + accumulated GPM rainfall."

**Per-satellite definitions**:
- **S1 RAW**: Calibrate + reproject + crop (no Lee filter, no QA) → ALIGNED
- **S1 PROCESSED**: + Lee filter 7×7 + QA analytics + COG export → DESPECKLED → COG
- **MODIS RAW**: Flood map only (no NDVI/NDWI) → ALIGNED
- **MODIS PROCESSED**: + NDVI + NDWI from reflectance → INDICES → COG
- **GPM RAW**: Daily rainfall only → ALIGNED
- **GPM PROCESSED**: + 24h/72h/7d accumulation → ACCUMULATED → COG

**Trade-off**: More complex UI (per-source checkboxes instead of one toggle) and more complex pipeline orchestration. Mitigated by a "Pilih Semua" master toggle for users who don't need fine control.

**Schema impact**: Processing config moves from a TEXT[] column on `datasets` to a separate `dataset_source_config` junction table with per-source `processing_levels`.

**Konsekuensi penamaan**: karena "PROCESSED" berarti tiga operasi yang tidak sejenis, tier hasilnya juga tidak bisa satu nama — lihat [D14](#d14-tier-dinamai-per-kontrak-bukan-medallion-supersede-d3).

## D3: Lakehouse (6 Tiers) Instead of Flat Storage

> **Sebagian di-supersede oleh [D14](#d14-tier-dinamai-per-kontrak-bukan-medallion-supersede-d3).** Keputusan *berjenjang vs datar* di bawah ini tetap berlaku; hanya **nama** tier yang berubah. Baca `BRONZE` sebagai `ALIGNED`, `SILVER` sebagai `DESPECKLED`/`INDICES`/`ACCUMULATED` (per source), `GOLD` sebagai `COG`, `FUSION` sebagai `FUSED`.

**Decision**: RAW → ALIGNED → (DESPECKLED | INDICES | ACCUMULATED) → COG → PREVIEW → FUSED tier hierarchy with automatic cleanup.

**Why**:
- RAW enables re-calibration without re-downloading (~1.6 GB/scene saved)
- ALIGNED is the checkpoint before expensive per-source processing
- The rank-2 tier enables quality inspection on processed output
- COG is the analysis-ready single-sensor format
- FUSED is the ML-ready multi-modal format (HDF5)
- Users only keep tiers they need; cleanup frees disk automatically

## D4: HDF5 for Fusion (Not Multi-Band GeoTIFF)

**Decision**: Fused output is HDF5 with grouped datasets, not a single multi-band GeoTIFF.

**Why**: HDF5 supports named groups (`/sentinel1/VV`, `/modis/NDVI`), mixed dtypes (uint8 for FLOOD, float32 for backscatter), internal chunking for efficient slicing, and gzip compression. Multi-band GeoTIFF would flatten everything into anonymous bands with uniform dtype. HDF5 is natively supported by PyTorch, TensorFlow, h5py, xarray.

## D5: PostgreSQL + PostGIS + TimescaleDB (Not NoSQL)

**Decision**: Relational database with spatial and time-series extensions.

**Why**: Structured metadata (scene→product→lineage) maps naturally to relational schema. PostGIS enables spatial queries (bbox intersection). TimescaleDB optimizes time-series queries (alert events, access logs). ACID guarantees protect lineage integrity during parallel processing. TimescaleDB is optional — schema degrades gracefully to plain tables.

## D6: Single-Machine Architecture (MAX_CONCURRENT=2)

**Decision**: No distributed computing, no cloud auto-scaling. Pipeline runs on one machine with a 2-scene concurrency limit.

**Why**: This is a research tool, not a production service competing with GEE or MPC. Single-machine keeps deployment simple, avoids cloud vendor lock-in, and runs offline. The concurrency limit prevents OOM on 8–16 GB machines during memory-intensive stages (Lee filter, HDF5 fusion).

**What we do NOT claim**: Computational superiority over GEE/MPC. They have planet-scale infrastructure; we have parametric configurability on a single machine.

## D7: APScheduler (Not System Cron)

**Decision**: In-process Python scheduler, not OS-level cron.

**Why**: Easy pause/resume, Python-native event handling, no external dependency. PostgreSQL advisory lock ensures only one API worker runs the scheduler in multi-worker deployments.

## D8: Vanilla JS Frontend (No React/Vue)

**Decision**: No frontend framework, no build step.

**Why**: The dashboard is a monitoring/configuration UI, not a complex SPA. Vanilla JS + Leaflet keeps deployment trivial (static files served by FastAPI), reduces build complexity, and avoids framework churn. The 4-step wizard and dataset cards don't justify React overhead.

## D9: SHA-256 Lineage Tracking

**Decision**: Every data product gets a SHA-256 checksum. Every transformation edge in `data_lineage` records input and output checksums.

**Why**: Reproducibility guarantee. Given a fusion HDF5 file, the lineage graph traces back to the exact RAW downloads with cryptographic proof that no intermediate was tampered with. No competitor (GEE, openEO, MPC) provides this level of provenance tracking.

## D10: Non-Fatal Auxiliary Sources

**Decision**: MODIS and GPM failures don't fail the pipeline. Missing layers are filled with NaN in fusion output.

**Why**: Optical MODIS is cloud-contaminated during monsoon season. GPM may have latency gaps. Blocking the entire pipeline on auxiliary data would drastically reduce output. NaN fill preserves array shape so downstream consumers (PyTorch DataLoader, etc.) don't crash. Fusion metadata records temporal offsets so consumers know which layers are same-day vs. offset.

## D11: Terminology — "Automated Data Ingestion" (Never "Scraping")

**Decision**: All documentation and code comments use "automated data ingestion" or "ETL harvester."

**Why**: CDSE and NASA Earthdata are authorized APIs with explicit authentication. "Scraping" implies unauthorized access and is factually incorrect. This matters for academic integrity and institutional cooperation.

## D12: No Authentication on API (Deferred)

**Decision**: API ships without auth. Add before any public exposure.

**Why**: Development velocity. Auth is orthogonal to the core research contribution and can be bolted on (API key middleware or OAuth) without architectural changes. Documented in deployment checklist as a required step.

## D13: Clone Last Config via Backend (Hybrid Strategy)

**Decision**: "Pakai Config Sebelumnya" button fetches last dataset config from DB via `GET /api/datasets/last-config`. Falls back gracefully if unavailable.

**Why**: 
- **DB-persisted, not session-dependent** — user's config survives browser close, multiple devices
- **API-first** — aligns with architecture (REST is single source of truth)
- **Graceful fallback** — if fetch fails, button still visible but shows error, doesn't break UX
- **Minimal cost** — single SELECT query on datasets table, ordered by created_at DESC LIMIT 1

**Alternative (not chosen)**: Pure LocalStorage would be faster (no network call) but loses data if user clears cache or switches device. Hybrid (try backend, fallback to localStorage) adds complexity for marginal benefit.

**Constraint**: Endpoint returns config fields (region_id, sources, fusion_strategy, preview_options, date_start, date_end), **not** datasets table keys like dataset_id or name. Date range IS included and pre-filled by the frontend (unlike `name`, which the user must always re-enter) -- it's still just a preset the user can edit before submitting, not a lock, so it doesn't reintroduce accidental duplication risk.

**UI Behavior**:
- Button only shows if at least 1 dataset exists
- Click triggers API call; loading state shown
- On success: form fields auto-populate, focus first editable field
- On failure: inline error message, user can still proceed manually
- User can edit any field after populate (clone is preset, not locked)

## D14: Tier Dinamai per Kontrak, Bukan Medallion (Supersede D3)

**Decision**: Buang kosakata medallion (BRONZE/SILVER/GOLD). Tier dinamai menurut **jaminan yang dipenuhi artefak**, dan khusus tahap "nilai tambah PROCESSED" nama dipecah per-source karena ketiga satelit memang mengerjakan hal yang tidak sejenis.

| Rank | Nama lama | Nama baru | Berlaku untuk | Kontrak |
|---|---|---|---|---|
| 0 | `RAW` | `RAW` | semua | format native vendor (SAFE / HDF4 / NetCDF4), apa adanya |
| 1 | `BRONZE` | `ALIGNED` | semua | EPSG:4326 + crop AOI + satuan fisis |
| 2 | `SILVER` | `DESPECKLED` | sentinel1 | Lee filter 7×7 sudah diterapkan |
| 2 | `SILVER` | `INDICES` | modis | NDVI + NDWI sudah dihitung |
| 2 | `SILVER` | `ACCUMULATED` | gpm | jendela 24h/72h/7d sudah dibangun |
| 3 | `GOLD` | `COG` | semua | Cloud-Optimized GeoTIFF, HTTP range-readable |
| 4 | `FUSION` | `FUSED` | lintas-source | HDF5 multi-modal |
| — | `PREVIEW` | `PREVIEW` | semua | PNG turunan, di luar rantai lineage |

**Why**:

1. **BRONZE/SILVER/GOLD tidak lagi berarti apa yang dijanjikan medallion.** GOLD bukan mutu lebih tinggi dari SILVER — pikselnya identik, hanya dibungkus ulang jadi COG. Itu keputusan *format*, bukan *kualitas*. Medallion juga hanya punya tiga level sementara pipeline ini punya enam; `FUSION` dan `PREVIEW` sudah tidak muat sejak awal.

2. **SILVER adalah satu nama untuk tiga operasi yang berbeda jenis** — Lee filter memperbaiki variabel yang sama, NDVI/NDWI menciptakan variabel baru, akumulasi membuat agregat temporal baru. Ini konsekuensi langsung dari D2 (processing level per-satelit): begitu "PROCESSED" didefinisikan per-sensor, tier hasilnya juga tidak bisa satu nama.

3. **ALIGNED dan COG justru seragam, jadi tidak dipecah.** Ketiga source menjamin hal yang identik di rank 1 dan rank 3. Memberi tiga nama berbeda di situ akan mengarang perbedaan yang tidak ada — kesalahan cermin dari nomor 2 — dan memaksa `storage_breakdown` serta filter `?tier=` memakai tiga kosakata untuk satu konsep.

4. **User tidak pernah memilih tier.** Yang dipilih di UI adalah mode per-satelit (RAW/PROCESSED) dan `fusion_strategy`. Tier adalah konsekuensi, bukan input. Nama yang deskriptif membuat isi folder hasil unduhan bisa dibaca tanpa perlu membuka dokumentasi.

5. **Lineage jadi self-documenting.** Baris `data_lineage` yang berbunyi `aligned → indices` langsung terbaca tanpa harus melihat `transformation_type`.

**Mekanisme — nama dipisah dari peringkat**: `TIER_ORDER` sebagai list tidak lagi memadai karena rank 2 punya tiga nilai. Ganti dengan `rank(tier) -> int` yang mengembalikan 0–4 sesuai tabel di atas. Seluruh aritmetika yang ada tetap hidup tanpa perubahan logika:

- `dataset_manager.compute_max_tier` — `max(..., key=rank)`
- `dataset_manager.compute_tiers_to_delete` — perbandingan indeks jadi perbandingan `rank`
- pemetaan tahap-pipeline → indeks tier tertinggi ([dataset_manager.py:30](../etl/dataset_manager.py#L30))
- aturan fusion "baca tier tertinggi yang tersedia per source" — jadi `max(rank)` per source, hasilnya `COG` untuk PROCESSED dan `ALIGNED` untuk RAW

**Call-site terdampak**:

| Berkas | Yang berubah |
|---|---|
| [etl/dataset_manager.py:28](../etl/dataset_manager.py#L28) | `TIER_ORDER` list → fungsi `rank()`; validasi tier ikut source |
| [etl/folder_manager.py:73](../etl/folder_manager.py#L73) | `TIERS` + `TIER_SOURCES` — rank 2 dipetakan per-source, bukan `SOURCES` penuh |
| [etl/database_client.py:321](../etl/database_client.py#L321) | komentar urutan tier yang harus sinkron dengan `rank()` |
| [etl/module4_gold_export.py](../etl/module4_gold_export.py) | rename → `module4_cog_export.py`; `GOLD_PRODUCT_TYPES` → `COG_PRODUCT_TYPES` |
| [etl/module5_orchestrator.py:650](../etl/module5_orchestrator.py#L650) | pemanggil `compute_tiers_to_delete` |
| `api/schemas.py` | `ProductTierEnum` — nilai baru |
| [api/routes/products.py:72](../api/routes/products.py#L72) | deskripsi query `tier=`; nilai rank 2 butuh `IN (...)` tiga nilai |
| [api/routes/preview.py:157](../api/routes/preview.py#L157), [quality.py:217](../api/routes/quality.py#L217), [scenes.py:79](../api/routes/scenes.py#L79) | konstanta `ProductTierEnum.SILVER` / `.GOLD` |
| DB | migration: rename nilai pada kolom `product_tier`; rank 2 dipetakan menurut `source` baris tersebut |

**Trade-off**: query "ambil semua produk tahap-menengah lintas source" tidak lagi satu perbandingan kesetaraan, melainkan `IN ('DESPECKLED','INDICES','ACCUMULATED')`. Ini harga yang disengaja: satu nama seragam di posisi itu hanya benar kalau operasinya seragam, dan operasinya tidak seragam.

**Alternatif yang tidak dipilih**:

- **Nama per-satelit untuk SEMUA rank** (mis. S1 `geocoded` / MODIS `floodmap` / GPM `daily` di rank 1). Ditolak karena memecah dua tier yang kontraknya benar-benar identik — lihat alasan 3. Juga memaksa peneliti menghafal tiga kamus untuk menyebut tahap yang setara saat membandingkan source dalam satu ablation study.
- **CEOS Processing Level (L1/L2/L3/L4)**. Standar komunitas EO dan pemetaannya hampir pas, tapi `L2` tetap satu nama untuk tiga operasi berbeda — tidak menyelesaikan keluhan utama — dan angka lebih buram dibaca di path folder ketimbang kata.
- **Dua sumbu terpisah** (`processing_level` × `artifact_form`). Paling benar secara arsitektur, tapi mengubah kedalaman folder dan seluruh skema DB untuk keuntungan yang sudah dicakup penamaan kontrak.
- **Hapus tier dari API/UI, sisakan internal.** Sebagian sudah terjadi (user memang tidak memilih tier), tapi tier tetap harus muncul di `storage_breakdown` dan file browser, jadi namanya tetap perlu jujur.

**Catatan koreksi dokumentasi** (ketidaksinkronan yang ditemukan saat keputusan ini disusun, harus dibereskan bersama migrasi):

1. ~~[DOCS/ETL.md:110](ETL.md#L110) menyatakan layout `{source}/{processing_level}/{tier}/` sementara kode membangun `{YYYYMMDD}/{tier}/{source}/`.~~ **Sudah dibereskan** oleh relayout D15: layout sekarang `{source}/{RAW|PROCESSED}/` dan docs-nya ditulis ulang. Ironisnya bentuk yang diklaim docs lama justru lebih dekat ke bentuk akhir daripada yang dibangun kode saat itu.
2. [DOCS/DESIGN.md:192](DESIGN.md#L192) menyebut "RAW tier quirk: tidak ada folder tier RAW". Sudah tidak berlaku — `folder_manager.TIERS` memuat `raw` sebagai tier tersendiri. Dengan D14 catatan quirk ini dihapus seluruhnya: `raw/` (native) dan `aligned/` (georeferenced) adalah dua kontrak berbeda yang namanya masing-masing sudah jelas, sehingga tidak ada lagi yang perlu dijelaskan sebagai pengecualian.

## D15: Amandemen D14 — Tier Bukan Lagi Segmen Path

**Decision**: Layout on-disk berubah jadi sumber-di-depan dengan dua laci per sumber (`{source}/{RAW|PROCESSED}/`). Nama tier kontrak dari D14 (`ALIGNED`, `DESPECKLED`, `INDICES`, `ACCUMULATED`, `COG`) **tidak jadi dipakai sebagai nama folder**; tier tetap hidup sebagai nilai `data_products.product_tier`, kosakata `data_lineage`, dan kunci `storage_breakdown`.

**Why**: D14 menjawab "nama tier mana yang jujur". Relayout menjawab pertanyaan yang lebih dulu perlu dijawab — **apakah tier pantas jadi segmen path sama sekali**. Ternyata tidak: yang paling sering diminta user adalah "ambil Sentinel-1 PROCESSED saja", dan di layout lama itu tersebar di satu folder per tanggal per tier. Begitu sumber dan level jadi dua segmen path, tier tidak menyisakan pekerjaan di jalur.

Pemetaannya jatuh tepat pada definisi D2:

| Tier | Laci | Isi |
|---|---|---|
| `BRONZE` | `{source}/RAW/` | terkalibrasi + ter-crop, tanpa Lee — definisi "S1 RAW" di D2 |
| `GOLD` | `{source}/PROCESSED/` | COG analysis-ready |
| `RAW`, `SILVER` | `_work/` | artefak antara, disapu di akhir job |

**Trade-off yang disengaja**: mengulang Lee filter dengan parameter berbeda berarti mengunduh ulang scene-nya (~1,6 GB), karena ZIP SAFE tidak lagi disimpan. Harganya dibayar supaya `{source}/RAW/` dan `{source}/PROCESSED/` berarti persis seperti yang dijanjikan D2, tanpa artefak lain berebut folder yang sama. Sidecar `metadata_qa.json` ikut hilang; metrik kualitasnya sendiri tetap di tabel `quality_metrics`.

**Konsekuensi lain**:
- Tanggal wajib ada di nama berkas. Ketiga sumber sudah melakukannya; hanya PNG preview yang perlu ditambahi prefiks — tanpa itu render tanggal kedua menimpa tanggal pertama.
- `list_date_dirs` berubah fungsi jadi detektor layout lama (`is_legacy_layout`).
- Dua scene Sentinel-1 pada hari yang sama berbagi satu "kunci scene" di listing, karena tidak ada lagi folder scene. Berkasnya tetap terpisah lewat `product_identifier` di nama.
- `_work/` disapu di akhir SETIAP job, bukan hanya saat `fusion_output_only` — isinya scratch menurut definisinya sendiri.

**Tidak ada migrasi**: dataset pra-relayout dibiarkan apa adanya, dideteksi `is_legacy_layout()`, dan panel Struktur menampilkan pesan "format lama" alih-alih merender pohon dengan kosakata yang sudah tidak berlaku. Berkasnya tetap bisa diunduh utuh.

## D16: Bbox JAWA Dipecah Jadi 4 Sub-Dataset, Bukan Dipersempit

**Decision**: Dataset 26 (JAWA) diganti oleh empat dataset bersebelahan 26a–26d dengan bbox strip membujur. Bbox tunggal yang lebih kecil **tidak dibuat**, karena tidak menghemat apa pun.

**Why**: Bbox JAWA (105,2095467 −8,780418 → 114,6053892 −5,8755039) memuat 333.976 km², 60,4% di antaranya laut. Tetapi extent poligon darat OSM di dalam bbox itu persis sama dengan bbox-nya — Jawa menyentuh keempat sisinya, karena pulaunya memanjang diagonal: ujung barat di utara, ujung timur di selatan. Lautnya bukan margin di tepi, melainkan ruang di dalam rectangle yang tidak diisi pulau. Memangkas tepi mana pun akan memotong daratan.

Penghematan hanya datang dari memecah jadi beberapa kotak (darat + buffer 5 km = 45,96%, batas bawah teoretis):

| Strip | Area | Hemat |
|---|---|---|
| 1 (asal) | 333.976 km² | 0% |
| 2 | 283.077 km² | 15,2% |
| **4** | **238.400 km²** | **28,6%** |
| 8 | 210.514 km² | 37,0% |
| 16 | 196.388 km² | 41,2% |
| 24 | 193.912 km² | 41,9% |

**Empat dipilih, bukan delapan**: 8 strip menghemat 8,4 poin lebih banyak tetapi menggandakan jumlah stack yang diserahkan ke ML engineer. Setelah 8, tiap kotak tambahan hanya menambah overhead orkestrasi.

**Bbox** (sudah dikunci ke kisi terpaku `datasets.fusion_grid` dataset 26, piksel 9,100297437167133e−05°, supaya piksel sub-dataset sejajar induknya):

| Sub | bbox_wkt | Area | Darat | Piksel |
|---|---|---|---|---|
| 26a | `POLYGON ((105.2095467 -7.6066535, 107.5585155 -7.6066535, 107.5585155 -5.8755039, 105.2095467 -5.8755039, 105.2095467 -7.6066535))` | 49.722 km² | 55,5% | 25812×19023 |
| 26b | `POLYGON ((107.5585155 -7.9009571, 109.9074842 -7.9009571, 109.9074842 -5.8788710, 107.5585155 -5.8788710, 107.5585155 -7.9009571))` | 58.060 km² | 56,4% | 25812×22220 |
| 26c | `POLYGON ((109.9074842 -8.4517981, 112.2564530 -8.4517981, 112.2564530 -5.8755039, 109.9074842 -5.8755039, 109.9074842 -8.4517981))` | 73.928 km² | 56,6% | 25812×28310 |
| 26d | `POLYGON ((112.2564530 -8.7804098, 114.6054218 -8.7804098, 114.6054218 -6.8020962, 112.2564530 -6.8020962, 112.2564530 -8.7804098))` | 56.691 km² | 52,9% | 25812×21739 |

Laut turun dari 60,4% ke 44,6%. Batas lat tiap strip = extent darat+5 km di dalam strip itu, dibulatkan ke kisi.

**Satu bbox per dataset adalah batas skema**: `datasets.bbox_wkt` kolom tunggal NOT NULL ([etl/database_client.py:908](../etl/database_client.py#L908)). Karena itu pemecahan berarti empat dataset, bukan empat baris bbox di satu dataset. ML engineer menerima empat stack bersebelahan yang piksel-sejajar, bukan satu.

**Yang TIDAK dijanjikan angka ini**: penghematan **area olahan**, bukan GB unduhan. Setelah diukur (D17), unduhannya justru NAIK.

## D17: Footprint Scene S1 Tidak Pernah Tercatat

**Temuan, belum diperbaiki.** Untuk jendela 1–6 Desember 2025, 18 dari 19 baris `satellite_scenes` menyimpan bbox dataset sebagai `bbox`-nya, bukan footprint SAFE yang sebenarnya; hanya satu baris yang punya footprint nyata (387 km²). Akibatnya setiap pertanyaan "scene mana yang gugur kalau bbox dipersempit" tidak bisa dijawab dari database, dan penghematan biaya unduh D16 tidak terhitung. Perbaikannya: isi `satellite_scenes.bbox` dari footprint di metadata produk saat discovery, lalu hitung ulang.


## D18: Pemecahan Menghemat Olahan tapi Menaikkan Unduhan

**Terukur 2026-09-21**, setelah 26a–26d (dataset 27–30) dibuat dan discovery-nya jalan:

| | Scene S1 di-fetch | Scene unik |
|---|---|---|
| Dataset 26 (satu bbox) | 14 | 14 |
| 26a+26b+26c+26d | 24 | 15 |

Sembilan scene diunduh **dua kali** karena footprint-nya memotong batas strip. Jadi D16 menukar **−28,6% area olahan** dengan **+71% operasi unduh**. Ini menjawab pertanyaan yang D17 sebut belum terjawab: footprint tidak perlu tersimpan di `satellite_scenes` untuk mengukurnya — cukup hitung scene yang ditemukan tiap strip.

Konsekuensinya arah penghematan jadi berlawanan tergantung sumber daya mana yang langka. Kalau kuota/waktu unduh yang mahal, memecah merugikan. Kalau CPU fusi dan disk stack yang mahal, memecah menguntungkan. Batas strip yang digeser supaya jatuh di celah antar-jalur orbit S1 akan mengurangi duplikasi, tetapi belum dicoba.

**Dua kendala operasional yang muncul saat run pertama:**
- Menjalankan empat job serentak memicu **HTTP 429** dari Copernicus; job 23 (JAWA_B) FAILED. Strip harus dijalankan berurutan, bukan paralel.
- Sisa disk 50 GB dari 953 GB. 24 unduhan (~41 GB) plus empat stack fusi (~22 GB) tidak muat. Perlu pembebasan ruang lebih dulu.

## D19: Penggabungan Lintas Dataset, dan Kenapa Strip Jawa Belum Bisa Digabung

**Modul**: [etl/dataset_merge.py](../etl/dataset_merge.py), endpoint `/api/merge/candidates` dan `/api/merge/run`, panel "Gabungkan Dataset" di atas daftar dataset.

Pemecahan AOI (D16) menyisakan N stack terpisah per tanggal. Modul ini menyatukannya kembali dengan **menempel, bukan meresample**: resample akan menginterpolasi backscatter SAR, besaran fisis dalam dB yang rata-ratanya tidak bermakna di batas darat/air. Karena itu grid yang tidak sejajar **ditolak**, bukan dipaksakan — `GridMismatch` ada supaya kegagalannya terang-terangan, bukan diam-diam mengubah angka.

Alurnya dua langkah, LIHAT lalu IZINKAN. `GET /candidates` tidak pernah menulis; `POST /run` tidak pernah menebak kandidat dan menolak kalau `dataset_ids` tidak disebut. Yang tahu apakah beberapa strip itu memang satu pulau yang sama adalah peneliti.

**Temuan yang membatalkan satu klaim D16.** D16 menjanjikan piksel sub-dataset sejajar dengan induknya karena batas strip dikunci ke `datasets.fusion_grid` dataset 26. Dijalankan pada stack yang sungguh ada, penggabungannya menolak — dan benar menolak:

| Dataset | Resolusi grid fusion |
|---|---|
| 26 (dipaku, sudah dihapus) | 9,100297437167133e−05 |
| 27 JAWA_A, 28 JAWA_B | 9,10306849989e−05 |
| 29 JAWA_C, 30 JAWA_D | 9,066768187e−05 |

Mengunci **bbox** ke grid dataset 26 tidak mengunci **grid fusion**-nya. Tiap dataset memaku grid-nya sendiri dari resolusi raster S1 miliknya sendiri, persis seperti yang sudah diperingatkan: grid fusion bukan konstanta, ia ikut scene. Akibatnya jarak bujur yang di grid 26 tepat 25812 piksel menjadi 25804,14 piksel di grid baru — meleset 0,14 piksel, jadi tidak bisa ditempel.

**Perbaikannya tidak butuh kode baru.** `_pin_dataset_grid` hanya menulis saat `fusion_grid IS NULL` ([module9_fusion.py:1441](../etl/module9_fusion.py#L1441)), jadi mengisi kolom itu lebih dulu dengan satu grid bersama untuk keempat strip — resolusi sama, origin berselisih kelipatan bulat piksel — akan membuat fusion memakainya apa adanya. Yang harus dibayar: stack yang sudah terlanjur dirakit di grid lama perlu dirakit ulang.

**Pelajaran untuk pemecahan berikutnya (Sumatra, dan seterusnya)**: grid bersama dipaku ke SEMUA sub-dataset sebelum fusion pertama jalan, bukan disimpulkan dari bbox. Bbox menentukan di mana, grid menentukan di kisi mana — dan hanya yang kedua yang menentukan bisa-tidaknya digabung.


## D20: Layer Referensi Membangun Ulang Diri Saat Grid Berubah

**Penjaga idempoten yang hanya memeriksa keberadaan berkas adalah bug.** `_ensure_land` dan `_ensure_occurrence` dulu mengembalikan `"exists"` begitu `.tif`-nya ada. Ketika keempat strip Jawa dipakukan ke satu grid bersama (D19) setelah masks-nya terlanjur dibuat, keenam berkas itu tertinggal di grid lama dan tidak pernah dibangun ulang:

| Dataset | Mask lama | Grid terpaku |
|---|---|---|
| 27 JAWA_A | 25805 x 19018 | 25907 x 19093 |
| 28 JAWA_B | 25805 x 22214 | 25908 x 22302 |
| 29 JAWA_C | 25908 x 28415 | 25907 x 28415 |

Yang membuatnya berbahaya adalah diamnya. Berkasnya tetap terbuka normal, tidak ada error di mana pun, tapi `mask[r,c]` tidak lagi menunjuk piksel yang sama dengan `stack[r,c]`. Konsumen yang mengindeks keduanya berdampingan membaca lokasi yang salah tanpa pernah diberi tahu — dan pada JAWA_C selisihnya cuma satu kolom, yang tidak akan pernah terlihat dari inspeksi mata.

**Perbaikannya**: penjaga sekarang membandingkan grid berkas yang ada dengan `fusion_grid` terpaku (`_grid_matches`, toleransi setengah piksel supaya galat pembulatan lewat JSON tidak memicu bangun ulang percuma) dan membangun ulang saat berbeda. Pemulihannya **terjadi sendiri** — tidak menunggu seseorang ingat memanggil `force=True`, karena yang menyebabkan kerusakan ini justru tidak adanya seorang pun yang tahu harus melakukannya.

**Padanannya untuk stack sudah ada**: `audit_dataset_grids` ([module9_fusion.py:1339](../etl/module9_fusion.py#L1339)) memeriksa setiap stack terhadap grid terpaku tiap kali fusion menulis, dan memperingati tanpa menggagalkan job. Yang hilang memang hanya sisi mask-nya. Setelah D20, kedua turunan grid punya penjaganya masing-masing.

**Sisa pekerjaan yang TIDAK diperbaiki sendiri**: tiga stack yang lahir sebelum pemakuan tetap di grid lama dan hanya bisa dipulihkan dengan fusi ulang — `27/fusion_20251204`, `27/fusion_20251206`, `28/fusion_20251204`. Audit melaporkannya; memperbaikinya keputusan operator, karena merakit ulang stack berjam-jam tidak boleh dipicu diam-diam oleh penjaga.


## D21: Merakit Ulang Satu Tanggal Tanpa Mengunduh Ulang

**Modul**: [etl/refusion.py](../etl/refusion.py).

Stack fusion bisa jadi usang tanpa scene-nya ikut usang — tiga stack strip Jawa tertinggal di grid lama setelah pemakuan grid bersama (D19). Ternyata tidak ada jalur untuk memperbaikinya:

- `run_dataset_job` **melewati scene yang `scene_is_done`** (CLEANUP/COMPLETED). Job 22 yang diulang selesai dalam 0,04 detik tanpa menyentuh fusion sama sekali.
- Mereset status scene memang memaksa pipeline mengulang, tapi mengulang **dari DOWNLOAD**. Sejak D15 ZIP SAFE tidak disimpan, jadi harganya ~1,7 GB per scene — 17 GB untuk pekerjaan yang tidak butuh satu byte pun dari jaringan, karena raster yang dibutuhkan fusi sudah ada di `sentinel-1/PROCESSED/`.

**Memakai ulang `_finalize_date`, bukan memanggil `create_fusion_stack`.** Dua dari tiga tanggal yang diperbaiki tertutup lebih dari satu frame S1 (JAWA_B 4 Desember: tiga frame). Memanggil `create_fusion_stack` dengan satu `scene_id` akan menghasilkan stack yang cuma memuat satu frame — persis penyakit yang `s1_mosaic` dibuat untuk menyembuhkan. Urutan mosaik → preview → fusi → layer referensi hidup di `_finalize_date`; menirunya berarti membuat salinan kedua yang menyimpang diam-diam begitu salah satunya diubah. Jadi modul ini menyusun `_JobContext` yang sama dan memanggil fungsi yang sama.

**Hasilnya**, tanpa satu pun unduhan:

| Dataset | Tanggal | Frame | Waktu |
|---|---|---|---|
| 27 JAWA_A | 20251206 | 1 | 95 detik |
| 27 JAWA_A | 20251204 | 2 (mosaik) | ~100 detik |
| 28 JAWA_B | 20251204 | 3 (mosaik) | ~200 detik |

Audit grid keempat strip sekarang bersih (11 stack, 0 tidak cocok), dan kandidat penggabungan naik dari 2 jadi **4 tanggal, semuanya bisa digabung**.

**Batasan yang disengaja**: hanya untuk tanggal yang raster PROCESSED-nya masih ada. Tanggal yang rasternya sudah tersapu memang harus lewat pipeline penuh, dan modul ini menolaknya alih-alih menghasilkan stack separuh.
