# LIVE MONITORING — Konsep Baru Dataset Live Trinity DataLab

Dokumen ini menjelaskan perombakan fitur **Dataset Live** menjadi **Live Monitoring**: pemantauan otomatis per daerah dengan preview, interpretasi kondisi, riwayat terbatas, grafik tren, dan forecasting ringan.

Dokumen ini menjelaskan **apa** yang harus dicapai dan **aturan bisnisnya**. Cara implementasi (struktur kode, nama file, library, skema tabel persis) diputuskan berdasarkan kode yang sudah ada di repo.

---

## 1. Latar Belakang

### 1.1 Kondisi saat ini
Dataset Live saat ini pada dasarnya hanya:
- mengambil data terbaru secara otomatis, dan
- menyimpannya sebagai backfill.

Tidak ada tampilan yang membantu orang memahami kondisi suatu daerah, dan data terus menumpuk tanpa batas.

### 1.2 Masukan dosen
Dua hal yang dianggap belum ada dan wajib ada:
1. **Otomatisasi yang mempermudah pekerjaan** — sistem bekerja sendiri tanpa pengguna mengunduh, memproses, dan menganalisis manual.
2. **Ramah untuk penggunaan publik** — orang awam bisa membuka satu halaman dan langsung paham kondisi daerahnya, tanpa perlu mengerti dB, NDVI, atau format HDF5.

### 1.3 Jawaban dari konsep baru
| Kebutuhan | Dijawab oleh |
|---|---|
| Otomatisasi | Unduh + proses scene baru otomatis, preview & interpretasi otomatis, hapus data lama otomatis, forecast otomatis |
| Ramah publik | Dropdown daerah → satu kartu berisi gambar, kalimat kondisi dalam bahasa sederhana, tanggal, dan grafik |

---

## 2. Istilah

| Istilah | Arti |
|---|---|
| **Daerah Live** | Area pemantauan yang ditambahkan pengguna (mis. Padang). Punya nama, batas wilayah, dan jumlah scene yang disimpan. |
| **Scene** | Satu titik waktu pemantauan. **Jangkar scene adalah tanggal akuisisi Sentinel-1** di daerah tersebut; MODIS dan GPM dicocokkan ke tanggal itu. |
| **Retensi** | Jumlah scene yang disimpan per daerah, **minimal 1, maksimal 12**. |
| **Preview** | Gambar PNG hasil render untuk ditampilkan di kartu. |
| **Interpretasi** | Kalimat kondisi otomatis untuk tiap preview. |

> Catatan penting: Sentinel-1 tidak lewat setiap hari (revisit ~6–12 hari tergantung wilayah & orbit). Maka "12 scene" berarti 12 akuisisi terakhir, bukan 12 hari kalender. UI harus menampilkan tanggal sebenarnya.

Untuk pencocokan MODIS & GPM ke tanggal scene, gunakan logika temporal matching yang sudah ada di pipeline fusion jika tersedia. Bila MODIS tidak ada untuk tanggal itu (awan / tidak ada data), ambil data valid terdekat dalam jendela yang wajar dan tandai sebagai "terdekat" di UI, atau tampilkan "tidak tersedia" — jangan kosong tanpa penjelasan.

---

## 3. Alur Pengguna

### 3.1 Menambah Daerah Live
- Form mirip dengan halaman **Tambah Dataset** (pilih/gambar area, beri nama). Gunakan ulang komponen/logic yang sudah ada sebisa mungkin.
- Field tambahan: **jumlah scene disimpan** (1–12).
- Setelah disimpan, sistem langsung melakukan pengisian awal (backfill) sampai jumlah scene terpenuhi, lalu masuk ke siklus otomatis.

### 3.2 Halaman Live
- Bagian atas: **dropdown** berisi daerah-daerah live milik pengguna + tombol tambah daerah.
- Memilih daerah → **kartu daerah** terbuka.
- Pengguna juga bisa mengubah retensi atau menghapus daerah live.

### 3.3 Siklus otomatis (per daerah)
1. Cek apakah ada scene Sentinel-1 baru.
2. Jika ada: unduh S1, MODIS, GPM yang cocok → proses → buat 8 preview → hitung metrik → buat interpretasi.
3. Hitung ulang forecast.
4. Jika jumlah scene > retensi → hapus scene paling lama (lihat bagian 7).
5. Catat semua langkah ke log.

Jadwal pengecekan memakai mekanisme scheduler/background job yang sudah ada di project. Kegagalan satu sumber (mis. GPM 401/500) tidak boleh menggagalkan scene — scene tetap tersimpan dengan status sumber yang gagal ditandai, dan bisa dicoba ulang.

---

## 4. Isi Kartu Daerah

Urutan dari atas ke bawah:

```
┌──────────────────────────────────────────────┐
│  PADANG · scene terbaru: 2026-09-24          │
│  Status singkat daerah (1 kalimat)           │
├──────────────────────────────────────────────┤
│  Baris 1  [S1 VV]        [S1 VH]             │
│  Baris 2  [MODIS 1] [MODIS 2] [MODIS 3]      │
│  Baris 3  [GPM 1]   [GPM 2]   [GPM 3]        │
│           (tiap preview + kalimat kondisi)   │
├──────────────────────────────────────────────┤
│  Tanggal tersimpan: [24/09] [12/09] [31/08]… │
├──────────────────────────────────────────────┤
│  Grafik S1     (tren + forecast)             │
│  Grafik MODIS  (tren + forecast)             │
│  Grafik GPM    (tren + forecast)             │
└──────────────────────────────────────────────┘
```

### 4.1 Preview (8 gambar)

**Baris 1 — Sentinel-1 (grayscale, tanpa overlay)**
| # | Preview | Metrik utama | Fungsi |
|---|---|---|---|
| 1 | VV | rata-rata backscatter VV (dB) | kondisi permukaan umum, kelembapan |
| 2 | VH | rata-rata backscatter VH (dB) + % piksel di bawah ambang air | **indikator utama genangan** |

**Baris 2 — MODIS (warna, di-overlay di atas Sentinel-1 VH, opasitas warna 40%)**
| # | Preview | Metrik utama | Fungsi |
|---|---|---|---|
| 3 | LST | suhu permukaan rata-rata (°C) + selisih dari rata-rata scene tersimpan | area dingin tak wajar → kemungkinan air |
| 4 | NDVI | NDVI rata-rata | tekanan vegetasi akibat genangan/kekeringan |
| 5 | Indeks air (NDWI/MNDWI, atau produk surface reflectance yang tersedia) | % area terindikasi air | konfirmasi optik untuk genangan |

> Jika produk MODIS yang sudah di-pipeline berbeda, pakai yang tersedia dan pilih tiga yang paling relevan untuk deteksi genangan. NDVI MODIS umumnya komposit 16-hari — tampilkan tanggal kompositnya.

**Baris 3 — GPM (warna, di-overlay di atas Sentinel-1 VH, opasitas warna 40%)**
| # | Preview | Metrik utama | Fungsi |
|---|---|---|---|
| 6 | Hujan 24 jam | akumulasi mm | aktivitas hujan terkini |
| 7 | Hujan 72 jam | akumulasi mm | **indikator risiko banjir utama** |
| 8 | Intensitas maksimum | mm/jam puncak | hujan ekstrem singkat |

> GPM resolusinya kasar (~10 km). Saat di-overlay ke grid Sentinel-1, hasilnya akan terlihat kotak-kotak besar — itu wajar; boleh dihaluskan untuk tampilan tapi metrik dihitung dari nilai asli.

**Overlay:** sama dengan konsep colored preview yang sudah ada di project — Sentinel-1 sebagai dasar grayscale, layer warna ditimpa dengan transparansi **40%**. Setiap preview berwarna wajib punya legenda skala warna kecil.

### 4.2 Kalimat Kondisi (interpretasi)

Format wajib, tiga bagian:

> **Menampilkan** *[apa]* **dalam kondisi** *[kategori]* **karena** *[argumen kuantitatif singkat]*.

Contoh:
- S1 VH: "Menampilkan permukaan tanah dalam kondisi **terindikasi genangan** karena 18% area memiliki VH < −20 dB, naik dari 6% pada scene sebelumnya."
- MODIS LST: "Menampilkan suhu permukaan dalam kondisi **lebih dingin dari biasanya** karena rata-rata 25,1 °C, 2,4 °C di bawah rata-rata scene tersimpan."
- GPM 72 jam: "Menampilkan curah hujan dalam kondisi **tinggi** karena akumulasi 3 hari 142 mm, di atas ambang 100 mm."

Aturan:
- Dihasilkan otomatis dari metrik + ambang (rule-based, bukan LLM).
- Kategori memakai kata sederhana (normal / waspada / tinggi / terindikasi genangan / tidak tersedia).
- Argumen selalu menyebut **angka** dan, bila ada, **pembanding** (scene sebelumnya atau ambang).
- Nilai ambang disimpan di satu tempat konfigurasi agar mudah diubah; beri nilai awal yang masuk akal dan dokumentasikan sumbernya.
- Jika data tidak tersedia: "Menampilkan … dalam kondisi **tidak tersedia** karena tidak ada citra valid (tutupan awan 92%)."

**Status singkat daerah** di header kartu: gabungan sederhana dari S1 VH, MODIS indeks air, dan GPM 72 jam (mis. "Waspada — hujan tinggi dan area genangan meningkat").

### 4.3 Daftar Tanggal Tersimpan
- Menampilkan semua scene yang tersimpan, terbaru di kiri.
- Jumlahnya = retensi (atau kurang jika masih mengisi).
- Klik tanggal → 8 preview + kalimat kondisi berganti ke scene tersebut. Grafik tetap menampilkan seluruh riwayat, dengan penanda di scene yang dipilih.
- Tanggal yang datanya sudah dihapus **tidak** muncul di sini (hanya ada di log).

### 4.4 Grafik Tren + Forecast
Tiga grafik, satu per satelit, sumbu-X = tanggal scene:
| Grafik | Sumbu-Y | Tipe |
|---|---|---|
| Sentinel-1 | rata-rata VH (dB) | garis |
| MODIS | LST (°C) atau indeks air — pilih satu yang paling informatif | garis |
| GPM | akumulasi 72 jam (mm) | batang, dengan garis ambang |

Forecast ditampilkan sebagai lanjutan garis putus-putus + pita ketidakpastian, jelas dibedakan dari data aktual dan diberi label "perkiraan".

---

## 5. Forecasting

### 5.1 Jumlah scene yang diramal
| Scene tersimpan | Scene diramal |
|---|---|
| 1–3 | 1 |
| 4–6 | 2 |
| 7–9 | 3 |
| 10–12 | 4 |

Rumus: `jumlah_forecast = ceil(n_scene / 3)`.

### 5.2 Metode
Harus **ringan**, cocok untuk deret sangat pendek (1–12 titik), tanpa training.
- **Utama:** Exponential smoothing — Holt (double, dengan tren) bila ≥ 4 titik; simple exponential smoothing bila 2–3 titik.
- **1 titik:** forecast = nilai terakhir (persistence), pita ketidakpastian lebar, beri label "data belum cukup".
- Pita ketidakpastian dari sebaran residual; untuk titik sedikit gunakan pita yang lebih lebar.
- Nilai yang tidak mungkin dipotong (hujan tidak boleh negatif).
- Tanggal forecast mengikuti interval rata-rata antar scene di daerah tersebut.

Forecast dihitung ulang setiap scene baru masuk, dan disimpan agar halaman tidak menghitung ulang setiap dibuka.

---

## 6. Retensi & Penghapusan

- Saat scene baru masuk dan jumlah scene melebihi retensi, scene paling lama **dihapus permanen (hard delete)** — raster mentah, hasil proses, dan preview PNG milik scene itu.
- Jika pengguna **menurunkan** retensi (mis. 12 → 6), kelebihan scene langsung dihapus dengan aturan sama.
- Menghapus daerah live menghapus semua file scene-nya; log tetap disimpan.
- File yang juga dipakai oleh dataset biasa (non-live) **tidak boleh** ikut terhapus. Pastikan penghapusan hanya menyentuh file milik daerah live tersebut.

### 6.1 Yang tetap disimpan (log)
Setiap scene — termasuk yang sudah dihapus — tetap punya jejak:
- daerah, tanggal scene, waktu dibuat, waktu dihapus, alasan hapus
- daftar file yang dihapus + ukurannya (total ruang yang dibebaskan)
- **metrik ringkas** tiap sumber (angka rata-rata yang dipakai di grafik) dan kalimat kondisinya
- status tiap sumber (berhasil / gagal / tidak tersedia)

Log ini ringan (teks/angka), jadi aman disimpan selamanya dan berguna untuk audit serta analisis jangka panjang.

---

## 7. Batasan

- **Jangan merusak** fitur yang sudah ada: Tambah Dataset, pipeline per-satelit, fusion, colored preview, report generation. Fitur baru bersifat tambahan, kecuali bagian Live lama yang memang diganti.
- Perubahan database bersifat **aditif** (tabel/kolom baru), dengan migrasi yang sesuai dengan tooling project.
- Data Live lama yang sudah ada: tentukan jalur migrasi yang aman (konversi ke Daerah Live atau dibiarkan), jangan dihapus diam-diam.
- Satu pengguna bisa punya beberapa daerah live; batasi jumlah wajar agar storage dan kuota API aman.
- Tampilan harus tetap terbaca di layar ponsel (grid preview boleh turun menjadi 1–2 kolom).

---

## 8. Kriteria Selesai

- [ ] Pengguna bisa menambah, mengubah retensi, dan menghapus Daerah Live.
- [ ] Daerah baru otomatis terisi hingga jumlah scene sesuai retensi.
- [ ] Scene baru masuk otomatis tanpa aksi pengguna.
- [ ] Kartu menampilkan 8 preview dengan susunan 2–3–3; MODIS & GPM ter-overlay di atas S1 dengan opasitas 40% dan legenda.
- [ ] Setiap preview punya kalimat "Menampilkan … dalam kondisi … karena …" dengan angka.
- [ ] Daftar tanggal bisa diklik untuk berganti scene.
- [ ] Tiga grafik tren dengan forecast sesuai tabel 5.1, dibedakan jelas dari data aktual.
- [ ] Scene melebihi retensi terhapus permanen; log & metrik ringkas tetap ada.
- [ ] Kegagalan satu sumber tidak menggagalkan scene.
- [ ] Fitur lain tidak rusak.
