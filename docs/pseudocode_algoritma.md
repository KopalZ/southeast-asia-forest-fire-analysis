# Pseudocode dan Algoritma Analisis Multi-Country

Dokumen ini menjelaskan cara kerja `scripts/03_analyze.py` yang menghasilkan
seluruh angka pada `outputs/analysis/`. Pseudocode ditulis mengikuti alur kode
aktual, bukan rancangan ideal yang berbeda dari implementasi.

- **Input utama:** `config/datasets.json` (10 dataset: 5 negara × archive/NRT)
- **Input pendukung:** `outputs/validation/validation_summary.json` (hanya untuk cek silang; validasi tidak diulang)
- **Output:** 6 file di `outputs/analysis/`
- **Bahasa:** Python 3, standard library saja
- **Pemrosesan:** streaming, satu kali baca per CSV

---

## 1. Tujuan algoritma

Algoritma menjawab delapan kebutuhan analisis dalam satu lintasan data:

| No | Kebutuhan | Dihasilkan di |
|---|---|---|
| 1 | Deteksi per negara dan tahun | `yearly_summary.csv` |
| 2 | Deteksi per negara dan bulan | `monthly_summary.csv` |
| 3 | Top 20 grid 0,1° | `spatial_top20.csv`, `analysis_summary.json` |
| 4 | Distribusi confidence | `characteristics.csv` |
| 5 | Distribusi day/night | `characteristics.csv` |
| 6 | Total dan rata-rata FRP | `characteristics.csv`, `analysis_summary.json` |
| 7 | Distribusi type (archive) | `characteristics.csv` |
| 8 | Ringkasan baris per negara dan feed | `analysis_summary.json`, `analysis_report.txt` |

---

## 2. Input dan struktur data

### 2.1 Manifest

Setiap entri manifest berisi:

```
dataset_id, country_code, country_name, feed, area_code,
path_local, path_hdfs, bytes, rows, n_columns, has_type,
date_min, date_max, version_raw
```

`feed` bernilai `archive` atau `nrt`. Archive memiliki 15 kolom termasuk
`type`; NRT memiliki 14 kolom tanpa `type`.

### 2.2 Kolom CSV yang dipakai

| Kolom | Kegunaan |
|---|---|
| `acq_date` | tahun (`YYYY`) dan bulan (`YYYY-MM`) |
| `latitude`, `longitude` | grid 0,1° |
| `frp` | total, rata-rata, dan hitungan FRP nol |
| `confidence` | distribusi `l` / `n` / `h` |
| `daynight` | distribusi `D` / `N` |
| `type` | hanya jika kolom ada (archive) |

Kolom lain (`brightness`, `scan`, `track`, `satellite`, `instrument`,
`version`, `bright_t31`, `acq_time`) tidak diagregasi pada tahap ini.

### 2.3 Akumulator

```
yearly[(country_code, feed, tahun)]        -> jumlah deteksi
monthly[(country_code, feed, "YYYY-MM")]   -> jumlah deteksi
grids[country_code][(lat1, lon1)]          -> [jumlah_archive, jumlah_nrt]
stats[(country_code, feed)]                -> detections, confidence{},
                                              daynight{}, type{}, frp_sum,
                                              frp_zero, frp_negative,
                                              invalid_date, invalid_coordinate,
                                              invalid_frp
feed_rows[(country_code, feed)]            -> jumlah baris terbaca
```

`lat1 = round(latitude, 1)` dan `lon1 = round(longitude, 1)`, sama dengan
metode baseline Indonesia.

---

## 3. Pseudocode utama

```
ALGORITMA AnalisisHotspotMultiNegara

INPUT:
  M = manifest berisi 10 dataset
  V = ringkasan validasi (opsional)

OUTPUT:
  yearly_summary.csv, monthly_summary.csv, spatial_top20.csv,
  characteristics.csv, analysis_summary.json, analysis_report.txt

1. BACA M
2. JIKA V tersedia: BACA V
3. country_order <- negara sesuai urutan kemunculan di M
4. names[country_code] <- country_name
5. dataset_by_key[(country_code, feed)] <- entri dataset

6. INISIALISASI semua akumulator pada Bagian 2.3

7. UNTUK SETIAP dataset D DALAM M:
       feed_rows[(D.country, D.feed)] <- ProsesFile(D)

8. BENTUK AGREGAT GABUNGAN
       yearly_combined[(cc, tahun)]  <- yearly[(cc,"archive",tahun)]
                                      + yearly[(cc,"nrt",tahun)]
       monthly_combined[(cc, bulan)] <- monthly[(cc,"archive",bulan)]
                                      + monthly[(cc,"nrt",bulan)]
       stats_combined[cc]            <- gabungan stats archive + nrt

9. TULIS yearly_summary.csv
10. TULIS monthly_summary.csv
11. TULIS spatial_top20.csv
12. TULIS characteristics.csv
13. HITUNG konsistensi:
       rows_manifest <- jumlah D.rows
       rows_validation <- jumlah V.datasets[].rows_actual
       rows_analyzed <- jumlah feed_rows
       PASS jika ketiganya sama dan tidak ada dataset yang berbeda
14. TULIS analysis_summary.json
15. TULIS analysis_report.txt
16. RETURN 0 jika konsisten, selain itu 1
```

---

## 4. Pseudocode pemrosesan satu file

```
FUNGSI ProsesFile(D) -> jumlah_baris

  path <- resolve(D.path_local)
  buka path sebagai stream CSV
  header <- baris pertama
  index <- peta nama_kolom -> posisi
  n <- jumlah kolom
  i_type <- index["type"] jika ada, selain itu NULL
  grid_negara <- grids[D.country_code]
  state <- stats[(D.country_code, D.feed)]   # dibuat jika belum ada
  rows <- 0

  UNTUK SETIAP baris R DALAM stream:
      JIKA panjang(R) != n:
          LEWATI                         # tidak dihitung
      rows <- rows + 1

      # tanggal
      tahun <- int(R[acq_date][0:4])
      bulan <- R[acq_date][0:7]
      JIKA gagal: state.invalid_date += 1; LEWATI

      # koordinat
      lat <- float(R[latitude]); lon <- float(R[longitude])
      JIKA gagal: state.invalid_coordinate += 1; LEWATI

      # FRP
      frp <- float(R[frp])
      JIKA gagal: state.invalid_frp += 1; LEWATI

      # nilai non-finite (lat, lon, atau frp) dicatat sebagai invalid_frp
      JIKA tidak finite(lat) atau tidak finite(lon) atau tidak finite(frp):
          state.invalid_frp += 1; LEWATI

      yearly[(cc, feed, tahun)] += 1
      monthly[(cc, feed, bulan)] += 1

      kunci <- (round(lat, 1), round(lon, 1))
      sel <- grid_negara[kunci] atau [0, 0]
      sel[posisi_feed] += 1              # 0 = archive, 1 = nrt

      state.detections += 1
      state.confidence[R.confidence] += 1
      state.daynight[R.daynight] += 1
      JIKA i_type ada: state.type[R.type] += 1
      state.frp_sum += frp
      JIKA frp == 0: state.frp_zero += 1
      JIKA frp < 0:  state.frp_negative += 1

  feed_rows[(cc, feed)] <- rows
  RETURN rows
```

Catatan penting:

- Baris dengan panjang kolom salah **tidak** dihitung sebagai deteksi.
- Baris dengan tanggal/koordinat/FRP tidak valid dicatat pada counter
  `invalid_*` dan tidak masuk agregat. Pada data aktual semua counter ini 0.
- Pemeriksaan nilai non-finite untuk lat/lon/FRP menaikkan counter
  `invalid_frp` (perilaku aktual script); pada data aktual tidak ada baris
  non-finite.
- FRP nol **tetap** dihitung sebagai deteksi dan masuk rata-rata.
- `type` hanya diagregasi jika kolomnya ada; NRT tidak diwajibkan punya `type`.

---

## 5. Aturan agregasi

### 5.1 Tahunan

Untuk setiap `(negara, feed, tahun)`:

```
detections = jumlah baris valid pada tahun tersebut
```

Tahun 2012 dan 2026 diberi `is_partial_year = True` karena:

- 2012 mulai 20 Januari (bukan 1 Januari);
- 2026 archive berakhir 31 Juli, NRT berakhir 1–2 November.

### 5.2 Bulanan

Untuk setiap `(negara, feed, YYYY-MM)`:

```
detections = jumlah baris valid pada bulan tersebut
```

### 5.3 Grid spasial

```
kunci_grid = (round(latitude, 1), round(longitude, 1))
```

Setiap sel menyimpan dua angka: jumlah archive dan jumlah NRT. Peringkat
top 20 dihitung dari **total keduanya**, lalu diurutkan menurun dengan
tie-break `country_code`, `latitude`, `longitude`. Negara selalu disertakan
agar sel dari negara berbeda tidak dianggap lokasi yang sama.

### 5.4 Gabungan archive + NRT

Baris `feed = "combined"` adalah penjumlahan deteksi archive dan NRT.
Rentang tanggal keduanya tidak tumpang tindih (overlap 0 hari pada hasil
validasi), sehingga penjumlahan tidak menggandakan tanggal. Ini **bukan**
jumlah kejadian kebakaran unik dan selalu diberi label `combined`.

### 5.5 Karakteristik

```
confidence_low      = jumlah nilai "l"
confidence_nominal  = jumlah nilai "n"      # bukan high
confidence_high     = jumlah nilai "h"
daynight_D          = jumlah nilai "D"
daynight_N          = jumlah nilai "N"
frp_sum             = Σ frp
frp_mean            = frp_sum / detections
frp_zero_count      = jumlah frp == 0
type_0 .. type_3    = distribusi kode type (archive)
type_not_available  = jumlah baris tanpa kolom type (NRT)
```

Rata-rata FRP memakai semua nilai non-negatif, termasuk nol. Median dan
persentil tidak dihitung karena memerlukan penyimpanan seluruh nilai.

---

## 6. Flowchart

```
┌─────────────────────────────┐
│ Mulai                       │
└──────────────┬──────────────┘
               v
┌─────────────────────────────┐      ┌──────────────────────────────┐
│ Baca config/datasets.json   │─────>│ Baca validation_summary.json │
└──────────────┬──────────────┘      │ (opsional, hanya cek silang) │
               v                     └──────────────────────────────┘
┌─────────────────────────────┐
│ Inisialisasi akumulator:    │
│ yearly, monthly, grids,     │
│ stats, feed_rows            │
└──────────────┬──────────────┘
               v
       ┌───────────────────┐
       │ Untuk 10 dataset  │<─────────────────────┐
       └───────┬───────────┘                      │
               v                                  │
┌─────────────────────────────┐                   │
│ Buka satu CSV (streaming)   │                   │
│ petakan indeks kolom        │                   │
└──────────────┬──────────────┘                   │
               v                                  │
       ┌───────────────────┐                      │
       │ Untuk setiap baris│<──────────┐          │
       └───────┬───────────┘           │          │
               v                       │          │
┌─────────────────────────────┐        │          │
│ Validasi panjang baris,     │        │          │
│ tanggal, koordinat, FRP     │────────┘          │
│ Update agregat satu lintasan│  baris berikutnya │
└──────────────┬──────────────┘                   │
               v                                  │
       ┌───────────────────┐                      │
       │ Masih ada dataset?│──────────────────────┘
       └───────┬───────────┘   ya
               │ tidak
               v
┌─────────────────────────────┐
│ Bentuk agregat combined     │
└──────────────┬──────────────┘
               v
┌─────────────────────────────┐
│ Tulis 4 CSV hasil agregasi  │
└──────────────┬──────────────┘
               v
┌─────────────────────────────┐
│ Cek konsistensi 12.416.036  │
│ baris (manifest/validasi)   │
└──────────────┬──────────────┘
               v
┌─────────────────────────────┐
│ Tulis JSON + laporan TXT    │
└──────────────┬──────────────┘
               v
┌─────────────────────────────┐
│ Selesai                     │
└─────────────────────────────┘
```

---

## 7. Mengapa streaming CSV

1. **Ukuran data:** 12.416.036 baris, 956,90 MiB. Memuat seluruh CSV ke
   list/dict akan memakan memori jauh lebih besar daripada ukuran file.
2. **Satu lintasan:** semua agregat (tahunan, bulanan, grid, confidence,
   daynight, type, FRP) dihitung saat baris dibaca. Tidak ada pembacaan
   ulang per pertanyaan analisis.
3. **Memori terukur:** puncak proses 39,3 MiB (PeakWorkingSetSize) pada
   eksekusi aktual, sementara file terbesar 393,94 MiB.
4. **Sederhana dan deterministik:** `csv.reader` membaca baris demi baris;
   tidak ada sampling dan tidak ada data yang dibuang.
5. **Aman untuk file besar:** satu file diproses sampai selesai sebelum file
   berikutnya dibuka, sehingga jumlah file terbuka tetap satu.

## 8. Mengapa Python standalone

1. **Tanpa instalasi tambahan:** hanya `csv`, `json`, `math`, `time`,
   `collections`, dan `pathlib` dari standard library.
2. **Tidak memerlukan cluster untuk komputasi:** data hanya 956,90 MiB dan
   analisisnya satu lintasan; MapReduce/Spark akan menambah kompleksitas
   tanpa manfaat berarti untuk skala ini.
3. **Mudah diaudit:** satu script, satu alur, output CSV/JSON/TXT yang dapat
   diperiksa ulang.
4. **Reproducible:** input, output, dan urutan pengolahan eksplisit; hasil
   dapat dihitung ulang kapan saja.
5. **HDFS tetap dipakai sebagai penyimpanan:** bukti HDFS ada di
   `laporan/bukti_hdfs/`; komputasi lokal tidak mengubah isi HDFS.

---

## 9. Kompleksitas

| Aspek | Nilai |
|---|---|
| Waktu | O(N), N = 12.416.036 baris; satu kali baca per file |
| Memori | O(G + Y + M + C); G = 31.529 sel grid, Y = tahun, M = bulan, C = negara/feed |
| Runtime aktual | 125,8 detik untuk 10 file |
| Puncak memori aktual | 39,3 MiB |
| Pembacaan file | 1 kali per CSV |

---

## 10. Pemetaan pseudocode ke script

| Pseudocode | Fungsi di `03_analyze.py` |
|---|---|
| Baca manifest/validasi | `main`, `load_validation` |
| Inisialisasi akumulator | `main`, `new_stats` |
| ProsesFile | `analyze_dataset` |
| Gabungan archive+NRT | `combine_stats`, `combined_yearly`, `combined_monthly` |
| Tulis tahunan | `write_yearly` |
| Tulis bulanan | `write_monthly` |
| Top 20 grid | `build_top_grids`, `write_spatial` |
| Karakteristik | `write_characteristics` |
| Cek konsistensi & laporan | `main` |

---

## 11. Batasan yang melekat pada algoritma

- Tidak melakukan deduplikasi baris.
- Tidak memeriksa overlap spasial antarnegara; grid selalu dipisah per negara.
- Tidak mengisi gap archive–NRT (misalnya Myanmar 31 Juli–1 Agustus 2026).
- Tidak menghitung median/persentil FRP.
- Tidak mengubah `type` menjadi label kebakaran; arti kode tidak diklaim.
- Jumlah deteksi bukan jumlah kebakaran terkonfirmasi dan FRP bukan luas
  area terbakar.
- Tahun 2012 dan 2026 adalah tahun parsial dan ditandai di output.
