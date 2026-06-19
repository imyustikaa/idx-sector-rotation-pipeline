# IDX Sector Rotation: Weekly Batch ETL Pipeline

Pipeline ETL yang digunakan untuk menganalisis **rotasi sektor di Bursa Efek Indonesia (IDX)** yang akan otomatis berjalan setiap hari Senin jam 7 pagi. Pipeline ini mengambil data harga saham dari 11 sektor (33 ticker) + benchmark IHSG untuk menghitung metrik keuangan, seperti price momentum, sharpe ratio, rolling beta, dan max drawdown. Pipeline ini menghasilkan sinyal **BUY / HOLD / SELL** per sektor yang diperbarui setiap minggunya. Semua task berjalan otomatis lewat Airflow dan bisa dilihat di dashboard Metabase.

## Alur Data Pipeline:
```
library yfinance (sumber data saham)
   │
   ▼
[1] EXTRACT  → Python script mengambil data harga saham → disimpan sebagai file Parquet di MinIO (data lake)
   │
   ▼
[2] TRANSFORM → Apache Spark membaca data dari MinIO, menghitung metrik, lalu simpan ke PostgreSQL (schema silver)
   │
   ▼
[3] LOAD → dbt memroses data modelling dan menghasilkan tabel hasil akhir di schema gold (PostgreSQL)
   │
   ▼
[4] TEST → dbt test, mengecek kualitas data (kolom tidak boleh kosong, sinyal harus valid, dll)
   │
   ▼
[5] SYNC → Metabase di-refresh agar dashboard menampilkan data terbaru
```
Semua langkah ini berjalan otomatis melalui DAG Airflow bernama `idx_sector_dag.py`.

### Tools yang Digunakan
| Tools | Fungsi |
|---|---|
| **Airflow** | "Penjadwal" yang menjalankan semua langkah secara berurutan tiap minggu |
| **yfinance** | Library Python untuk mengambil data harga saham dari Yahoo Finance |
| **MinIO** | Object storage untuk menyimpan data mentah |
| **Apache Spark** | Mesin pemroses data besar, menghitung metrik dari data harga |
| **PostgreSQL (warehouse)** | Database tempat data hasil olahan disimpan |
| **dbt** | Tool untuk transformasi data di dalam database pakai SQL, sekaligus menguji kualitas data |
| **Metabase** | Dashboard untuk melihat hasil analisis secara visual |

---

## 2. Struktur Folder Project
```
project/
├── docker-compose.yml
├── Dockerfile
├── dags/
│   └── idx_sector_dag.py
├── extract/
│   └── extract_idx_sector.py
├── transform/
│   └── transform_spark.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── macros/
│       ├── generate_schema_name.sql
│   └── models/
│       ├── silver/
│       │   ├── models.yml
│       │   ├── sources.yml
│       │   ├── stg_benchmark.sql
│       │   └── stg_ticker_metrics.sql
│       └── gold/
│           ├── sector_rotation.sql
│           ├── sector_ranking.sql
│           └── rotation_history.sql
└── scripts/
    └── init_warehouse.sql
```
---

## 3. Prasyarat: Yang Harus Sudah Terpasang

- **Docker** dan **Docker Compose** (semua service jalan sebagai container)
- Koneksi internet aktif (untuk download data dari Yahoo Finance dan image Docker)
- RAM minimal disarankan **8 GB** ke atas (karena ada Spark + Airflow + 3 PostgreSQL + Metabase jalan bersamaan)

Cek dulu apakah Docker sudah ada:
```bash
docker --version
docker compose version
```

---

## 4. Langkah Instalasi & Menjalankan Pipeline

### Step 1 — Jalankan semua service dengan Docker Compose

Dari folder root project:

```bash
docker compose up -d --build
```
Ini akan menyalakan service berikut:
- `postgres-airflow` (database metadata Airflow)
- `postgres-warehouse` (database hasil olahan, port host **5433**)
- `minio` (data lake, UI di port **9001**)
- `minio-init` (otomatis bikin bucket `idx-raw` & `idx-processed`)
- `spark-master` & `spark-worker` (mesin pemroses data)
- `airflow-init` (setup awal, bikin user admin)
- `airflow-webserver` (UI Airflow, port host **8081**)
- `airflow-scheduler` (penjadwal DAG)
- `postgres-metabase` (database metadata Metabase)
- `metabase` (dashboard, port **3000**)
Tunggu beberapa menit sampai semua container `healthy`.

### Step 2 — Cek Database Warehouse Sudah Terbentuk

Schema `silver` dan `gold` akan otomatis dibuat oleh `init_warehouse.sql` saat container `postgres-warehouse` pertama kali dijalankan.
```bash
docker exec -it postgres_warehouse psql -U warehouse -d idx_warehouse -c "\dn"
```

### Step 3 — Login ke Airflow & Setup Variable

Buka browser ke:
```
http://localhost:8081
```
Login pakai akun default yang dibuat melalui service `airflow-init`:
- **Username:** `admin`
- **Password:** `admin`

#### Setup Airflow Variables (WAJIB sebelum DAG dijalankan)

DAG ini butuh beberapa **Airflow Variables** supaya task `metabase_sync` dan fitur full-refresh dbt bisa berjalan. 
Caranya:
1. Di menu atas, klik **Admin → Variables**
2. Klik tombol **+** (Add a new record)
3. Tambahkan variable berikut satu per satu:
4. 
| Key | Value | Keterangan |
|---|---|---|
| `metabase_admin` | *(email admin Metabase)* | Dipakai untuk login otomatis ke Metabase API |
| `metabase_pass` | *(password admin Metabase)* | Pasangan dari akun di atas |
| `dbt_full_refresh` | `false` | Kalau `true`, dbt akan rebuild ulang total tabel gold (full refresh). Pipeline otomatis mereset ini ke `false` setelah dipakai sekali |

4. Klik **Save**.


### Step 4 — Setup Metabase (Akun Admin & Koneksi Database)
Buka browser ke:
```
http://localhost:3000
```

Saat pertama kali dibuka, perlu melakakuan ini:
1. **Buat akun admin** — isi nama, email, dan password. **Email & password inilah yang nanti dimasukkan ke Airflow Variable `metabase_admin` dan `metabase_pass`.**
2. **Hubungkan ke database warehouse**, isi seperti berikut:

| Field | Value |
|---|---|
| Database type | PostgreSQL |
| Host | `postgres-warehouse` |
| Port | `5432` |
| Database name | `idx_warehouse` |
| Username | `warehouse` |
| Password | `warehouse` |

3. Selesaikan wizard sampai masuk ke dashboard utama Metabase.
4. **Cek ID Database** — buka **Admin Settings → Databases**, klik database warehouse yang baru dibuat, lalu lihat angka di URL (contoh: `/admin/databases/2`). Angka ini harus **sama** dengan `MB_WAREHOUSE_DB_ID` di `idx_sector_dag.py` (defaultnya `2`). Kalau beda, edit baris berikut di `idx_sector_dag.py`:
   ```python
   MB_WAREHOUSE_DB_ID = 2  # ganti sesuai ID database di Metabase
   ```
5. Setelah dbt selesai membuat tabel di schema `gold` (lewat run DAG pertama kali), kembali ke Metabase dan membuat **dashboard/chart** dari tabel berikut:
   - `gold.sector_rotation` — tabel utama, skor & sinyal per sektor per minggu
   - `gold.sector_ranking` — ranking sektor terbaru
   - `gold.rotation_history` — riwayat rotasi sektor (urut dari minggu terbaru)

---

## 5. Urutan Task di Dalam DAG
```
upgrade_yfinance
      │
      ▼
extract_data           ← ambil data saham dari yfinance, simpan ke MinIO
      │
      ▼
transform_spark         ← Spark baca dari MinIO, hitung metrik, simpan ke silver (PostgreSQL)
      │
      ▼
dbt_run                 ← dbt bangun tabel gold (sector_rotation, sector_ranking, rotation_history)
      │
      ▼
reset_dbt_fullrefresh   ← reset flag full_refresh balik ke false
      │
      ▼
dbt_test                ← cek kualitas data (not_null, accepted_values)
      │
      ▼
metabase_sync           ← Metabase sync schema & rescan data terbaru
```

Penjelasan tiap task:
| Task | Detail |
|---|---|
| `upgrade_yfinance` | Update library `yfinance`, install ulang `cffi`, install `dbt-postgres` sebelum pipeline jalan |
| `extract_data` | Download data harga saham 11 sektor + IHSG dari Yahoo Finance, simpan sebagai file `.parquet` ke MinIO bucket `idx-raw` |
| `transform_spark` | Spark membaca parquet dari MinIO, menghitung 4 metrik (price momentum, sharpe ratio, rolling beta, max drawdown), lalu menulis ke tabel `silver.*` di PostgreSQL |
| `dbt_run` | Menjalankan model dbt: dari `silver` (view, hasil cleaning) ke `gold` (tabel hasil scoring & ranking sektor) |
| `reset_dbt_fullrefresh` | Mengembalikan Airflow Variable `dbt_full_refresh` ke `false` setelah dipakai, supaya run berikutnya normal (incremental) |
| `dbt_test` | Menjalankan test kualitas data dbt (kolom wajib tidak boleh kosong, nilai `signal` harus salah satu dari `BUY`/`HOLD`/`SELL`) |
| `metabase_sync` | Login ke Metabase via API, lalu trigger `sync_schema` dan `rescan_values` supaya dashboard menampilkan data terbaru |

---

## 6. Hasil Visualisasi di Metabase
<img width="1180" height="662" alt="Dashboard IDX Sector Rotation-1" src="https://github.com/user-attachments/assets/a22d7a4d-29c1-4393-94ae-74ed36a19ae0" />
<img width="1180" height="539" alt="Dashboard IDX Sector Rotation-2" src="https://github.com/user-attachments/assets/7bb4bcf0-986a-4dbf-992c-e27c1efa593a" />
<img width="858" height="414" alt="Dashboard IDX Sector Rotation-3" src="https://github.com/user-attachments/assets/8d60a3ac-20bb-4c36-962d-f2aba4e9ff47" />


---

## 7. Penjelasan Metrik & Sinyal
Setiap saham dihitung 4 metrik mingguan:
- **Price Momentum**: perbandingan harga 1 bulan lalu vs 1 tahun lalu (tren naik/turun jangka menengah)
- **Sharpe Ratio**: rasio return terhadap risiko (semakin tinggi semakin bagus secara risk-adjusted)
- **Rolling Beta**: seberapa sensitif saham terhadap pergerakan IHSG
- **Max Drawdown**: penurunan terbesar dari titik puncak (mengukur risiko kerugian)

Skor gabungan (`momentum_score`) dihitung dari ranking persentil ke-4 metrik tersebut, lalu sektor di-ranking. Hasil akhirnya:
- Ranking **1–3** → sinyal **BUY**
- Ranking **4–7** → sinyal **HOLD**
- Ranking **8–11** → sinyal **SELL**

---

## 8. Ringkasan Port yang Dipakai
| Service | Port di Host | Akses |
|---|---|---|
| Airflow Webserver | `8081` | http://localhost:8081 |
| Metabase | `3000` | http://localhost:3000 |
| MinIO API | `9000` | http://localhost:9000 |
| MinIO Console | `9001` | http://localhost:9001 |
| PostgreSQL Warehouse | `5433` | `localhost:5433` (dari luar Docker) |
| Spark Master UI | `8080` | http://localhost:8080 |

---
