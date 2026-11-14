
import csv
import os
from collections import defaultdict
from datetime import datetime

# ==================================================
# 1. KONFIGURASI FILE
# ==================================================

folder = os.path.dirname(os.path.abspath(__file__))

files = [
    os.path.join(folder, "fire_archive_SV-C2_837816.csv"),
    os.path.join(folder, "fire_nrt_SV-C2_837816.csv")
]

output_folder = os.path.join(folder, "hasil_analisis")
os.makedirs(output_folder, exist_ok=True)

# ==================================================
# 2. VARIABEL AGREGASI
# ==================================================

total_rows = 0

jumlah_tahunan = defaultdict(int)
jumlah_bulanan = defaultdict(int)
jumlah_spasial = defaultdict(int)

jumlah_confidence = defaultdict(int)
jumlah_daynight = defaultdict(int)

# Statistik FRP
total_frp = 0.0
jumlah_frp = 0
min_frp = float("inf")
max_frp = float("-inf")

# Statistik geografis
min_lat = float("inf")
max_lat = float("-inf")
min_lon = float("inf")
max_lon = float("-inf")

tanggal_awal = None
tanggal_akhir = None

# ==================================================
# 3. MEMBACA DAN MENGANALISIS DATA
# ==================================================

for filename in files:

    print(f"\nMembaca: {os.path.basename(filename)}")

    with open(filename, "r", newline="", encoding="utf-8") as f:

        reader = csv.DictReader(f)

        for row in reader:

            total_rows += 1

            # Tanggal akuisisi
            tanggal = datetime.strptime(
                row["acq_date"], "%Y-%m-%d"
            ).date()

            tahun = tanggal.year
            bulan = tanggal.strftime("%Y-%m")

            jumlah_tahunan[tahun] += 1
            jumlah_bulanan[bulan] += 1

            if tanggal_awal is None or tanggal < tanggal_awal:
                tanggal_awal = tanggal

            if tanggal_akhir is None or tanggal > tanggal_akhir:
                tanggal_akhir = tanggal

            # Koordinat geografis
            lat = float(row["latitude"])
            lon = float(row["longitude"])

            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)
            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)

            # Pengelompokan grid 0.1 derajat
            grid_lat = round(lat, 1)
            grid_lon = round(lon, 1)

            jumlah_spasial[(grid_lat, grid_lon)] += 1

            # Confidence
            confidence = row["confidence"]
            jumlah_confidence[confidence] += 1

            # Siang atau malam
            daynight = row["daynight"]
            jumlah_daynight[daynight] += 1

            # Fire Radiative Power (FRP)
            frp = float(row["frp"])

            total_frp += frp
            jumlah_frp += 1

            min_frp = min(min_frp, frp)
            max_frp = max(max_frp, frp)

    print("Selesai:", os.path.basename(filename))

# ==================================================
# 4. MENYIMPAN HASIL ANALISIS KE CSV
# ==================================================

def simpan_csv(nama_file, header, data):

    path = os.path.join(output_folder, nama_file)

    with open(path, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)

    print("Tersimpan:", path)


# A. Analisis tahunan
data_tahunan = [
    [tahun, jumlah]
    for tahun, jumlah in sorted(jumlah_tahunan.items())
]

simpan_csv(
    "hotspot_tahunan.csv",
    ["tahun", "jumlah_hotspot"],
    data_tahunan
)

# B. Analisis bulanan
data_bulanan = [
    [bulan, jumlah]
    for bulan, jumlah in sorted(jumlah_bulanan.items())
]

simpan_csv(
    "hotspot_bulanan.csv",
    ["bulan", "jumlah_hotspot"],
    data_bulanan
)

# C. Analisis spasial
data_spasial = [
    [lat, lon, jumlah]
    for (lat, lon), jumlah in sorted(
        jumlah_spasial.items(),
        key=lambda item: item[1],
        reverse=True
    )
]

simpan_csv(
    "hotspot_spasial.csv",
    ["grid_latitude", "grid_longitude", "jumlah_hotspot"],
    data_spasial
)

# D. Analisis confidence dan day/night
data_karakteristik = []

for kategori, jumlah in sorted(jumlah_confidence.items()):
    data_karakteristik.append(["confidence", kategori, jumlah])

for kategori, jumlah in sorted(jumlah_daynight.items()):
    data_karakteristik.append(["daynight", kategori, jumlah])

simpan_csv(
    "hotspot_karakteristik.csv",
    ["jenis", "kategori", "jumlah"],
    data_karakteristik
)

# ==================================================
# 5. MENYIMPAN RINGKASAN
# ==================================================

rata_frp = total_frp / jumlah_frp if jumlah_frp else 0

summary_path = os.path.join(output_folder, "ringkasan.txt")

with open(summary_path, "w", encoding="utf-8") as f:

    f.write("RINGKASAN ANALISIS HOTSPOT NASA FIRMS\n")
    f.write("=" * 45 + "\n")

    f.write(f"Total baris: {total_rows:,}\n")
    f.write(f"Tanggal awal: {tanggal_awal}\n")
    f.write(f"Tanggal akhir: {tanggal_akhir}\n")

    f.write(f"Jumlah tahun: {len(jumlah_tahunan)}\n")
    f.write(f"Jumlah bulan: {len(jumlah_bulanan)}\n")
    f.write(f"Jumlah grid spasial: {len(jumlah_spasial)}\n")

    f.write(f"Latitude minimum: {min_lat}\n")
    f.write(f"Latitude maksimum: {max_lat}\n")
    f.write(f"Longitude minimum: {min_lon}\n")
    f.write(f"Longitude maksimum: {max_lon}\n")

    f.write(f"Rata-rata FRP: {rata_frp:.2f}\n")
    f.write(f"FRP minimum: {min_frp}\n")
    f.write(f"FRP maksimum: {max_frp}\n")

    f.write("\nTAHUN DENGAN HOTSPOT TERBANYAK\n")

    for tahun, jumlah in sorted(
        jumlah_tahunan.items(),
        key=lambda item: item[1],
        reverse=True
    )[:5]:

        f.write(f"{tahun}: {jumlah:,}\n")

    f.write("\nBULAN DENGAN HOTSPOT TERBANYAK\n")

    for bulan, jumlah in sorted(
        jumlah_bulanan.items(),
        key=lambda item: item[1],
        reverse=True
    )[:5]:

        f.write(f"{bulan}: {jumlah:,}\n")

print("\n========================================")
print("ANALISIS SELESAI")
print("========================================")
print(f"Total baris dianalisis: {total_rows:,}")
print(f"Rentang tanggal: {tanggal_awal} - {tanggal_akhir}")
print(f"Rata-rata FRP: {rata_frp:.2f}")
print(f"Hasil tersimpan di: {output_folder}")
