import csv
import os
import matplotlib.pyplot as plt

# Lokasi folder tempat visualisasi.py berada
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Folder input dan output
folder_input = os.path.join(BASE_DIR, "hasil_analisis")
folder_output = os.path.join(folder_input, "visualisasi")

os.makedirs(folder_output, exist_ok=True)


def baca_csv(nama_file):
    path = os.path.join(folder_input, nama_file)

    with open(path, "r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


# ==========================================
# 1. GRAFIK JUMLAH HOTSPOT PER TAHUN
# ==========================================

data_tahunan = baca_csv("hotspot_tahunan.csv")

tahun = [row["tahun"] for row in data_tahunan]
jumlah_tahunan = [
    int(row["jumlah_hotspot"]) for row in data_tahunan
]

plt.figure(figsize=(12, 6))
plt.bar(tahun, jumlah_tahunan)

plt.title("Jumlah Deteksi Hotspot per Tahun (2012–2026)")
plt.xlabel("Tahun")
plt.ylabel("Jumlah Deteksi Hotspot")
plt.xticks(rotation=45)
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig(
    os.path.join(folder_output, "01_hotspot_tahunan.png"),
    dpi=200
)
plt.close()


# ==========================================
# 2. POLA MUSIMAN BERDASARKAN BULAN
# ==========================================

data_bulanan = baca_csv("hotspot_bulanan.csv")

# Jumlahkan Januari dari semua tahun, Februari dari semua tahun, dst.
jumlah_per_bulan = {str(i).zfill(2): 0 for i in range(1, 13)}

for row in data_bulanan:
    bulan = row["bulan"].split("-")[1]
    jumlah_per_bulan[bulan] += int(row["jumlah_hotspot"])

nama_bulan = [
    "Jan", "Feb", "Mar", "Apr", "Mei", "Jun",
    "Jul", "Agu", "Sep", "Okt", "Nov", "Des"
]

nilai_bulan = [
    jumlah_per_bulan[str(i).zfill(2)]
    for i in range(1, 13)
]

plt.figure(figsize=(11, 6))
plt.bar(nama_bulan, nilai_bulan)

plt.title("Distribusi Deteksi Hotspot Berdasarkan Bulan")
plt.xlabel("Bulan")
plt.ylabel("Total Deteksi Hotspot")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig(
    os.path.join(folder_output, "02_pola_musiman.png"),
    dpi=200
)
plt.close()


# ==========================================
# 3. KONSENTRASI SPASIAL 15 GRID TERTINGGI
# ==========================================

data_spasial = baca_csv("hotspot_spasial.csv")

data_spasial.sort(
    key=lambda row: int(row["jumlah_hotspot"]),
    reverse=True
)

top_spasial = data_spasial[:15]

latitude = [
    float(row["grid_latitude"]) for row in top_spasial
]

longitude = [
    float(row["grid_longitude"]) for row in top_spasial
]

jumlah_spasial = [
    int(row["jumlah_hotspot"]) for row in top_spasial
]

plt.figure(figsize=(11, 7))

scatter = plt.scatter(
    longitude,
    latitude,
    s=[nilai / 15 for nilai in jumlah_spasial],
    c=jumlah_spasial,
    alpha=0.75
)

for i, row in enumerate(top_spasial):
    plt.annotate(
        f'{row["grid_latitude"]}, {row["grid_longitude"]}',
        (longitude[i], latitude[i]),
        xytext=(5, 5),
        textcoords="offset points",
        fontsize=8
    )

plt.colorbar(scatter, label="Jumlah Deteksi Hotspot")
plt.title("15 Grid dengan Konsentrasi Deteksi Hotspot Tertinggi")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.grid(alpha=0.3)
plt.tight_layout()

plt.savefig(
    os.path.join(folder_output, "03_konsentrasi_spasial.png"),
    dpi=200
)
plt.close()


# ==========================================
# 4. KARAKTERISTIK CONFIDENCE DAN DAY/NIGHT
# ==========================================

data_karakteristik = baca_csv("hotspot_karakteristik.csv")

confidence = {}
daynight = {}

for row in data_karakteristik:
    jenis = row["jenis"]
    kategori = row["kategori"]
    jumlah = int(row["jumlah"])

    if jenis == "confidence":
        confidence[kategori] = jumlah
    elif jenis == "daynight":
        daynight[kategori] = jumlah

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

# Confidence
kategori_conf = ["l", "n", "h"]
nilai_conf = [confidence.get(k, 0) for k in kategori_conf]

axes[0].bar(kategori_conf, nilai_conf)
axes[0].set_title("Distribusi Confidence")
axes[0].set_xlabel("Kategori Confidence")
axes[0].set_ylabel("Jumlah Deteksi")
axes[0].grid(axis="y", alpha=0.3)

# Siang dan malam
kategori_dn = ["D", "N"]
nilai_dn = [daynight.get(k, 0) for k in kategori_dn]

axes[1].bar(kategori_dn, nilai_dn)
axes[1].set_title("Distribusi Deteksi Siang dan Malam")
axes[1].set_xlabel("Day/Night (D=Siang, N=Malam)")
axes[1].set_ylabel("Jumlah Deteksi")
axes[1].grid(axis="y", alpha=0.3)

plt.tight_layout()

plt.savefig(
    os.path.join(folder_output, "04_karakteristik_deteksi.png"),
    dpi=200
)
plt.close()


print("VISUALISASI SELESAI")
print("Semua grafik disimpan di:", folder_output)

for nama_file in os.listdir(folder_output):
    print("-", nama_file)
