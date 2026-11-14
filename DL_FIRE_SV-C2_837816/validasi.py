
import csv
import os
from collections import Counter
from datetime import datetime

# Mengambil lokasi folder tempat program berada
folder = os.path.dirname(os.path.abspath(__file__))

files = [
    os.path.join(folder, "fire_archive_SV-C2_837816.csv"),
    os.path.join(folder, "fire_nrt_SV-C2_837816.csv")
]

# Memeriksa setiap file secara terpisah
for filename in files:

    print("\n" + "=" * 60)
    print("VALIDASI:", os.path.basename(filename))
    print("=" * 60)

    total_rows = 0

    missing = Counter()
    confidence = Counter()
    daynight = Counter()

    yearly = Counter()
    monthly = Counter()

    invalid_date = 0
    invalid_coordinate = 0
    invalid_frp = 0

    with open(filename, "r", encoding="utf-8", newline="") as f:

        reader = csv.DictReader(f)
        columns = reader.fieldnames

        print("Jumlah kolom:", len(columns))
        print("Nama kolom:", columns)

        for row in reader:

            total_rows += 1

            # 1. Memeriksa nilai kosong
            for col in columns:
                value = row.get(col)

                if value is None or value.strip() == "":
                    missing[col] += 1

            # 2. Memeriksa tanggal dan membuat kelompok waktu
            date = row.get("acq_date", "")

            try:
                parsed_date = datetime.strptime(
                    date, "%Y-%m-%d"
                )

                yearly[str(parsed_date.year)] += 1
                monthly[parsed_date.strftime("%Y-%m")] += 1

            except ValueError:
                invalid_date += 1

            # 3. Menghitung confidence
            confidence[row.get("confidence", "KOSONG")] += 1

            # 4. Menghitung siang dan malam
            daynight[row.get("daynight", "KOSONG")] += 1

            # 5. Memeriksa koordinat
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])

                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    invalid_coordinate += 1

            except (ValueError, TypeError, KeyError):
                invalid_coordinate += 1

            # 6. Memeriksa FRP
            try:
                frp = float(row["frp"])

                if frp < 0:
                    invalid_frp += 1

            except (ValueError, TypeError, KeyError):
                invalid_frp += 1

    # Menampilkan hasil
    print("\nTOTAL BARIS:", f"{total_rows:,}")

    print("\nMISSING VALUES:")
    for col in columns:
        print(f"{col}: {missing[col]:,}")

    print("\nCONFIDENCE:")
    for category, count in confidence.most_common():
        print(category, f"{count:,}")

    print("\nSIANG DAN MALAM:")
    for category, count in daynight.most_common():
        print(category, f"{count:,}")

    print("\nJUMLAH DETEKSI PER TAHUN:")
    for year, count in sorted(yearly.items()):
        print(year, f"{count:,}")

    print("\n5 BULAN DENGAN DETEKSI TERBANYAK:")
    for month, count in monthly.most_common(5):
        print(month, f"{count:,}")

    print("\nDATA TIDAK VALID:")
    print("Tanggal:", f"{invalid_date:,}")
    print("Koordinat:", f"{invalid_coordinate:,}")
    print("FRP:", f"{invalid_frp:,}")

    print("\nVALIDASI SELESAI")
