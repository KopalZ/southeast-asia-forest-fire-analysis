import csv
import os
from datetime import datetime

# Mengambil lokasi folder tempat kode.py berada
folder = os.path.dirname(os.path.abspath(__file__))

files = [
    os.path.join(folder, "fire_archive_SV-C2_837816.csv"),
    os.path.join(folder, "fire_nrt_SV-C2_837816.csv")
]

for filename in files:
    print(f"\n=== {filename} ===")

    total_rows = 0
    min_date = None
    max_date = None

    with open(filename, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        print("Kolom:", reader.fieldnames)

        for row in reader:
            total_rows += 1
            date = datetime.strptime(
                row["acq_date"], "%Y-%m-%d"
            ).date()

            if min_date is None or date < min_date:
                min_date = date

            if max_date is None or date > max_date:
                max_date = date

    print("Jumlah baris:", f"{total_rows:,}")
    print("Tanggal pertama:", min_date)
    print("Tanggal terakhir:", max_date)
