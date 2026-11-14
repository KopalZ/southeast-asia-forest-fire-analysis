import csv
import os
import sys

for path in sys.argv[1:]:
    ukuran = os.path.getsize(path)
    baris = 0
    min_lat = min_lon = float("inf")
    max_lat = max_lon = float("-inf")
    tgl_awal = tgl_akhir = None
    sensor = set()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        kolom = reader.fieldnames
        for row in reader:
            baris += 1
            lat, lon = float(row["latitude"]), float(row["longitude"])
            min_lat, max_lat = min(min_lat, lat), max(max_lat, lat)
            min_lon, max_lon = min(min_lon, lon), max(max_lon, lon)
            t = row["acq_date"]
            if tgl_awal is None or t < tgl_awal:
                tgl_awal = t
            if tgl_akhir is None or t > tgl_akhir:
                tgl_akhir = t
            sensor.add((row["satellite"], row["instrument"]))
    print("\n" + os.path.basename(path))
    print(f"  Ukuran     : {ukuran:,} byte ({ukuran/1048576:.1f} MiB)")
    print(f"  Baris      : {baris:,}")
    print(f"  Kolom ({len(kolom)}) : {kolom}")
    print(f"  Tanggal    : {tgl_awal} s.d. {tgl_akhir}")
    print(f"  Lat        : {min_lat} s.d. {max_lat}")
    print(f"  Lon        : {min_lon} s.d. {max_lon}")
    print(f"  Satelit/instrumen: {sensor}")
