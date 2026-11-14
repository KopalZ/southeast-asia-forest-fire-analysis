import csv
import io
import resource
import subprocess
import time
from collections import Counter

FILES = [
    ("indonesia",   "archive", "/karhutla/raw/archive/fire_archive_SV-C2_837816.csv"),
    ("indonesia",   "nrt",     "/karhutla/raw/nrt/fire_nrt_SV-C2_837816.csv"),
    ("malaysia",    "archive", "/karhutla/raw/asean/malaysia/fire_archive_SV-C2_838165.csv"),
    ("malaysia",    "nrt",     "/karhutla/raw/asean/malaysia/fire_nrt_SV-C2_838165.csv"),
    ("myanmar",     "archive", "/karhutla/raw/asean/myanmar/fire_archive_SV-C2_838170.csv"),
    ("myanmar",     "nrt",     "/karhutla/raw/asean/myanmar/fire_nrt_SV-C2_838170.csv"),
    ("philippines", "archive", "/karhutla/raw/asean/philippines/fire_archive_SV-C2_838168.csv"),
    ("philippines", "nrt",     "/karhutla/raw/asean/philippines/fire_nrt_SV-C2_838168.csv"),
    ("thailand",    "archive", "/karhutla/raw/asean/thailand/fire_archive_SV-C2_838094.csv"),
    ("thailand",    "nrt",     "/karhutla/raw/asean/thailand/fire_nrt_SV-C2_838094.csv"),
]


def baca_hdfs(path):
    p = subprocess.Popen(
        ["hdfs", "dfs", "-cat", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    teks = io.TextIOWrapper(p.stdout, encoding="utf-8", newline="")
    reader = csv.DictReader(teks)
    yield reader.fieldnames
    for row in reader:
        yield row
    p.stdout.close()
    if p.wait() != 0:
        raise RuntimeError(f"hdfs dfs -cat gagal untuk {path}")


t_mulai = time.time()
hasil = []
tgl_per_negara = {}

for negara, jenis, path in FILES:
    t0 = time.time()
    gen = baca_hdfs(path)
    kolom = next(gen)
    n = 0
    tgl_min = tgl_max = None
    lat_min = lon_min = float("inf")
    lat_max = lon_max = float("-inf")
    tipe = Counter()
    instr = set()
    ada_type = "type" in kolom
    for row in gen:
        n += 1
        t = row["acq_date"]
        if tgl_min is None or t < tgl_min:
            tgl_min = t
        if tgl_max is None or t > tgl_max:
            tgl_max = t
        lat, lon = float(row["latitude"]), float(row["longitude"])
        lat_min, lat_max = min(lat_min, lat), max(lat_max, lat)
        lon_min, lon_max = min(lon_min, lon), max(lon_max, lon)
        instr.add((row["satellite"], row["instrument"]))
        if ada_type:
            tipe[row["type"]] += 1
    tgl_per_negara.setdefault(negara, {})[jenis] = (tgl_min, tgl_max)
    hasil.append([negara, jenis, path, n, len(kolom), tgl_min, tgl_max,
                  lat_min, lat_max, lon_min, lon_max,
                  ";".join(f"{a}/{b}" for a, b in sorted(instr)),
                  ";".join(f"type{k}={v}" for k, v in sorted(tipe.items())) if ada_type else "tanpa kolom type"])
    print(f"\n[{negara} | {jenis}] {path}")
    print(f"  Baris   : {n:,} | Kolom: {len(kolom)} | type: {'ada' if ada_type else 'TIDAK ADA'}")
    print(f"  Tanggal : {tgl_min} s.d. {tgl_max}")
    print(f"  Lat     : {lat_min} s.d. {lat_max} | Lon: {lon_min} s.d. {lon_max}")
    print(f"  Sensor  : {sorted(instr)}")
    if ada_type:
        print(f"  Sebaran type: {dict(sorted(tipe.items()))}")
    print(f"  Waktu   : {time.time() - t0:.1f} detik")

with open("hasil_asean/metadata_file.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["negara", "jenis", "path_hdfs", "baris", "kolom", "tgl_awal", "tgl_akhir",
                "lat_min", "lat_max", "lon_min", "lon_max", "satelit_instrumen", "sebaran_type"])
    w.writerows(hasil)

print("\n" + "=" * 60)
print(f"TOTAL BARIS SEMUA FILE : {sum(r[3] for r in hasil):,}")
print("PEMERIKSAAN TUMPANG TINDIH ARCHIVE vs NRT (tanggal)")
for negara, d in tgl_per_negara.items():
    arc_akhir, nrt_awal = d["archive"][1], d["nrt"][0]
    print(f"  {negara:12s}: archive berakhir {arc_akhir}, NRT mulai {nrt_awal} -> "
          f"{'tidak tumpang tindih' if arc_akhir < nrt_awal else 'TUMPANG TINDIH'}")
print(f"\nTotal waktu: {time.time() - t_mulai:.1f} detik")
print(f"Puncak memori proses Python: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024:.1f} MB")
print("Metadata tersimpan: hasil_asean/metadata_file.csv")
