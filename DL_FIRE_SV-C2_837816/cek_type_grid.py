import csv
import io
import subprocess
import time
from collections import Counter, defaultdict

PATH = "/karhutla/raw/archive/fire_archive_SV-C2_837816.csv"

t0 = time.time()

proses = subprocess.Popen(
    ["hdfs", "dfs", "-cat", PATH],
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
)
teks = io.TextIOWrapper(proses.stdout, encoding="utf-8", newline="")
reader = csv.DictReader(teks)

type_total = Counter()
type_tahun = defaultdict(Counter)
grid_total = Counter()
grid_type = defaultdict(Counter)
grid_tahun = defaultdict(set)
grid_bulan = defaultdict(set)

for row in reader:
    tipe = row["type"]
    tahun = row["acq_date"][:4]
    bulan = row["acq_date"][:7]
    grid = (round(float(row["latitude"]), 1), round(float(row["longitude"]), 1))

    type_total[tipe] += 1
    type_tahun[tahun][tipe] += 1
    grid_total[grid] += 1
    grid_type[grid][tipe] += 1
    grid_tahun[grid].add(tahun)
    grid_bulan[grid].add(bulan)

proses.stdout.close()
kode = proses.wait()
if kode != 0:
    raise RuntimeError(f"hdfs dfs -cat gagal (exit code {kode})")

print("SEBARAN KOLOM type (Archive)")
for tipe, jumlah in sorted(type_total.items()):
    print(f"  type {tipe}: {jumlah:,}")

print("\nSEBARAN type PER TAHUN")
for tahun in sorted(type_tahun):
    rincian = ", ".join(
        f"type {t}={j:,}" for t, j in sorted(type_tahun[tahun].items())
    )
    print(f"  {tahun}: {rincian}")

print("\n10 GRID TERATAS (Archive saja)")
for grid, jumlah in grid_total.most_common(10):
    rincian = ", ".join(
        f"type {t}={j:,}" for t, j in sorted(grid_type[grid].items())
    )
    print(
        f"  {grid}: {jumlah:,} | {rincian} | "
        f"aktif {len(grid_tahun[grid])} tahun, {len(grid_bulan[grid])} bulan"
    )

print(f"\nWaktu proses: {time.time() - t0:.1f} detik")
