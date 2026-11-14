import csv
import io
import os
import subprocess
import time
from collections import Counter

ARCHIVE = "/karhutla/raw/archive/fire_archive_SV-C2_837816.csv"
NRT = "/karhutla/raw/nrt/fire_nrt_SV-C2_837816.csv"
MIN_TOTAL = 50                  # grid dengan deteksi < 50 diabaikan (terlalu sedikit)
AMBANG = [0.3, 0.5, 0.7]        # porsi non-vegetasi minimum agar grid dianggap statis


def baca_hdfs(path):
    p = subprocess.Popen(
        ["hdfs", "dfs", "-cat", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    teks = io.TextIOWrapper(p.stdout, encoding="utf-8", newline="")
    for row in csv.DictReader(teks):
        yield row
    p.stdout.close()
    if p.wait() != 0:
        raise RuntimeError(f"hdfs dfs -cat gagal untuk {path}")


def grid_dari(row):
    return (round(float(row["latitude"]), 1), round(float(row["longitude"]), 1))


t0 = time.time()

# --- Pass 1: Archive ---
grid_total = Counter()
grid_nonveg = Counter()
for row in baca_hdfs(ARCHIVE):
    g = grid_dari(row)
    grid_total[g] += 1
    if row["type"] != "0":
        grid_nonveg[g] += 1

total_arc = sum(grid_total.values())
total_nonveg = sum(grid_nonveg.values())
total_veg = total_arc - total_nonveg

# --- Pass 2: NRT ---
grid_nrt = Counter()
for row in baca_hdfs(NRT):
    grid_nrt[grid_dari(row)] += 1
total_nrt = sum(grid_nrt.values())

print(f"Grid unik di Archive : {len(grid_total):,}")
print(f"Archive total        : {total_arc:,} (type 0 = {total_veg:,}; non-0 = {total_nonveg:,})")
print(f"NRT total            : {total_nrt:,}")

mask_050 = None
for thr in AMBANG:
    mask = {
        g for g, t in grid_total.items()
        if t >= MIN_TOTAL and grid_nonveg[g] / t >= thr
    }
    if thr == 0.5:
        mask_050 = mask

    det_mask = sum(grid_total[g] for g in mask)
    nonveg_mask = sum(grid_nonveg[g] for g in mask)
    veg_mask = det_mask - nonveg_mask
    nrt_mask = sum(grid_nrt[g] for g in mask)

    print(f"\n--- Ambang non-vegetasi >= {thr:.0%} (min {MIN_TOTAL} deteksi/grid) ---")
    print(f"Grid bermasker                          : {len(mask):,}")
    print(f"Deteksi Archive di grid bermasker       : {det_mask:,} ({det_mask/total_arc:.2%} dari Archive)")
    print(f"  non-vegetasi tertangkap               : {nonveg_mask:,} ({nonveg_mask/total_nonveg:.1%} dari semua non-vegetasi)")
    print(f"  type 0 ikut terbuang                  : {veg_mask:,} ({veg_mask/total_veg:.2%} dari semua type 0)")
    print(f"Deteksi NRT di grid bermasker           : {nrt_mask:,} ({nrt_mask/total_nrt:.2%} dari NRT)")

# Simpan daftar grid kandidat (ambang 50%) untuk dipakai di tahap berikutnya
os.makedirs("hasil_analisis", exist_ok=True)
path_out = os.path.join("hasil_analisis", "grid_statis_kandidat.csv")
with open(path_out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["grid_latitude", "grid_longitude", "total_archive", "non_vegetasi", "porsi_non_vegetasi"])
    for g in sorted(mask_050, key=lambda x: grid_total[x], reverse=True):
        w.writerow([g[0], g[1], grid_total[g], grid_nonveg[g], round(grid_nonveg[g] / grid_total[g], 4)])
print(f"\nDaftar grid kandidat (ambang 50%) disimpan: {path_out}")
print(f"Waktu proses: {time.time() - t0:.1f} detik")
