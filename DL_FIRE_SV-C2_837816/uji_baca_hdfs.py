import csv
import io
import resource
import subprocess
import time

PATHS = {
    "archive": "/karhutla/raw/archive/fire_archive_SV-C2_837816.csv",
    "nrt": "/karhutla/raw/nrt/fire_nrt_SV-C2_837816.csv",
}

EXPECTED = {"archive": 3568824, "nrt": 571763}


def hitung_baris_dari_hdfs(path_hdfs):
    """Membaca CSV dari HDFS secara streaming dan menghitung barisnya."""
    proses = subprocess.Popen(
        ["hdfs", "dfs", "-cat", path_hdfs],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    teks = io.TextIOWrapper(proses.stdout, encoding="utf-8", newline="")
    reader = csv.DictReader(teks)
    kolom = reader.fieldnames

    jumlah = 0
    for _ in reader:
        jumlah += 1

    proses.stdout.close()
    kode = proses.wait()

    if kode != 0:
        raise RuntimeError(f"hdfs dfs -cat gagal (exit code {kode}) untuk {path_hdfs}")

    return jumlah, kolom


waktu_mulai = time.time()
total = 0

for nama, path in PATHS.items():
    t0 = time.time()
    jumlah, kolom = hitung_baris_dari_hdfs(path)
    durasi = time.time() - t0
    total += jumlah

    status = "COCOK" if jumlah == EXPECTED[nama] else "TIDAK COCOK"
    print(f"\n[{nama}] {path}")
    print(f"  Jumlah kolom : {len(kolom)}")
    print(f"  Nama kolom   : {kolom}")
    print(f"  Jumlah baris : {jumlah:,} (diharapkan {EXPECTED[nama]:,}) -> {status}")
    print(f"  Waktu baca   : {durasi:.1f} detik")

print("\n" + "=" * 50)
print(f"Total baris dari HDFS : {total:,}")
print(f"Total waktu           : {time.time() - waktu_mulai:.1f} detik")
puncak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
print(f"Puncak memori proses Python: {puncak_mb:.1f} MB")
