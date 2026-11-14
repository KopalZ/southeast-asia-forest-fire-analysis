"""Bangun manifest dataset lima negara dari file aktual.

Output:
  config/datasets.json
  outputs/manifest/manifest_summary.csv
  outputs/manifest/manifest_build_log.txt

Tidak menulis, memindahkan, atau mengubah file dataset. Hanya membaca.
"""

from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from common import (
    MANIFEST_PATH,
    PROJECT_ROOT,
    csv_stream,
    ensure_dir,
    format_int,
    resolve_path,
    sha256_file,
    write_json,
)

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "manifest"

# Nilai acuan berasal dari metadata yang sudah ada (hasil_asean/metadata_file.csv)
# dan diverifikasi ulang terhadap file aktual oleh script ini.
SEED = [
    {
        "country_code": "ID",
        "country_name": "Indonesia",
        "feed": "archive",
        "area_code": "837816",
        "path_local": "DL_FIRE_SV-C2_837816/fire_archive_SV-C2_837816.csv",
        "path_hdfs": "/karhutla/raw/archive/fire_archive_SV-C2_837816.csv",
        "expected_bytes": 289294138,
        "expected_rows": 3568824,
        "expected_columns": 15,
        "expected_has_type": True,
        "expected_date_min": "2012-01-20",
        "expected_date_max": "2026-07-31",
        "version_raw": "2",
        "is_baseline": True,
        "notes": "Baseline Indonesia v1; archive berakhir 2026-07-31, NRT mulai 2026-08-01.",
    },
    {
        "country_code": "ID",
        "country_name": "Indonesia",
        "feed": "nrt",
        "area_code": "837816",
        "path_local": "DL_FIRE_SV-C2_837816/fire_nrt_SV-C2_837816.csv",
        "path_hdfs": "/karhutla/raw/nrt/fire_nrt_SV-C2_837816.csv",
        "expected_bytes": 48147253,
        "expected_rows": 571763,
        "expected_columns": 14,
        "expected_has_type": False,
        "expected_date_min": "2026-08-01",
        "expected_date_max": "2026-11-02",
        "version_raw": "2.0NRT",
        "is_baseline": True,
        "notes": "NRT tanpa kolom type; data preliminer.",
    },
    {
        "country_code": "MY",
        "country_name": "Malaysia",
        "feed": "archive",
        "area_code": "838165",
        "path_local": "data_asia_tenggara/Malaysia/fire_archive_SV-C2_838165.csv",
        "path_hdfs": "/karhutla/raw/asean/malaysia/fire_archive_SV-C2_838165.csv",
        "expected_bytes": 18028691,
        "expected_rows": 224485,
        "expected_columns": 15,
        "expected_has_type": True,
        "expected_date_min": "2012-01-20",
        "expected_date_max": "2026-07-31",
        "version_raw": "2",
        "is_baseline": False,
        "notes": "Archive memiliki 23.601 deteksi non-vegetasi (type 2/3).",
    },
    {
        "country_code": "MY",
        "country_name": "Malaysia",
        "feed": "nrt",
        "area_code": "838165",
        "path_local": "data_asia_tenggara/Malaysia/fire_nrt_SV-C2_838165.csv",
        "path_hdfs": "/karhutla/raw/asean/malaysia/fire_nrt_SV-C2_838165.csv",
        "expected_bytes": 270322,
        "expected_rows": 3243,
        "expected_columns": 14,
        "expected_has_type": False,
        "expected_date_min": "2026-08-01",
        "expected_date_max": "2026-11-02",
        "version_raw": "2.0NRT",
        "is_baseline": False,
        "notes": "NRT tanpa kolom type; data preliminer.",
    },
    {
        "country_code": "MM",
        "country_name": "Myanmar",
        "feed": "archive",
        "area_code": "838170",
        "path_local": "data_asia_tenggara/Myanmar/fire_archive_SV-C2_838170.csv",
        "path_hdfs": "/karhutla/raw/asean/myanmar/fire_archive_SV-C2_838170.csv",
        "expected_bytes": 413081183,
        "expected_rows": 5144894,
        "expected_columns": 15,
        "expected_has_type": True,
        "expected_date_min": "2012-01-20",
        "expected_date_max": "2026-07-30",
        "version_raw": "2",
        "is_baseline": False,
        "notes": "Archive berakhir 2026-07-30; NRT mulai 2026-08-02 (gap 2 hari).",
    },
    {
        "country_code": "MM",
        "country_name": "Myanmar",
        "feed": "nrt",
        "area_code": "838170",
        "path_local": "data_asia_tenggara/Myanmar/fire_nrt_SV-C2_838170.csv",
        "path_hdfs": "/karhutla/raw/asean/myanmar/fire_nrt_SV-C2_838170.csv",
        "expected_bytes": 104108,
        "expected_rows": 1249,
        "expected_columns": 14,
        "expected_has_type": False,
        "expected_date_min": "2026-08-02",
        "expected_date_max": "2026-11-01",
        "version_raw": "2.0NRT",
        "is_baseline": False,
        "notes": "NRT tanpa kolom type; data preliminer; gap 2026-07-31..2026-08-01.",
    },
    {
        "country_code": "PH",
        "country_name": "Filipina",
        "feed": "archive",
        "area_code": "838168",
        "path_local": "data_asia_tenggara/Philippines/fire_archive_SV-C2_838168.csv",
        "path_hdfs": "/karhutla/raw/asean/philippines/fire_archive_SV-C2_838168.csv",
        "expected_bytes": 37209509,
        "expected_rows": 459570,
        "expected_columns": 15,
        "expected_has_type": True,
        "expected_date_min": "2012-01-20",
        "expected_date_max": "2026-07-31",
        "version_raw": "2",
        "is_baseline": False,
        "notes": "Archive memiliki type 1/2/3 (vulkanik, sumber statis, lepas pantai).",
    },
    {
        "country_code": "PH",
        "country_name": "Filipina",
        "feed": "nrt",
        "area_code": "838168",
        "path_local": "data_asia_tenggara/Philippines/fire_nrt_SV-C2_838168.csv",
        "path_hdfs": "/karhutla/raw/asean/philippines/fire_nrt_SV-C2_838168.csv",
        "expected_bytes": 232787,
        "expected_rows": 2779,
        "expected_columns": 14,
        "expected_has_type": False,
        "expected_date_min": "2026-08-01",
        "expected_date_max": "2026-11-02",
        "version_raw": "2.0NRT",
        "is_baseline": False,
        "notes": "NRT tanpa kolom type; data preliminer.",
    },
    {
        "country_code": "TH",
        "country_name": "Thailand",
        "feed": "archive",
        "area_code": "838094",
        "path_local": "data_asia_tenggara/Thailand/fire_archive_SV-C2_838094.csv",
        "path_hdfs": "/karhutla/raw/asean/thailand/fire_archive_SV-C2_838094.csv",
        "expected_bytes": 196890954,
        "expected_rows": 2437813,
        "expected_columns": 15,
        "expected_has_type": True,
        "expected_date_min": "2012-01-20",
        "expected_date_max": "2026-07-31",
        "version_raw": "2",
        "is_baseline": False,
        "notes": "Archive memiliki 20.533 deteksi non-vegetasi (type 2/3).",
    },
    {
        "country_code": "TH",
        "country_name": "Thailand",
        "feed": "nrt",
        "area_code": "838094",
        "path_local": "data_asia_tenggara/Thailand/fire_nrt_SV-C2_838094.csv",
        "path_hdfs": "/karhutla/raw/asean/thailand/fire_nrt_SV-C2_838094.csv",
        "expected_bytes": 119010,
        "expected_rows": 1416,
        "expected_columns": 14,
        "expected_has_type": False,
        "expected_date_min": "2026-08-01",
        "expected_date_max": "2026-11-01",
        "version_raw": "2.0NRT",
        "is_baseline": False,
        "notes": "NRT tanpa kolom type; data preliminer; berakhir 2026-11-01.",
    },
]


def inspect_dataset(entry: dict) -> dict:
    path = resolve_path(entry["path_local"])
    checks = {}

    if not path.exists():
        return {
            **entry,
            "dataset_id": f"{entry['country_code']}-{entry['feed']}-{entry['area_code']}",
            "path_local": entry["path_local"],
            "exists": False,
            "status": "MISSING",
            "checks": {"exists": False},
        }

    size = path.stat().st_size
    digest = sha256_file(path)

    rows = 0
    bad_field_count = 0
    date_min = None
    date_max = None
    header = []
    has_type = False

    with csv_stream(path) as (header, reader):
        has_type = "type" in header
        try:
            date_idx = header.index("acq_date")
        except ValueError:
            date_idx = None

        for row in reader:
            rows += 1
            if len(row) != len(header):
                bad_field_count += 1
            if date_idx is not None and date_idx < len(row):
                value = row[date_idx]
                if date_min is None or value < date_min:
                    date_min = value
                if date_max is None or value > date_max:
                    date_max = value

    checks = {
        "exists": True,
        "bytes": size == entry["expected_bytes"],
        "rows": rows == entry["expected_rows"],
        "columns": len(header) == entry["expected_columns"],
        "has_type": has_type == entry["expected_has_type"],
        "date_min": date_min == entry["expected_date_min"],
        "date_max": date_max == entry["expected_date_max"],
        "field_count": bad_field_count == 0,
    }
    status = "VERIFIED" if all(checks.values()) else "MISMATCH"

    return {
        "dataset_id": f"{entry['country_code']}-{entry['feed']}-{entry['area_code']}",
        "country_code": entry["country_code"],
        "country_name": entry["country_name"],
        "feed": entry["feed"],
        "area_code": entry["area_code"],
        "path_local": entry["path_local"],
        "path_hdfs": entry["path_hdfs"],
        "bytes": size,
        "mib": round(size / (1024 * 1024), 2),
        "rows": rows,
        "sha256": digest,
        "n_columns": len(header),
        "has_type": has_type,
        "date_min": date_min,
        "date_max": date_max,
        "version_raw": entry["version_raw"],
        "sensor": "SNPP",
        "instrument": "SNPP",
        "is_baseline": entry["is_baseline"],
        "notes": entry["notes"],
        "status": status,
        "checks": checks,
        "bad_field_count": bad_field_count,
    }


def main() -> int:
    ensure_dir(OUTPUT_DIR)
    started = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    datasets = []
    lines = [f"MANIFEST BUILD — {started}", "=" * 72]

    for entry in SEED:
        result = inspect_dataset(entry)
        datasets.append(result)

        if result.get("status") == "MISSING":
            lines.append(f"[MISSING] {result['dataset_id']} -> {result['path_local']}")
            continue

        lines.append(
            f"[{result['status']}] {result['dataset_id']} | rows={format_int(result['rows'])} "
            f"| cols={result['n_columns']} | type={result['has_type']} "
            f"| {result['date_min']}..{result['date_max']} | {result['mib']} MiB"
        )
        failed = [name for name, ok in result["checks"].items() if not ok]
        if failed:
            lines.append(f"         gagal: {', '.join(failed)}")

    manifest = {
        "manifest_version": 1,
        "project": "Analisis Pola Temporal dan Persebaran Hotspot Kebakaran Hutan dan Lahan di Asia Tenggara",
        "generated_at": started,
        "scope": ["Indonesia", "Malaysia", "Filipina", "Myanmar", "Thailand"],
        "excluded": [
            "DL_FIRE_SV-C2_837806/fire_archive_SV-C2_837806.csv (duplikat/subset Indonesia 2020-2025, tidak dipakai)"
        ],
        "dataset_count": len(datasets),
        "total_rows": sum(d["rows"] for d in datasets if d.get("rows")),
        "total_bytes": sum(d["bytes"] for d in datasets if d.get("bytes")),
        "datasets": datasets,
    }

    write_json(MANIFEST_PATH, manifest)

    summary_path = OUTPUT_DIR / "manifest_summary.csv"
    with open(summary_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset_id",
                "country_code",
                "country_name",
                "feed",
                "area_code",
                "rows",
                "bytes",
                "mib",
                "n_columns",
                "has_type",
                "date_min",
                "date_max",
                "version_raw",
                "sha256",
                "status",
            ]
        )
        for d in datasets:
            writer.writerow(
                [
                    d["dataset_id"],
                    d["country_code"],
                    d["country_name"],
                    d["feed"],
                    d["area_code"],
                    d.get("rows", ""),
                    d.get("bytes", ""),
                    d.get("mib", ""),
                    d.get("n_columns", ""),
                    d.get("has_type", ""),
                    d.get("date_min", ""),
                    d.get("date_max", ""),
                    d.get("version_raw", ""),
                    d.get("sha256", ""),
                    d.get("status", ""),
                ]
            )

    log_path = OUTPUT_DIR / "manifest_build_log.txt"
    with open(log_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")

    print("\n".join(lines))
    print("-" * 72)
    print(f"Total dataset : {len(datasets)}")
    print(f"Total baris   : {format_int(manifest['total_rows'])}")
    print(f"Total ukuran  : {manifest['total_bytes'] / (1024 * 1024):.2f} MiB")
    print(f"Manifest      : {MANIFEST_PATH}")
    print(f"Ringkasan     : {summary_path}")
    print(f"Log           : {log_path}")

    mismatches = [d for d in datasets if d.get("status") != "VERIFIED"]
    if mismatches:
        print(f"\nPERINGATAN: {len(mismatches)} dataset tidak VERIFIED.")
        return 1
    print("\nSEMUA DATASET VERIFIED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
