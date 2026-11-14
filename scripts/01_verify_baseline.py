"""Bekukan output Indonesia lama dan verifikasi reproduksi baseline.

Langkah:
  1. Salin output lama ke baseline/indonesia_v1/ (tidak mengubah output asli).
  2. Hitung ulang metrik baseline dari dua CSV mentah Indonesia dengan metode lama.
  3. Bandingkan angka lama vs baru, tulis laporan PASS/FAIL.

Output:
  baseline/indonesia_v1/            (salinan beku + SHA256SUMS.txt + baseline_meta.json)
  outputs/baseline/recomputed/      (hasil hitung ulang)
  outputs/baseline/baseline_report.txt
  outputs/baseline/baseline_report.json
  outputs/baseline/baseline_verify_log.txt
"""

from __future__ import annotations

import csv
import re
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from math import inf
from pathlib import Path

from common import (
    BASELINE_DIR,
    PROJECT_ROOT,
    csv_stream,
    ensure_dir,
    format_int,
    resolve_path,
    sha256_file,
    write_json,
)

OLD_DIR = PROJECT_ROOT / "DL_FIRE_SV-C2_837816" / "hasil_analisis"
RAW_FILES = [
    PROJECT_ROOT / "DL_FIRE_SV-C2_837816" / "fire_archive_SV-C2_837816.csv",
    PROJECT_ROOT / "DL_FIRE_SV-C2_837816" / "fire_nrt_SV-C2_837816.csv",
]
BASELINE_TARGET = BASELINE_DIR / "indonesia_v1"
RECOMP_DIR = PROJECT_ROOT / "outputs" / "baseline" / "recomputed"
REPORT_DIR = PROJECT_ROOT / "outputs" / "baseline"
REQUIRED_BASELINE_FILES = [
    "ringkasan.txt",
    "hotspot_tahunan.csv",
    "hotspot_bulanan.csv",
    "hotspot_spasial.csv",
    "hotspot_karakteristik.csv",
]


def freeze_baseline() -> tuple[str, list[dict]]:
    if not OLD_DIR.exists():
        raise FileNotFoundError(f"Output lama tidak ditemukan: {OLD_DIR}")

    if BASELINE_TARGET.exists():
        status = "already_exists"
        missing = [name for name in REQUIRED_BASELINE_FILES if not (BASELINE_TARGET / name).exists()]
        if missing:
            raise RuntimeError(
                f"Baseline beku ada tetapi tidak lengkap; file hilang: {', '.join(missing)}"
            )
    else:
        shutil.copytree(OLD_DIR, BASELINE_TARGET)
        status = "created"

    files = []
    for path in sorted(BASELINE_TARGET.rglob("*")):
        if not path.is_file():
            continue
        if path.name in {"SHA256SUMS.txt", "baseline_meta.json"}:
            continue
        files.append(
            {
                "path": path.relative_to(BASELINE_TARGET).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    checksum_path = BASELINE_TARGET / "SHA256SUMS.txt"
    with open(checksum_path, "w", encoding="utf-8", newline="\n") as handle:
        for item in files:
            handle.write(f"{item['sha256']}  {item['path']}\n")

    return status, files


def read_dict_rows(path: Path) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def recompute_baseline() -> dict:
    yearly = defaultdict(int)
    monthly = defaultdict(int)
    grid = defaultdict(int)
    confidence = defaultdict(int)
    daynight = defaultdict(int)

    total_rows = 0
    total_frp = 0.0
    min_frp = inf
    max_frp = -inf
    min_lat = inf
    max_lat = -inf
    min_lon = inf
    max_lon = -inf
    date_start = None
    date_end = None

    for path in RAW_FILES:
        with csv_stream(path) as (header, reader):
            index = {name: position for position, name in enumerate(header)}
            for row in reader:
                total_rows += 1

                date_value = row[index["acq_date"]]
                parsed = datetime.strptime(date_value, "%Y-%m-%d").date()
                yearly[parsed.year] += 1
                monthly[parsed.strftime("%Y-%m")] += 1
                if date_start is None or parsed < date_start:
                    date_start = parsed
                if date_end is None or parsed > date_end:
                    date_end = parsed

                lat = float(row[index["latitude"]])
                lon = float(row[index["longitude"]])
                min_lat = min(min_lat, lat)
                max_lat = max(max_lat, lat)
                min_lon = min(min_lon, lon)
                max_lon = max(max_lon, lon)

                # Metode baseline lama: pembulatan satu desimal, bukan floor.
                grid[(round(lat, 1), round(lon, 1))] += 1

                confidence[row[index["confidence"]]] += 1
                daynight[row[index["daynight"]]] += 1

                frp = float(row[index["frp"]])
                total_frp += frp
                min_frp = min(min_frp, frp)
                max_frp = max(max_frp, frp)

    return {
        "total_rows": total_rows,
        "total_frp": total_frp,
        "mean_frp": total_frp / total_rows if total_rows else 0.0,
        "min_frp": min_frp,
        "max_frp": max_frp,
        "min_lat": min_lat,
        "max_lat": max_lat,
        "min_lon": min_lon,
        "max_lon": max_lon,
        "date_start": date_start,
        "date_end": date_end,
        "yearly": dict(yearly),
        "monthly": dict(monthly),
        "grid": dict(grid),
        "confidence": dict(confidence),
        "daynight": dict(daynight),
    }


def write_recomputed(result: dict) -> None:
    ensure_dir(RECOMP_DIR)

    with open(RECOMP_DIR / "hotspot_tahunan.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["tahun", "jumlah_hotspot"])
        for year in sorted(result["yearly"]):
            writer.writerow([year, result["yearly"][year]])

    with open(RECOMP_DIR / "hotspot_bulanan.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["bulan", "jumlah_hotspot"])
        for month in sorted(result["monthly"]):
            writer.writerow([month, result["monthly"][month]])

    with open(RECOMP_DIR / "hotspot_spasial.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["grid_latitude", "grid_longitude", "jumlah_hotspot"])
        for (lat, lon), count in sorted(
            result["grid"].items(), key=lambda item: item[1], reverse=True
        ):
            writer.writerow([lat, lon, count])

    with open(RECOMP_DIR / "hotspot_karakteristik.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["jenis", "kategori", "jumlah"])
        for category in sorted(result["confidence"]):
            writer.writerow(["confidence", category, result["confidence"][category]])
        for category in sorted(result["daynight"]):
            writer.writerow(["daynight", category, result["daynight"][category]])

    lines = [
        "RINGKASAN ANALISIS HOTSPOT NASA FIRMS",
        "=" * 45,
        f"Total baris: {format_int(result['total_rows'])}",
        f"Tanggal awal: {result['date_start']}",
        f"Tanggal akhir: {result['date_end']}",
        f"Jumlah tahun: {len(result['yearly'])}",
        f"Jumlah bulan: {len(result['monthly'])}",
        f"Jumlah grid spasial: {len(result['grid'])}",
        f"Latitude minimum: {result['min_lat']}",
        f"Latitude maksimum: {result['max_lat']}",
        f"Longitude minimum: {result['min_lon']}",
        f"Longitude maksimum: {result['max_lon']}",
        f"Rata-rata FRP: {result['mean_frp']:.2f}",
        f"FRP minimum: {result['min_frp']}",
        f"FRP maksimum: {result['max_frp']}",
        "",
        "TAHUN DENGAN HOTSPOT TERBANYAK",
    ]
    for year, count in sorted(result["yearly"].items(), key=lambda item: item[1], reverse=True)[:5]:
        lines.append(f"{year}: {format_int(count)}")
    lines.extend(["", "BULAN DENGAN HOTSPOT TERBANYAK"])
    for month, count in sorted(result["monthly"].items(), key=lambda item: item[1], reverse=True)[:5]:
        lines.append(f"{month}: {format_int(count)}")

    with open(RECOMP_DIR / "ringkasan.txt", "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")


def grab(text: str, label: str) -> str | None:
    match = re.search(rf"^{re.escape(label)}:\s*(.*)$", text, re.MULTILINE)
    return match.group(1).strip() if match else None


def parse_top_section(text: str, header: str) -> list[tuple[str, int]]:
    lines = text.splitlines()
    try:
        start = lines.index(header) + 1
    except ValueError:
        return []
    output = []
    for line in lines[start:]:
        stripped = line.strip()
        if not stripped:
            break
        match = re.match(r"^([^:]+):\s*([\d,]+)$", stripped)
        if not match:
            break
        output.append((match.group(1).strip(), int(match.group(2).replace(",", ""))))
    return output


def compare_dict(name: str, expected: dict, actual: dict, checks: list[dict]) -> None:
    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    different = {
        key: (expected[key], actual[key])
        for key in set(expected) & set(actual)
        if expected[key] != actual[key]
    }
    ok = not missing and not extra and not different
    note = ""
    if not ok:
        note = f"hilang={len(missing)} ekstra={len(extra)} beda={len(different)}"
        examples = list(different.items())[:5]
        if examples:
            note += f" contoh={examples}"
    checks.append(
        {
            "name": name,
            "status": "PASS" if ok else "FAIL",
            "expected": f"{len(expected)} entri",
            "actual": f"{len(actual)} entri",
            "note": note,
        }
    )


def add_check(
    checks: list[dict], name: str, expected, actual, ok: bool | None = None, note: str = ""
) -> None:
    if ok is None:
        ok = expected == actual
    checks.append(
        {
            "name": name,
            "status": "PASS" if ok else "FAIL",
            "expected": expected,
            "actual": actual,
            "note": note,
        }
    )


def main() -> int:
    ensure_dir(REPORT_DIR)
    started = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    freeze_status, frozen_files = freeze_baseline()

    old_yearly = {
        int(row["tahun"]): int(row["jumlah_hotspot"])
        for row in read_dict_rows(OLD_DIR / "hotspot_tahunan.csv")
    }
    old_monthly = {
        row["bulan"]: int(row["jumlah_hotspot"])
        for row in read_dict_rows(OLD_DIR / "hotspot_bulanan.csv")
    }
    old_spatial = {
        (float(row["grid_latitude"]), float(row["grid_longitude"])): int(row["jumlah_hotspot"])
        for row in read_dict_rows(OLD_DIR / "hotspot_spasial.csv")
    }
    old_characteristics = {
        (row["jenis"], row["kategori"]): int(row["jumlah"])
        for row in read_dict_rows(OLD_DIR / "hotspot_karakteristik.csv")
    }
    old_summary = (OLD_DIR / "ringkasan.txt").read_text(encoding="utf-8")

    new = recompute_baseline()
    write_recomputed(new)

    checks: list[dict] = []

    old_total = int(grab(old_summary, "Total baris").replace(",", ""))
    add_check(checks, "total_baris", old_total, new["total_rows"])

    add_check(checks, "tanggal_awal", grab(old_summary, "Tanggal awal"), str(new["date_start"]))
    add_check(checks, "tanggal_akhir", grab(old_summary, "Tanggal akhir"), str(new["date_end"]))

    add_check(
        checks,
        "jumlah_tahun",
        int(grab(old_summary, "Jumlah tahun")),
        len(new["yearly"]),
    )
    add_check(
        checks,
        "jumlah_bulan",
        int(grab(old_summary, "Jumlah bulan")),
        len(new["monthly"]),
    )
    add_check(
        checks,
        "jumlah_grid_spasial",
        int(grab(old_summary, "Jumlah grid spasial")),
        len(new["grid"]),
    )

    add_check(
        checks,
        "latitude_minimum",
        grab(old_summary, "Latitude minimum"),
        f"{new['min_lat']}",
    )
    add_check(
        checks,
        "latitude_maksimum",
        grab(old_summary, "Latitude maksimum"),
        f"{new['max_lat']}",
    )
    add_check(
        checks,
        "longitude_minimum",
        grab(old_summary, "Longitude minimum"),
        f"{new['min_lon']}",
    )
    add_check(
        checks,
        "longitude_maksimum",
        grab(old_summary, "Longitude maksimum"),
        f"{new['max_lon']}",
    )
    add_check(
        checks,
        "rata_rata_frp",
        grab(old_summary, "Rata-rata FRP"),
        f"{new['mean_frp']:.2f}",
    )
    add_check(
        checks,
        "frp_minimum",
        grab(old_summary, "FRP minimum"),
        f"{new['min_frp']}",
    )
    add_check(
        checks,
        "frp_maksimum",
        grab(old_summary, "FRP maksimum"),
        f"{new['max_frp']}",
    )

    compare_dict("hotspot_tahunan", old_yearly, new["yearly"], checks)
    compare_dict("hotspot_bulanan", old_monthly, new["monthly"], checks)
    compare_dict("hotspot_spasial", old_spatial, new["grid"], checks)
    compare_dict("hotspot_karakteristik", old_characteristics, {
        **{("confidence", key): value for key, value in new["confidence"].items()},
        **{("daynight", key): value for key, value in new["daynight"].items()},
    }, checks)

    old_top_years = parse_top_section(old_summary, "TAHUN DENGAN HOTSPOT TERBANYAK")
    new_top_years = [
        (str(year), count)
        for year, count in sorted(
            new["yearly"].items(), key=lambda item: item[1], reverse=True
        )[:5]
    ]
    add_check(checks, "top5_tahun", old_top_years, new_top_years)

    old_top_months = parse_top_section(old_summary, "BULAN DENGAN HOTSPOT TERBANYAK")
    new_top_months = [
        (month, count)
        for month, count in sorted(
            new["monthly"].items(), key=lambda item: item[1], reverse=True
        )[:5]
    ]
    add_check(checks, "top5_bulan", old_top_months, new_top_months)

    passed = sum(1 for check in checks if check["status"] == "PASS")
    total = len(checks)
    overall = "PASS" if passed == total else "FAIL"

    report = {
        "generated_at": started,
        "baseline_source": str(OLD_DIR),
        "baseline_frozen": str(BASELINE_TARGET),
        "freeze_status": freeze_status,
        "frozen_file_count": len(frozen_files),
        "recomputed_dir": str(RECOMP_DIR),
        "overall": overall,
        "passed": passed,
        "total_checks": total,
        "checks": checks,
    }
    write_json(REPORT_DIR / "baseline_report.json", report)

    meta = {
        "baseline_name": "indonesia_v1",
        "source": str(OLD_DIR),
        "frozen_at": started,
        "freeze_status": freeze_status,
        "files": frozen_files,
        "expected_metrics": {
            "total_rows": old_total,
            "date_start": grab(old_summary, "Tanggal awal"),
            "date_end": grab(old_summary, "Tanggal akhir"),
            "yearly": old_yearly,
            "monthly": old_monthly,
            "characteristics": {
                f"{key[0]}:{key[1]}": value for key, value in old_characteristics.items()
            },
        },
    }
    write_json(BASELINE_TARGET / "baseline_meta.json", meta)

    lines = [
        "BASELINE INDONESIA v1 — VERIFIKASI REPRODUKSI",
        "=" * 56,
        f"Waktu              : {started}",
        f"Sumber output lama : {OLD_DIR}",
        f"Baseline beku      : {BASELINE_TARGET}",
        f"Status pembekuan   : {freeze_status}",
        f"Jumlah file beku   : {len(frozen_files)}",
        f"Hasil hitung ulang : {RECOMP_DIR}",
        "",
        "HASIL PERBANDINGAN",
    ]
    for check in checks:
        lines.append(
            f"[{check['status']}] {check['name']}: "
            f"expected={check['expected']} actual={check['actual']}"
            + (f" ({check['note']})" if check["note"] else "")
        )
    lines.extend(["", f"OVERALL: {overall} ({passed}/{total} checks)"])

    text = "\n".join(lines) + "\n"
    (REPORT_DIR / "baseline_report.txt").write_text(text, encoding="utf-8", newline="\n")
    (REPORT_DIR / "baseline_verify_log.txt").write_text(text, encoding="utf-8", newline="\n")

    print(text)
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
