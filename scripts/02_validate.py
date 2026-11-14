"""Validasi multi-country untuk 10 dataset (5 negara x archive/NRT).

Sumber daftar dataset: config/datasets.json
Pemrosesan: streaming, satu file pada satu waktu.
Output: outputs/validation/ (CSV, JSON, TXT).

Tidak mengubah dataset mentah, manifest, baseline, output lama, atau HDFS.
"""

from __future__ import annotations

import csv
import json
import math
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

from common import (
    PROJECT_ROOT,
    csv_stream,
    ensure_dir,
    format_int,
    load_manifest,
    resolve_path,
    write_json,
)

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "validation"

COMMON_COLUMNS = [
    "latitude",
    "longitude",
    "brightness",
    "scan",
    "track",
    "acq_date",
    "acq_time",
    "satellite",
    "instrument",
    "confidence",
    "version",
    "bright_t31",
    "frp",
    "daynight",
]
ARCHIVE_EXTRA_COLUMNS = ["type"]

CRITICAL_COLUMNS = [
    "latitude",
    "longitude",
    "acq_date",
    "acq_time",
    "confidence",
    "frp",
    "daynight",
]
NONCRITICAL_COLUMNS = [
    "brightness",
    "scan",
    "track",
    "satellite",
    "instrument",
    "version",
    "bright_t31",
    "type",
]

ALLOWED_CONFIDENCE = {"l", "n", "h"}
ALLOWED_DAYNIGHT = {"D", "N"}
ALLOWED_TYPE = {"0", "1", "2", "3"}

STATUS_RANK = {"NA": 0, "PASS": 0, "WARNING": 1, "FAIL": 2}


def add_check(
    checks: list[dict],
    name: str,
    status: str,
    expected,
    actual,
    note: str = "",
) -> None:
    checks.append(
        {
            "check": name,
            "status": status,
            "expected": expected,
            "actual": actual,
            "note": note,
        }
    )


def worst_status(checks: list[dict]) -> str:
    status = "PASS"
    for check in checks:
        if STATUS_RANK.get(check["status"], 0) > STATUS_RANK.get(status, 0):
            status = check["status"]
    return status


def header_rule(header: list[str], feed: str) -> tuple[str, str, str]:
    expected = COMMON_COLUMNS + (ARCHIVE_EXTRA_COLUMNS if feed == "archive" else [])
    expected_text = ",".join(expected)
    actual_text = ",".join(header)

    if header == expected:
        return "PASS", expected_text, actual_text, "urutan kolom sesuai"

    if set(header) == set(expected):
        return "WARNING", expected_text, actual_text, "set kolom sesuai, urutan berbeda"

    if feed == "nrt" and set(header) == set(expected) | {"type"}:
        return (
            "WARNING",
            expected_text,
            actual_text,
            "NRT memiliki kolom type tambahan; tidak wajib, tetapi perlu dicatat",
        )

    missing = [column for column in expected if column not in header]
    extra = [column for column in header if column not in expected]
    return (
        "FAIL",
        expected_text,
        actual_text,
        f"kolom hilang={missing}; kolom ekstra={extra}",
    )


def anomaly_status(count: int, total: int) -> str:
    if count == 0:
        return "PASS"
    if total and count >= total:
        return "FAIL"
    return "WARNING"


def missing_status(count: int, total: int) -> str:
    return anomaly_status(count, total)


def failure_result(dataset: dict, message: str) -> dict:
    checks = [
        {
            "check": "file_read",
            "status": "FAIL",
            "expected": "file dapat dibaca",
            "actual": message,
            "note": "",
        }
    ]
    return {
        "dataset_id": dataset.get("dataset_id", "UNKNOWN"),
        "country_code": dataset.get("country_code"),
        "country_name": dataset.get("country_name"),
        "feed": dataset.get("feed"),
        "area_code": dataset.get("area_code"),
        "path_local": dataset.get("path_local"),
        "path_hdfs": dataset.get("path_hdfs"),
        "status": "FAIL",
        "rows_manifest": dataset.get("rows"),
        "rows_actual": 0,
        "bytes_manifest": dataset.get("bytes"),
        "bytes_actual": None,
        "n_columns": None,
        "has_type": None,
        "date_min": None,
        "date_max": None,
        "distinct_dates": 0,
        "missing": {},
        "missing_total": 0,
        "missing_critical": 0,
        "missing_noncritical": 0,
        "invalid": {},
        "distributions": {},
        "version_values": [],
        "satellite_values": [],
        "instrument_values": [],
        "checks": checks,
        "notes": [message],
    }


def validate_dataset(dataset: dict) -> dict:
    dataset_id = dataset.get("dataset_id", "UNKNOWN")
    feed = dataset.get("feed")
    path = resolve_path(dataset["path_local"])

    checks: list[dict] = []

    if not path.exists():
        result = failure_result(dataset, f"file tidak ditemukan: {path}")
        return result

    bytes_actual = path.stat().st_size
    add_check(
        checks,
        "file_size",
        "PASS" if bytes_actual == dataset.get("bytes") else "FAIL",
        dataset.get("bytes"),
        bytes_actual,
        "ukuran file dibandingkan dengan manifest",
    )

    missing: Counter = Counter()
    confidence_counts: Counter = Counter()
    daynight_counts: Counter = Counter()
    type_counts: Counter = Counter()
    version_values: set[str] = set()
    satellite_values: set[str] = set()
    instrument_values: set[str] = set()

    total_rows = 0
    bad_field_count = 0
    invalid_date = 0
    date_out_of_range = 0
    invalid_coordinate = 0
    zero_coordinate = 0
    invalid_frp = 0
    frp_zero = 0
    invalid_confidence = 0
    invalid_daynight = 0
    invalid_type = 0
    invalid_acq_time = 0

    date_min: date | None = None
    date_max: date | None = None
    dates_seen: set[str] = set()

    try:
        with csv_stream(path) as (header, reader):
            header_status, header_expected, header_actual, header_note = header_rule(header, feed)
            add_check(checks, "header", header_status, header_expected, header_actual, header_note)

            n_columns = len(header)
            add_check(
                checks,
                "n_columns",
                "PASS" if n_columns == dataset.get("n_columns") else "FAIL",
                dataset.get("n_columns"),
                n_columns,
            )

            index = {name: position for position, name in enumerate(header)}
            has_type = "type" in index
            expected_columns = COMMON_COLUMNS + (ARCHIVE_EXTRA_COLUMNS if feed == "archive" else [])
            required_for_row_checks = ["acq_date", "latitude", "longitude", "frp", "confidence", "daynight"]
            can_validate_rows = all(column in index for column in required_for_row_checks)

            for row in reader:
                total_rows += 1
                if len(row) != n_columns:
                    bad_field_count += 1

                for column in expected_columns:
                    position = index.get(column)
                    value = row[position] if position is not None and position < len(row) else ""
                    if not value or value.isspace():
                        missing[column] += 1

                if not can_validate_rows:
                    continue

                raw_date = row[index["acq_date"]]
                try:
                    parsed_date = date.fromisoformat(raw_date)
                except ValueError:
                    invalid_date += 1
                else:
                    if date_min is None or parsed_date < date_min:
                        date_min = parsed_date
                    if date_max is None or parsed_date > date_max:
                        date_max = parsed_date
                    dates_seen.add(raw_date)
                    if parsed_date.year < 2012 or parsed_date.year > 2026:
                        date_out_of_range += 1

                try:
                    lat = float(row[index["latitude"]])
                    lon = float(row[index["longitude"]])
                except ValueError:
                    invalid_coordinate += 1
                else:
                    if not (math.isfinite(lat) and math.isfinite(lon)):
                        invalid_coordinate += 1
                    elif not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        invalid_coordinate += 1
                    elif lat == 0.0 and lon == 0.0:
                        zero_coordinate += 1

                try:
                    frp = float(row[index["frp"]])
                except ValueError:
                    invalid_frp += 1
                else:
                    if not math.isfinite(frp) or frp < 0:
                        invalid_frp += 1
                    elif frp == 0.0:
                        frp_zero += 1

                confidence = row[index["confidence"]]
                confidence_counts[confidence] += 1
                if confidence not in ALLOWED_CONFIDENCE:
                    invalid_confidence += 1

                daynight = row[index["daynight"]]
                daynight_counts[daynight] += 1
                if daynight not in ALLOWED_DAYNIGHT:
                    invalid_daynight += 1

                if has_type:
                    type_value = row[index["type"]]
                    type_counts[type_value] += 1
                    if type_value not in ALLOWED_TYPE:
                        invalid_type += 1

                if "acq_time" in index:
                    time_value = row[index["acq_time"]]
                    if not (
                        len(time_value) == 4
                        and time_value.isdigit()
                        and 0 <= int(time_value) <= 2359
                    ):
                        invalid_acq_time += 1

                if "version" in index:
                    version_values.add(row[index["version"]])
                if "satellite" in index:
                    satellite_values.add(row[index["satellite"]])
                if "instrument" in index:
                    instrument_values.add(row[index["instrument"]])

    except (OSError, UnicodeDecodeError, ValueError) as exc:
        return failure_result(dataset, f"gagal membaca {path}: {exc}")

    add_check(
        checks,
        "row_count",
        "PASS" if total_rows == dataset.get("rows") else "FAIL",
        dataset.get("rows"),
        total_rows,
        "jumlah baris dibandingkan dengan manifest",
    )
    add_check(
        checks,
        "field_count",
        "PASS" if bad_field_count == 0 else "FAIL",
        0,
        bad_field_count,
        "baris dengan jumlah field tidak sama dengan header",
    )

    missing_critical = sum(missing[column] for column in CRITICAL_COLUMNS)
    missing_noncritical = sum(missing[column] for column in NONCRITICAL_COLUMNS)
    add_check(
        checks,
        "missing_critical",
        missing_status(missing_critical, total_rows),
        0,
        missing_critical,
        "kolom kritis: " + ", ".join(CRITICAL_COLUMNS),
    )
    add_check(
        checks,
        "missing_noncritical",
        missing_status(missing_noncritical, total_rows),
        0,
        missing_noncritical,
        "kolom non-kritis: " + ", ".join(NONCRITICAL_COLUMNS),
    )

    add_check(
        checks,
        "invalid_date",
        anomaly_status(invalid_date, total_rows),
        0,
        invalid_date,
        "tanggal tidak dapat diparsing sebagai YYYY-MM-DD",
    )
    add_check(
        checks,
        "date_out_of_range",
        "PASS" if date_out_of_range == 0 else "WARNING",
        0,
        date_out_of_range,
        "tahun di luar rentang proyek 2012-2026",
    )

    manifest_range = f"{dataset.get('date_min')}..{dataset.get('date_max')}"
    actual_range = f"{date_min}..{date_max}" if date_min is not None else "tidak ada tanggal valid"
    add_check(
        checks,
        "date_range_manifest",
        "PASS" if actual_range == manifest_range else "WARNING",
        manifest_range,
        actual_range,
        "rentang tanggal aktual dibandingkan manifest",
    )

    add_check(
        checks,
        "invalid_coordinate",
        anomaly_status(invalid_coordinate, total_rows),
        0,
        invalid_coordinate,
        "koordinat non-numerik, non-finite, atau di luar -90..90 / -180..180",
    )
    add_check(
        checks,
        "coordinate_zero",
        "PASS" if zero_coordinate == 0 else "WARNING",
        0,
        zero_coordinate,
        "koordinat (0,0) mencurigakan; bukan aturan keras",
    )

    add_check(
        checks,
        "invalid_frp",
        anomaly_status(invalid_frp, total_rows),
        0,
        invalid_frp,
        "FRP non-numerik, non-finite, atau negatif",
    )
    add_check(
        checks,
        "frp_zero",
        "PASS" if frp_zero == 0 else "WARNING",
        0,
        frp_zero,
        "FRP bernilai 0; bukan pelanggaran sintaks, ambang minimum tidak diverifikasi",
    )

    add_check(
        checks,
        "invalid_confidence",
        anomaly_status(invalid_confidence, total_rows),
        0,
        invalid_confidence,
        "nilai di luar {l,n,h}",
    )
    add_check(
        checks,
        "invalid_daynight",
        anomaly_status(invalid_daynight, total_rows),
        0,
        invalid_daynight,
        "nilai di luar {D,N}",
    )

    if feed == "archive":
        type_presence_status = "PASS" if has_type else "FAIL"
        type_expected = "ada"
        type_actual = "ada" if has_type else "tidak ada"
    else:
        type_presence_status = "PASS" if not has_type else "WARNING"
        type_expected = "tidak ada (NRT)"
        type_actual = "tidak ada" if not has_type else "ada (tidak terduga)"
    add_check(
        checks,
        "type_presence",
        type_presence_status,
        type_expected,
        type_actual,
        "type wajib untuk archive; tidak wajib untuk NRT",
    )

    if has_type:
        add_check(
            checks,
            "invalid_type",
            anomaly_status(invalid_type, total_rows),
            0,
            invalid_type,
            "nilai di luar {0,1,2,3}",
        )
    else:
        add_check(
            checks,
            "invalid_type",
            "NA",
            "tidak tersedia",
            "tidak tersedia",
            "kolom type tidak ada (NRT); validasi type dilewati",
        )

    add_check(
        checks,
        "invalid_acq_time",
        "PASS" if invalid_acq_time == 0 else "WARNING",
        0,
        invalid_acq_time,
        "format HHMM 4 digit; zona waktu belum diverifikasi",
    )

    expected_version = dataset.get("version_raw")
    if version_values == {expected_version}:
        version_status = "PASS"
    else:
        version_status = "WARNING"
    add_check(
        checks,
        "version_values",
        version_status,
        expected_version,
        ",".join(sorted(version_values)) if version_values else "tidak ada",
        "versi produk di dalam file",
    )

    for check_name, values, expected_value in [
        ("satellite_values", satellite_values, "SNPP"),
        ("instrument_values", instrument_values, "SNPP"),
    ]:
        status = "PASS" if values == {expected_value} else "WARNING"
        add_check(
            checks,
            check_name,
            status,
            expected_value,
            ",".join(sorted(values)) if values else "tidak ada",
            "nilai sensor/instrumen di dalam file",
        )

    status = worst_status(checks)

    notes = []
    for check in checks:
        if check["status"] in {"WARNING", "FAIL"}:
            notes.append(
                f"{check['check']}: {check['actual']}"
                + (f" ({check['note']})" if check["note"] else "")
            )

    return {
        "dataset_id": dataset_id,
        "country_code": dataset.get("country_code"),
        "country_name": dataset.get("country_name"),
        "feed": feed,
        "area_code": dataset.get("area_code"),
        "path_local": dataset.get("path_local"),
        "path_hdfs": dataset.get("path_hdfs"),
        "status": status,
        "rows_manifest": dataset.get("rows"),
        "rows_actual": total_rows,
        "bytes_manifest": dataset.get("bytes"),
        "bytes_actual": bytes_actual,
        "n_columns": n_columns,
        "has_type": has_type,
        "date_min": str(date_min) if date_min is not None else None,
        "date_max": str(date_max) if date_max is not None else None,
        "distinct_dates": len(dates_seen),
        "missing": dict(missing),
        "missing_total": sum(missing.values()),
        "missing_critical": missing_critical,
        "missing_noncritical": missing_noncritical,
        "invalid": {
            "date": invalid_date,
            "date_out_of_range": date_out_of_range,
            "coordinate": invalid_coordinate,
            "coordinate_zero": zero_coordinate,
            "frp": invalid_frp,
            "frp_zero": frp_zero,
            "confidence": invalid_confidence,
            "daynight": invalid_daynight,
            "type": invalid_type,
            "acq_time": invalid_acq_time,
        },
        "distributions": {
            "confidence": dict(confidence_counts),
            "daynight": dict(daynight_counts),
            "type": dict(type_counts),
        },
        "version_values": sorted(version_values),
        "satellite_values": sorted(satellite_values),
        "instrument_values": sorted(instrument_values),
        "checks": checks,
        "notes": notes,
    }


def build_gaps(results: list[dict]) -> list[dict]:
    by_country: dict[str, dict] = {}
    for result in results:
        by_country.setdefault(result["country_code"], {})[result["feed"]] = result

    gaps = []
    for country_code in sorted(by_country):
        feeds = by_country[country_code]
        archive = feeds.get("archive", {})
        nrt = feeds.get("nrt", {})
        country_name = archive.get("country_name") or nrt.get("country_name") or country_code

        archive_min = archive.get("date_min")
        archive_max = archive.get("date_max")
        nrt_min = nrt.get("date_min")
        nrt_max = nrt.get("date_max")

        gap_days = None
        overlap_days = None
        status = "NOT VERIFIED"
        note = "rentang tanggal archive atau NRT tidak tersedia"

        if archive_min and archive_max and nrt_min and nrt_max:
            archive_min_date = date.fromisoformat(archive_min)
            archive_max_date = date.fromisoformat(archive_max)
            nrt_min_date = date.fromisoformat(nrt_min)

            if archive_max_date < nrt_min_date:
                gap_days = (nrt_min_date - archive_max_date).days - 1
                overlap_days = 0
                status = "PASS" if gap_days == 0 else "WARNING"
                note = (
                    "archive dan NRT bersambung tanpa gap"
                    if gap_days == 0
                    else f"ada {gap_days} hari tanpa data di antara archive dan NRT"
                )
            else:
                overlap_days = (archive_max_date - nrt_min_date).days + 1
                gap_days = 0
                status = "WARNING"
                note = f"rentang archive dan NRT tumpang tindih {overlap_days} hari"

        archive_partial_start = None
        archive_partial_end = None
        if archive_min:
            archive_partial_start = date.fromisoformat(archive_min).strftime("%m-%d") != "01-01"
        if archive_max:
            archive_partial_end = date.fromisoformat(archive_max).strftime("%m-%d") != "12-31"

        gaps.append(
            {
                "country_code": country_code,
                "country_name": country_name,
                "archive_date_min": archive_min,
                "archive_date_max": archive_max,
                "archive_distinct_dates": archive.get("distinct_dates"),
                "nrt_date_min": nrt_min,
                "nrt_date_max": nrt_max,
                "nrt_distinct_dates": nrt.get("distinct_dates"),
                "gap_days": gap_days,
                "overlap_days": overlap_days,
                "archive_partial_start": archive_partial_start,
                "archive_partial_end": archive_partial_end,
                "status": status,
                "note": note,
            }
        )
    return gaps


def write_outputs(results: list[dict], gaps: list[dict], generated_at: str) -> dict:
    ensure_dir(OUTPUT_DIR)

    counts = Counter(result["status"] for result in results)
    gap_counts = Counter(gap["status"] for gap in gaps)
    if counts.get("FAIL"):
        overall = "FAIL"
    elif counts.get("WARNING") or gap_counts.get("WARNING"):
        overall = "WARNING"
    else:
        overall = "PASS"

    findings = []
    for result in results:
        if result["status"] in {"WARNING", "FAIL"}:
            findings.append(
                f"{result['dataset_id']} ({result['status']}): "
                + ("; ".join(result["notes"][:4]) if result["notes"] else "lihat detail")
            )
    for gap in gaps:
        if gap["status"] in {"WARNING", "FAIL"}:
            findings.append(
                f"{gap['country_code']} archive/NRT ({gap['status']}): "
                f"gap={gap['gap_days']} hari, overlap={gap['overlap_days']} hari | {gap['note']}"
            )

    methodological_notes = [
        "Archive memiliki 15 kolom termasuk type; NRT memiliki 14 kolom tanpa type.",
        "Arti kode type (0/1/2/3) belum diverifikasi dari dokumentasi FIRMS pada tahap ini; validasi hanya memeriksa domain nilai.",
        "Zona waktu acq_time (UTC vs lokal) belum diverifikasi.",
        "Overlap yang dihitung adalah overlap rentang tanggal, bukan overlap spasial bounding box.",
        "FRP diperiksa numerik dan non-negatif; satuan dan ambang fisik tidak divalidasi.",
        "FRP bernilai 0 dicatat sebagai WARNING karena ambang minimum tidak diverifikasi, bukan dianggap error otomatis.",
        "brightness, scan, track, dan bright_t31 belum divalidasi rentangnya pada tahap ini.",
        "Tahun 2012 dan 2026 adalah periode parsial; jangan membandingkan total mentah dengan tahun penuh.",
        "Jumlah hari unik adalah hari yang memiliki deteksi hotspot; hari tanpa deteksi tidak otomatis berarti data hilang.",
    ]

    summary = {
        "generated_at": generated_at,
        "manifest": "config/datasets.json",
        "dataset_count": len(results),
        "overall_status": overall,
        "counts": {
            "PASS": counts.get("PASS", 0),
            "WARNING": counts.get("WARNING", 0),
            "FAIL": counts.get("FAIL", 0),
        },
        "gap_counts": {
            "PASS": gap_counts.get("PASS", 0),
            "WARNING": gap_counts.get("WARNING", 0),
            "FAIL": gap_counts.get("FAIL", 0),
        },
        "datasets": results,
        "gaps": gaps,
        "findings": findings,
        "methodological_notes": methodological_notes,
    }
    write_json(OUTPUT_DIR / "validation_summary.json", summary)

    with open(OUTPUT_DIR / "validation_summary.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset_id",
                "country_code",
                "country_name",
                "feed",
                "rows_manifest",
                "rows_actual",
                "bytes_manifest",
                "bytes_actual",
                "n_columns",
                "has_type",
                "date_min",
                "date_max",
                "distinct_dates",
                "missing_total",
                "missing_critical",
                "invalid_date",
                "invalid_coordinate",
                "invalid_frp",
                "frp_zero",
                "invalid_confidence",
                "invalid_daynight",
                "invalid_type",
                "invalid_acq_time",
                "status",
                "notes",
            ]
        )
        for result in results:
            invalid = result["invalid"]
            writer.writerow(
                [
                    result["dataset_id"],
                    result["country_code"],
                    result["country_name"],
                    result["feed"],
                    result["rows_manifest"],
                    result["rows_actual"],
                    result["bytes_manifest"],
                    result["bytes_actual"],
                    result["n_columns"],
                    result["has_type"],
                    result["date_min"],
                    result["date_max"],
                    result["distinct_dates"],
                    result["missing_total"],
                    result["missing_critical"],
                    invalid.get("date"),
                    invalid.get("coordinate"),
                    invalid.get("frp"),
                    invalid.get("frp_zero"),
                    invalid.get("confidence"),
                    invalid.get("daynight"),
                    invalid.get("type"),
                    invalid.get("acq_time"),
                    result["status"],
                    " | ".join(result["notes"]),
                ]
            )

    with open(OUTPUT_DIR / "validation_details.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dataset_id", "check", "status", "expected", "actual", "note"])
        for result in results:
            for check in result["checks"]:
                writer.writerow(
                    [
                        result["dataset_id"],
                        check["check"],
                        check["status"],
                        check["expected"],
                        check["actual"],
                        check["note"],
                    ]
                )

    with open(OUTPUT_DIR / "validation_missing.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dataset_id", "column", "missing_count", "missing_pct", "note"])
        for result in results:
            total = result["rows_actual"] or 0
            for column in sorted(result["missing"]):
                count = result["missing"][column]
                pct = (count / total * 100) if total else 0.0
                note = "kolom tidak ada" if result["n_columns"] is not None and column not in (
                    COMMON_COLUMNS + (ARCHIVE_EXTRA_COLUMNS if result["feed"] == "archive" else [])
                ) else ""
                writer.writerow([result["dataset_id"], column, count, f"{pct:.6f}", note])

    with open(OUTPUT_DIR / "validation_gaps.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "country_code",
                "country_name",
                "archive_date_min",
                "archive_date_max",
                "archive_distinct_dates",
                "nrt_date_min",
                "nrt_date_max",
                "nrt_distinct_dates",
                "gap_days",
                "overlap_days",
                "archive_partial_start",
                "archive_partial_end",
                "status",
                "note",
            ]
        )
        for gap in gaps:
            writer.writerow(
                [
                    gap["country_code"],
                    gap["country_name"],
                    gap["archive_date_min"],
                    gap["archive_date_max"],
                    gap["archive_distinct_dates"],
                    gap["nrt_date_min"],
                    gap["nrt_date_max"],
                    gap["nrt_distinct_dates"],
                    gap["gap_days"],
                    gap["overlap_days"],
                    gap["archive_partial_start"],
                    gap["archive_partial_end"],
                    gap["status"],
                    gap["note"],
                ]
            )

    lines = [
        "VALIDASI MULTI-COUNTRY — 10 DATASET",
        "=" * 60,
        f"Waktu     : {generated_at}",
        f"Manifest  : config/datasets.json",
        f"Overall   : {overall}",
        f"PASS      : {counts.get('PASS', 0)}",
        f"WARNING   : {counts.get('WARNING', 0)}",
        f"FAIL      : {counts.get('FAIL', 0)}",
        f"Gap PASS  : {gap_counts.get('PASS', 0)}",
        f"Gap WARN  : {gap_counts.get('WARNING', 0)}",
        f"Gap FAIL  : {gap_counts.get('FAIL', 0)}",
        "",
        "RINGKASAN PER DATASET",
    ]
    for result in results:
        lines.append(
            f"[{result['status']}] {result['dataset_id']} | "
            f"{format_int(result['rows_actual'])} baris | "
            f"{result['n_columns']} kolom | type={result['has_type']} | "
            f"{result['date_min']}..{result['date_max']} | "
            f"{result['distinct_dates']} hari unik"
        )
        if result["notes"]:
            for note in result["notes"][:5]:
                lines.append(f"    - {note}")

    lines.extend(["", "SCHEMA ARCHIVE vs NRT"])
    lines.append("- Archive: 15 kolom termasuk type.")
    lines.append("- NRT: 14 kolom tanpa type; validasi type dilewati (NA).")

    lines.extend(["", "GAP ARCHIVE–NRT PER NEGARA"])
    for gap in gaps:
        lines.append(
            f"[{gap['status']}] {gap['country_name']} ({gap['country_code']}): "
            f"archive {gap['archive_date_min']}..{gap['archive_date_max']}, "
            f"NRT {gap['nrt_date_min']}..{gap['nrt_date_max']}, "
            f"gap={gap['gap_days']} hari, overlap={gap['overlap_days']} hari | {gap['note']}"
        )

    lines.extend(["", "TEMUAN"])
    if findings:
        for finding in findings:
            lines.append(f"- {finding}")
    else:
        lines.append("- Tidak ada WARNING/FAIL pada dataset.")

    lines.extend(["", "CATATAN METODOLOGI"])
    for note in methodological_notes:
        lines.append(f"- {note}")

    text = "\n".join(lines) + "\n"
    (OUTPUT_DIR / "validation_report.txt").write_text(text, encoding="utf-8", newline="\n")

    return summary


def main() -> int:
    generated_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    manifest = load_manifest()
    datasets = manifest.get("datasets", [])

    if len(datasets) != 10:
        print(f"PERINGATAN: manifest berisi {len(datasets)} dataset, diharapkan 10.")

    results = []
    for position, dataset in enumerate(datasets, start=1):
        print(f"[{position}/{len(datasets)}] {dataset.get('dataset_id')} ...", end=" ", flush=True)
        result = validate_dataset(dataset)
        results.append(result)
        print(result["status"])

    gaps = build_gaps(results)
    summary = write_outputs(results, gaps, generated_at)

    print("\n" + "=" * 60)
    print(f"OVERALL: {summary['overall_status']}")
    print(
        f"PASS={summary['counts']['PASS']} "
        f"WARNING={summary['counts']['WARNING']} "
        f"FAIL={summary['counts']['FAIL']} | "
        f"gap WARNING={summary['gap_counts']['WARNING']}"
    )
    print(f"Output: {OUTPUT_DIR}")
    return 0 if summary["overall_status"] in {"PASS", "WARNING"} else 1


if __name__ == "__main__":
    sys.exit(main())
