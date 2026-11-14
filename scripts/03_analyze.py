"""Analisis multi-country hotspot NASA FIRMS (5 negara x archive/NRT).

Sumber dataset : config/datasets.json
Sumber validasi: outputs/validation/validation_summary.json (dipakai, tidak diulang)
Pemrosesan     : satu kali baca per CSV, streaming, satu file pada satu waktu.
Output         : outputs/analysis/ (6 file CSV/JSON/TXT).

Tidak mengubah raw data, manifest, baseline, hasil validasi, script lama, atau HDFS.
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
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

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "analysis"
VALIDATION_PATH = PROJECT_ROOT / "outputs" / "validation" / "validation_summary.json"

FEEDS = ("archive", "nrt")
PARTIAL_YEARS = {2012, 2026}

CONFIDENCE_LABELS = {
    "l": "confidence_low",
    "n": "confidence_nominal",
    "h": "confidence_high",
}
DAYNIGHT_LABELS = {"D": "daynight_D", "N": "daynight_N"}
TYPE_LABELS = {"0": "type_0", "1": "type_1", "2": "type_2", "3": "type_3"}

METHODOLOGY_NOTES = [
    "Jumlah yang dilaporkan adalah jumlah baris deteksi hotspot, bukan jumlah kejadian kebakaran unik yang terverifikasi.",
    "Archive dan NRT dihitung terpisah. Baris 'combined' adalah penjumlahan deteksi archive+NRT yang rentang tanggalnya tidak tumpang tindih, bukan kejadian unik.",
    "FRP adalah Fire Radiative Power, bukan luas area terbakar dan bukan ukuran kerugian.",
    "Confidence 'n' berarti nominal, bukan high. Hanya 'h' yang high.",
    "Arti kode type 0/1/2/3 belum diverifikasi dari dokumentasi FIRMS; hanya distribusinya yang dicatat.",
    "Grid spasial memakai pembulatan satu desimal round(lat,1)/round(lon,1), sama dengan baseline Indonesia.",
    "Setiap grid menyertakan country_code agar lokasi antarnegara tidak tercampur.",
    "Tahun 2012 dan 2026 adalah tahun parsial; total mentahnya tidak boleh dibandingkan dengan tahun penuh.",
    "Gap Myanmar 2026-07-31..2026-08-01 tidak diisi dengan asumsi apa pun.",
    "FRP nol tetap dihitung sebagai nilai valid non-negatif; jumlahnya dicatat terpisah.",
    "Tahun parsial ditandai, tetapi tren tahunan tetap disajikan apa adanya.",
]


def peak_memory_mib() -> float | None:
    """Peak working set proses (Windows). None jika tidak tersedia."""
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.windll.kernel32
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        handle = kernel32.GetCurrentProcess()
        function = getattr(kernel32, "K32GetProcessMemoryInfo", None)
        if function is None:
            function = ctypes.windll.psapi.GetProcessMemoryInfo
        function.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        function.restype = wintypes.BOOL
        ok = function(handle, ctypes.byref(counters), counters.cb)
        if not ok:
            return None
        return counters.PeakWorkingSetSize / (1024 * 1024)
    except Exception:
        return None


def load_validation() -> dict | None:
    if not VALIDATION_PATH.exists():
        return None
    with open(VALIDATION_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def new_stats() -> dict:
    return {
        "detections": 0,
        "confidence": defaultdict(int),
        "daynight": defaultdict(int),
        "type": defaultdict(int),
        "frp_sum": 0.0,
        "frp_zero": 0,
        "frp_negative": 0,
        "invalid_date": 0,
        "invalid_coordinate": 0,
        "invalid_frp": 0,
    }


def analyze_dataset(dataset: dict, yearly, monthly, grids, stats, feed_rows) -> int:
    country_code = dataset["country_code"]
    feed = dataset["feed"]
    feed_index = 0 if feed == "archive" else 1
    path = resolve_path(dataset["path_local"])

    grid = grids.setdefault(country_code, {})
    key = (country_code, feed)
    state = stats.get(key)
    if state is None:
        state = stats[key] = new_stats()

    rows = 0
    with csv_stream(path) as (header, reader):
        index = {name: position for position, name in enumerate(header)}
        n_columns = len(header)
        i_date = index["acq_date"]
        i_lat = index["latitude"]
        i_lon = index["longitude"]
        i_frp = index["frp"]
        i_confidence = index["confidence"]
        i_daynight = index["daynight"]
        i_type = index.get("type")

        for row in reader:
            if len(row) != n_columns:
                continue
            rows += 1

            raw_date = row[i_date]
            try:
                year = int(raw_date[:4])
                month_key = raw_date[:7]
                int(raw_date[5:7])
            except (ValueError, IndexError):
                state["invalid_date"] += 1
                continue

            try:
                lat = float(row[i_lat])
                lon = float(row[i_lon])
            except ValueError:
                state["invalid_coordinate"] += 1
                continue

            try:
                frp = float(row[i_frp])
            except ValueError:
                state["invalid_frp"] += 1
                continue
            if not math.isfinite(frp) or not (math.isfinite(lat) and math.isfinite(lon)):
                state["invalid_frp"] += 1
                continue

            yearly[(country_code, feed, year)] += 1
            monthly[(country_code, feed, month_key)] += 1

            cell_key = (round(lat, 1), round(lon, 1))
            cell = grid.get(cell_key)
            if cell is None:
                cell = grid[cell_key] = [0, 0]
            cell[feed_index] += 1

            state["detections"] += 1
            state["confidence"][row[i_confidence]] += 1
            state["daynight"][row[i_daynight]] += 1
            if i_type is not None:
                state["type"][row[i_type]] += 1
            if frp == 0.0:
                state["frp_zero"] += 1
            elif frp < 0:
                state["frp_negative"] += 1
            state["frp_sum"] += frp

    feed_rows[key] = rows
    return rows


def partial_year_note(year: int) -> str:
    if year == 2012:
        return "tahun parsial: data dimulai 2012-01-20"
    if year == 2026:
        return "tahun parsial: archive berakhir 2026-07-31, NRT 2026-11-02"
    return ""


def combine_stats(stats: dict, country_code: str) -> dict:
    combined = new_stats()
    for feed in FEEDS:
        state = stats.get((country_code, feed))
        if not state:
            continue
        combined["detections"] += state["detections"]
        combined["frp_sum"] += state["frp_sum"]
        combined["frp_zero"] += state["frp_zero"]
        combined["frp_negative"] += state["frp_negative"]
        for category, count in state["confidence"].items():
            combined["confidence"][category] += count
        for category, count in state["daynight"].items():
            combined["daynight"][category] += count
        for category, count in state["type"].items():
            combined["type"][category] += count
    return combined


def write_yearly(country_order, names, yearly, combined_yearly) -> None:
    path = OUTPUT_DIR / "yearly_summary.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["country_code", "country_name", "feed", "year", "detections", "is_partial_year", "note"]
        )
        for country_code in country_order:
            for feed in FEEDS:
                years = sorted(year for (cc, f, year) in yearly if cc == country_code and f == feed)
                for year in years:
                    writer.writerow(
                        [
                            country_code,
                            names[country_code],
                            feed,
                            year,
                            yearly[(country_code, feed, year)],
                            year in PARTIAL_YEARS,
                            partial_year_note(year),
                        ]
                    )
            for (cc, year), count in sorted(combined_yearly.items()):
                if cc != country_code:
                    continue
                note = partial_year_note(year)
                if note:
                    note += "; gabungan archive+NRT"
                else:
                    note = "gabungan archive+NRT, bukan kejadian unik"
                writer.writerow(
                    [country_code, names[country_code], "combined", year, count, year in PARTIAL_YEARS, note]
                )


def write_monthly(country_order, names, monthly, combined_monthly) -> None:
    path = OUTPUT_DIR / "monthly_summary.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "country_code",
                "country_name",
                "feed",
                "year",
                "month",
                "year_month",
                "detections",
                "is_partial_year",
            ]
        )
        for country_code in country_order:
            for feed in FEEDS:
                keys = sorted(key for (cc, f, key) in monthly if cc == country_code and f == feed)
                for month_key in keys:
                    year = int(month_key[:4])
                    month = int(month_key[5:7])
                    writer.writerow(
                        [
                            country_code,
                            names[country_code],
                            feed,
                            year,
                            month,
                            month_key,
                            monthly[(country_code, feed, month_key)],
                            year in PARTIAL_YEARS,
                        ]
                    )
            for (cc, month_key), count in sorted(combined_monthly.items()):
                if cc != country_code:
                    continue
                year = int(month_key[:4])
                month = int(month_key[5:7])
                writer.writerow(
                    [
                        country_code,
                        names[country_code],
                        "combined",
                        year,
                        month,
                        month_key,
                        count,
                        year in PARTIAL_YEARS,
                    ]
                )


def build_top_grids(grids, names, limit=20) -> list[dict]:
    flat = []
    for country_code, cells in grids.items():
        for (lat, lon), counts in cells.items():
            archive_count, nrt_count = counts
            flat.append(
                {
                    "country_code": country_code,
                    "country_name": names[country_code],
                    "grid_latitude": lat,
                    "grid_longitude": lon,
                    "detections_total": archive_count + nrt_count,
                    "detections_archive": archive_count,
                    "detections_nrt": nrt_count,
                }
            )
    flat.sort(
        key=lambda item: (
            -item["detections_total"],
            item["country_code"],
            item["grid_latitude"],
            item["grid_longitude"],
        )
    )
    for rank, item in enumerate(flat[:limit], start=1):
        item["rank"] = rank
    return flat[:limit]


def write_spatial(top_grids: list[dict]) -> None:
    path = OUTPUT_DIR / "spatial_top20.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "rank",
                "country_code",
                "country_name",
                "grid_latitude",
                "grid_longitude",
                "detections_total",
                "detections_archive",
                "detections_nrt",
            ]
        )
        for item in top_grids:
            writer.writerow(
                [
                    item["rank"],
                    item["country_code"],
                    item["country_name"],
                    f"{item['grid_latitude']:.1f}",
                    f"{item['grid_longitude']:.1f}",
                    item["detections_total"],
                    item["detections_archive"],
                    item["detections_nrt"],
                ]
            )


def write_characteristics(country_order, names, stats, combined) -> None:
    path = OUTPUT_DIR / "characteristics.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "country_code",
                "country_name",
                "scope",
                "detections",
                "confidence_low",
                "confidence_nominal",
                "confidence_high",
                "confidence_other",
                "daynight_D",
                "daynight_N",
                "daynight_other",
                "frp_sum",
                "frp_mean",
                "frp_zero_count",
                "frp_negative_count",
                "type_0",
                "type_1",
                "type_2",
                "type_3",
                "type_other",
                "type_not_available",
                "note",
            ]
        )
        for country_code in country_order:
            for scope in ("archive", "nrt", "combined"):
                if scope == "combined":
                    state = combined[country_code]
                    type_values = state["type"]
                    type_not_available = stats.get((country_code, "nrt"), {}).get("detections", 0)
                    note = "type hanya dari porsi archive; NRT tidak punya kolom type"
                else:
                    state = stats.get((country_code, scope))
                    if not state:
                        continue
                    type_values = state["type"]
                    type_not_available = 0 if scope == "archive" else state["detections"]
                    note = "kolom type tidak tersedia (NRT)" if scope == "nrt" else ""

                detections = state["detections"]
                confidence = state["confidence"]
                daynight = state["daynight"]
                known_confidence = sum(
                    confidence.get(category, 0) for category in CONFIDENCE_LABELS
                )
                known_daynight = sum(daynight.get(category, 0) for category in DAYNIGHT_LABELS)
                known_type = sum(type_values.get(category, 0) for category in TYPE_LABELS)

                writer.writerow(
                    [
                        country_code,
                        names[country_code],
                        scope,
                        detections,
                        confidence.get("l", 0),
                        confidence.get("n", 0),
                        confidence.get("h", 0),
                        detections - known_confidence,
                        daynight.get("D", 0),
                        daynight.get("N", 0),
                        detections - known_daynight,
                        f"{state['frp_sum']:.4f}",
                        f"{(state['frp_sum'] / detections) if detections else 0.0:.4f}",
                        state["frp_zero"],
                        state["frp_negative"],
                        type_values.get("0", 0) if scope != "nrt" else "",
                        type_values.get("1", 0) if scope != "nrt" else "",
                        type_values.get("2", 0) if scope != "nrt" else "",
                        type_values.get("3", 0) if scope != "nrt" else "",
                        (sum(type_values.values()) - known_type) if scope != "nrt" else "",
                        type_not_available,
                        note,
                    ]
                )


def country_top_grids(grids, country_code, limit=3) -> list[tuple[float, float, int, int, int]]:
    cells = grids.get(country_code, {})
    flat = [
        (lat, lon, counts[0] + counts[1], counts[0], counts[1])
        for (lat, lon), counts in cells.items()
    ]
    flat.sort(key=lambda item: (-item[2], item[0], item[1]))
    return flat[:limit]


def main() -> int:
    started = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    clock = time.perf_counter()

    manifest = load_manifest()
    datasets = manifest.get("datasets", [])
    validation = load_validation()

    ensure_dir(OUTPUT_DIR)

    country_order: list[str] = []
    names: dict[str, str] = {}
    dataset_by_key: dict[tuple[str, str], dict] = {}
    for dataset in datasets:
        country_code = dataset["country_code"]
        if country_code not in names:
            country_order.append(country_code)
            names[country_code] = dataset["country_name"]
        dataset_by_key[(country_code, dataset["feed"])] = dataset

    yearly = defaultdict(int)
    monthly = defaultdict(int)
    grids: dict[str, dict] = {}
    stats: dict[tuple[str, str], dict] = {}
    feed_rows: dict[tuple[str, str], int] = {}

    print(f"Analisis {len(datasets)} dataset (satu kali baca per file)...")
    for position, dataset in enumerate(datasets, start=1):
        rows = analyze_dataset(dataset, yearly, monthly, grids, stats, feed_rows)
        print(
            f"[{position}/{len(datasets)}] {dataset['dataset_id']} "
            f"-> {format_int(rows)} baris"
        )

    combined_yearly = defaultdict(int)
    for (country_code, feed, year), count in yearly.items():
        combined_yearly[(country_code, year)] += count

    combined_monthly = defaultdict(int)
    for (country_code, feed, month_key), count in monthly.items():
        combined_monthly[(country_code, month_key)] += count

    combined = {country_code: combine_stats(stats, country_code) for country_code in country_order}

    write_yearly(country_order, names, yearly, combined_yearly)
    write_monthly(country_order, names, monthly, combined_monthly)
    top_grids = build_top_grids(grids, names, limit=20)
    write_spatial(top_grids)
    write_characteristics(country_order, names, stats, combined)

    rows_manifest = sum(dataset.get("rows", 0) for dataset in datasets)
    rows_validation = None
    validation_status = None
    if validation:
        rows_validation = sum(item.get("rows_actual", 0) for item in validation.get("datasets", []))
        validation_status = validation.get("overall_status")
    rows_analyzed = sum(feed_rows.values())

    mismatches = []
    for dataset in datasets:
        key = (dataset["country_code"], dataset["feed"])
        if feed_rows.get(key) != dataset.get("rows"):
            mismatches.append(
                f"{dataset['dataset_id']}: manifest={dataset.get('rows')} analisis={feed_rows.get(key)}"
            )

    consistency = (
        rows_analyzed == rows_manifest
        and (rows_validation is None or rows_analyzed == rows_validation)
        and not mismatches
    )

    country_payload = []
    for country_code in country_order:
        archive = stats.get((country_code, "archive"), new_stats())
        nrt = stats.get((country_code, "nrt"), new_stats())
        total = combined[country_code]
        date_archive = dataset_by_key.get((country_code, "archive"), {})
        date_nrt = dataset_by_key.get((country_code, "nrt"), {})
        top = country_top_grids(grids, country_code, limit=3)
        country_payload.append(
            {
                "country_code": country_code,
                "country_name": names[country_code],
                "rows_archive": archive["detections"],
                "rows_nrt": nrt["detections"],
                "rows_total": total["detections"],
                "date_archive": f"{date_archive.get('date_min')}..{date_archive.get('date_max')}",
                "date_nrt": f"{date_nrt.get('date_min')}..{date_nrt.get('date_max')}",
                "frp_sum_archive": round(archive["frp_sum"], 4),
                "frp_sum_nrt": round(nrt["frp_sum"], 4),
                "frp_sum_total": round(total["frp_sum"], 4),
                "frp_mean_total": round(
                    total["frp_sum"] / total["detections"] if total["detections"] else 0.0, 4
                ),
                "frp_zero_total": total["frp_zero"],
                "confidence_total": {
                    "low": total["confidence"].get("l", 0),
                    "nominal": total["confidence"].get("n", 0),
                    "high": total["confidence"].get("h", 0),
                },
                "daynight_total": {
                    "D": total["daynight"].get("D", 0),
                    "N": total["daynight"].get("N", 0),
                },
                "type_archive": {
                    category: archive["type"].get(category, 0) for category in TYPE_LABELS
                },
                "grid_cells": len(grids.get(country_code, {})),
                "top_grids": [
                    {
                        "grid_latitude": f"{lat:.1f}",
                        "grid_longitude": f"{lon:.1f}",
                        "detections_total": count,
                        "detections_archive": archive_count,
                        "detections_nrt": nrt_count,
                    }
                    for lat, lon, count, archive_count, nrt_count in top
                ],
            }
        )

    yearly_totals = []
    for year in sorted({year for (_, year) in combined_yearly}):
        yearly_totals.append(
            {
                "year": year,
                "detections": sum(
                    count for (country_code, y), count in combined_yearly.items() if y == year
                ),
                "is_partial_year": year in PARTIAL_YEARS,
            }
        )

    runtime = time.perf_counter() - clock
    memory = peak_memory_mib()

    summary = {
        "generated_at": started,
        "runtime_seconds": round(runtime, 3),
        "peak_memory_mib": round(memory, 2) if memory is not None else None,
        "manifest": {
            "path": "config/datasets.json",
            "dataset_count": len(datasets),
            "rows": rows_manifest,
        },
        "validation": {
            "path": "outputs/validation/validation_summary.json",
            "available": validation is not None,
            "overall_status": validation_status,
            "rows": rows_validation,
        },
        "consistency": {
            "rows_manifest": rows_manifest,
            "rows_validation": rows_validation,
            "rows_analyzed": rows_analyzed,
            "status": "PASS" if consistency else "FAIL",
            "mismatches": mismatches,
        },
        "totals": {
            "detections": rows_analyzed,
            "archive": sum(feed_rows.get((cc, "archive"), 0) for cc in country_order),
            "nrt": sum(feed_rows.get((cc, "nrt"), 0) for cc in country_order),
            "frp_sum": round(sum(total["frp_sum"] for total in combined.values()), 4),
            "frp_zero": sum(total["frp_zero"] for total in combined.values()),
            "grid_cells": sum(len(cells) for cells in grids.values()),
        },
        "countries": country_payload,
        "yearly_totals": yearly_totals,
        "top_grids": top_grids,
        "methodology_notes": METHODOLOGY_NOTES,
    }
    write_json(OUTPUT_DIR / "analysis_summary.json", summary)

    lines = [
        "ANALISIS MULTI-COUNTRY HOTSPOT NASA FIRMS",
        "=" * 62,
        f"Waktu            : {started}",
        f"Runtime          : {runtime:.1f} detik",
        (
            f"Memori puncak    : {memory:.1f} MiB (PeakWorkingSetSize)"
            if memory is not None
            else "Memori puncak    : tidak tersedia"
        ),
        "Manifest         : config/datasets.json",
        "Validasi         : outputs/validation/validation_summary.json",
        "",
        "KONSISTENSI JUMLAH BARIS",
        f"  Manifest   : {format_int(rows_manifest)}",
        (
            f"  Validasi   : {format_int(rows_validation)}"
            if rows_validation is not None
            else "  Validasi   : tidak tersedia"
        ),
        f"  Dianalisis : {format_int(rows_analyzed)}",
        f"  Status     : {'PASS' if consistency else 'FAIL'}",
    ]
    if mismatches:
        for mismatch in mismatches:
            lines.append(f"    - {mismatch}")

    lines.extend(["", "RINGKASAN PER NEGARA"])
    for country in country_payload:
        total = country["rows_total"]
        lines.append("")
        lines.append(f"{country['country_name']} ({country['country_code']})")
        lines.append(
            f"  Baris     : archive {format_int(country['rows_archive'])} | "
            f"NRT {format_int(country['rows_nrt'])} | total {format_int(total)}"
        )
        lines.append(
            f"  Rentang   : archive {country['date_archive']} | NRT {country['date_nrt']}"
        )
        lines.append(
            f"  FRP       : total {country['frp_sum_total']:,.2f} | "
            f"rata-rata {country['frp_mean_total']:.2f} | nol {format_int(country['frp_zero_total'])}"
        )
        confidence = country["confidence_total"]
        daynight = country["daynight_total"]
        lines.append(
            f"  Confidence: l={format_int(confidence['low'])} | "
            f"n={format_int(confidence['nominal'])} | h={format_int(confidence['high'])}"
        )
        lines.append(
            f"  Day/night : D={format_int(daynight['D'])} | N={format_int(daynight['N'])}"
        )
        types = country["type_archive"]
        lines.append(
            "  Type (archive): "
            + " | ".join(f"{key}={format_int(value)}" for key, value in types.items())
        )
        lines.append(f"  Grid 0,1 derajat : {format_int(country['grid_cells'])} sel")
        for grid in country["top_grids"]:
            lines.append(
                f"    ({grid['grid_latitude']}, {grid['grid_longitude']}): "
                f"{format_int(grid['detections_total'])} "
                f"(archive {format_int(grid['detections_archive'])}, "
                f"NRT {format_int(grid['detections_nrt'])})"
            )

    lines.extend(["", "TOP 20 GRID SPASIAL (SEMUA NEGARA)"])
    for item in top_grids:
        lines.append(
            f"  {item['rank']:>2}. {item['country_code']} "
            f"({item['grid_latitude']:.1f}, {item['grid_longitude']:.1f}) "
            f"= {format_int(item['detections_total'])} deteksi "
            f"(archive {format_int(item['detections_archive'])}, "
            f"NRT {format_int(item['detections_nrt'])})"
        )

    lines.extend(["", "TOTAL PER TAHUN (GABUNGAN NEGARA)"])
    for item in yearly_totals:
        flag = " [parsial]" if item["is_partial_year"] else ""
        lines.append(f"  {item['year']}: {format_int(item['detections'])}{flag}")

    lines.extend(["", "CATATAN METODOLOGI"])
    for note in METHODOLOGY_NOTES:
        lines.append(f"- {note}")

    lines.extend(
        [
            "",
            "FILE OUTPUT",
            "  yearly_summary.csv",
            "  monthly_summary.csv",
            "  spatial_top20.csv",
            "  characteristics.csv",
            "  analysis_summary.json",
            "  analysis_report.txt",
        ]
    )

    text = "\n".join(lines) + "\n"
    (OUTPUT_DIR / "analysis_report.txt").write_text(text, encoding="utf-8", newline="\n")

    print("")
    print(f"Runtime      : {runtime:.1f} detik")
    if memory is not None:
        print(f"Memori puncak: {memory:.1f} MiB")
    print(f"Total baris  : {format_int(rows_analyzed)}")
    print(f"Konsistensi  : {'PASS' if consistency else 'FAIL'}")
    print(f"Output       : {OUTPUT_DIR}")
    return 0 if consistency else 1


if __name__ == "__main__":
    sys.exit(main())
