"""Visualisasi hasil analisis multi-country (maksimal 4 grafik).

Sumber data : outputs/analysis/*.csv (tidak membaca raw CSV)
Output      : outputs/visualization/*.png + ringkasan_visualisasi.txt

Tidak mengubah hasil analisis, manifest, validasi, baseline, raw data, atau HDFS.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from common import PROJECT_ROOT, ensure_dir, format_int

ANALYSIS_DIR = PROJECT_ROOT / "outputs" / "analysis"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "visualization"

SOURCE_TEXT = (
    "Sumber: NASA FIRMS SNPP VIIRS SV-C2 (archive + NRT), diolah pada outputs/analysis. "
    "Deteksi hotspot bukan kebakaran terkonfirmasi."
)

COLORS = {
    "ID": "#1f77b4",
    "MY": "#ff7f0e",
    "MM": "#2ca02c",
    "PH": "#d62728",
    "TH": "#9467bd",
}
MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]


def read_csv(name: str) -> list[dict]:
    with open(ANALYSIS_DIR / name, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def format_decimal(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def save(fig, filename: str) -> Path:
    path = OUTPUT_DIR / filename
    fig.savefig(path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def load_yearly() -> tuple[list[str], dict[str, str], dict[str, dict[int, int]]]:
    rows = read_csv("yearly_summary.csv")
    order: list[str] = []
    names: dict[str, str] = {}
    data: dict[str, dict[int, int]] = defaultdict(dict)
    for row in rows:
        if row["feed"] != "combined":
            continue
        code = row["country_code"]
        if code not in names:
            order.append(code)
            names[code] = row["country_name"]
        data[code][int(row["year"])] = int(row["detections"])
    return order, names, data


def chart_yearly(order, names, data) -> Path:
    fig, ax = plt.subplots(figsize=(11, 6.2))
    years = sorted({year for values in data.values() for year in values})

    for code in order:
        points = sorted(data[code].items())
        ax.plot(
            [point[0] for point in points],
            [point[1] for point in points],
            marker="o",
            linewidth=2,
            markersize=5,
            color=COLORS[code],
            label=names[code],
        )

    ax.set_yscale("log")
    ax.set_xticks(years)
    ax.set_xlim(years[0] - 0.6, years[-1] + 0.6)
    ax.set_xlabel("Tahun")
    ax.set_ylabel("Jumlah deteksi hotspot (skala logaritmik)")
    ax.set_title(
        "Tren Tahunan Deteksi Hotspot per Negara, 2012–2026\n"
        "(area abu-abu = tahun parsial 2012 dan 2026)",
        fontsize=13,
        pad=12,
    )
    ax.grid(True, which="both", alpha=0.25)

    for year in (2012, 2026):
        ax.axvspan(year - 0.45, year + 0.45, color="grey", alpha=0.16)

    handles, labels = ax.get_legend_handles_labels()
    handles.append(Patch(facecolor="grey", alpha=0.3))
    labels.append("Tahun parsial (2012, 2026)")
    ax.legend(handles, labels, title="Negara", loc="upper left",
              bbox_to_anchor=(1.01, 1.0), fontsize=9)

    fig.text(0.5, 0.01, SOURCE_TEXT, ha="center", fontsize=7.5, color="dimgray")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    return save(fig, "01_tren_tahunan.png")


def chart_monthly(order, names) -> Path:
    rows = read_csv("monthly_summary.csv")
    totals: dict[tuple[str, int], int] = defaultdict(int)
    years_seen: dict[tuple[str, int], set[int]] = defaultdict(set)
    for row in rows:
        if row["feed"] != "combined":
            continue
        year = int(row["year"])
        if year < 2013 or year > 2025:
            continue
        month = int(row["month"])
        key = (row["country_code"], month)
        totals[key] += int(row["detections"])
        years_seen[key].add(year)

    fig, ax = plt.subplots(figsize=(11, 6.2))
    months = list(range(1, 13))
    for code in order:
        means = []
        for month in months:
            count = len(years_seen[(code, month)])
            means.append(totals[(code, month)] / count if count else 0.0)
        ax.plot(months, means, marker="o", linewidth=2, markersize=5,
                color=COLORS[code], label=names[code])

    ax.set_xticks(months)
    ax.set_xticklabels(MONTH_LABELS)
    ax.set_xlabel("Bulan")
    ax.set_ylabel("Rata-rata deteksi hotspot per tahun")
    ax.set_title(
        "Pola Musiman Deteksi Hotspot per Negara, 2013–2025\n"
        "(tahun parsial 2012 dan 2026 dikecualikan)",
        fontsize=13,
        pad=12,
    )
    ax.grid(True, alpha=0.25)
    ax.legend(title="Negara", loc="upper left", fontsize=9)

    fig.text(0.5, 0.01, SOURCE_TEXT, ha="center", fontsize=7.5, color="dimgray")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    return save(fig, "02_pola_bulanan.png")


def chart_top_grid(order, names) -> Path:
    rows = read_csv("spatial_top20.csv")
    rows = sorted(rows, key=lambda row: int(row["rank"]))
    labels = [
        f"{row['country_code']} ({float(row['grid_latitude']):.1f}; "
        f"{float(row['grid_longitude']):.1f})"
        for row in rows
    ]
    values = [int(row["detections_total"]) for row in rows]
    colors = [COLORS.get(row["country_code"], "grey") for row in rows]

    fig, ax = plt.subplots(figsize=(11, 7.6))
    bars = ax.barh(labels, values, color=colors, edgecolor="black", linewidth=0.4)
    ax.invert_yaxis()
    ax.set_xlabel("Jumlah deteksi hotspot")
    ax.set_ylabel("Negara (lintang; bujur)")
    ax.set_title("Top 20 Sel Grid 0,1° Berdasarkan Jumlah Deteksi, 2012–2026", fontsize=13, pad=12)
    ax.grid(True, axis="x", alpha=0.25)
    ax.set_xlim(0, max(values) * 1.14)

    for bar, value in zip(bars, values):
        ax.text(value + max(values) * 0.01, bar.get_y() + bar.get_height() / 2,
                format_int(value), va="center", fontsize=8)

    present = [code for code in order if any(row["country_code"] == code for row in rows)]
    handles = [Patch(facecolor=COLORS[code], edgecolor="black", label=names[code]) for code in present]
    ax.legend(handles=handles, title="Negara", loc="lower right", fontsize=9)

    fig.text(0.5, 0.01, SOURCE_TEXT, ha="center", fontsize=7.5, color="dimgray")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    return save(fig, "03_top20_grid.png")


def chart_characteristics(order, names) -> Path:
    rows = {row["country_code"]: row for row in read_csv("characteristics.csv")
            if row["scope"] == "combined"}

    fig, axes = plt.subplots(1, 3, figsize=(15, 6.4))
    positions = list(range(len(order)))
    ylabels = [names[code] for code in order]

    # (a) confidence
    ax = axes[0]
    low = [int(rows[code]["confidence_low"]) for code in order]
    nominal = [int(rows[code]["confidence_nominal"]) for code in order]
    high = [int(rows[code]["confidence_high"]) for code in order]
    totals = [low[i] + nominal[i] + high[i] for i in range(len(order))]
    low_pct = [low[i] / totals[i] * 100 for i in range(len(order))]
    nominal_pct = [nominal[i] / totals[i] * 100 for i in range(len(order))]
    high_pct = [high[i] / totals[i] * 100 for i in range(len(order))]
    ax.barh(positions, low_pct, color="#8c8c8c", label="low (l)")
    ax.barh(positions, nominal_pct, left=low_pct, color="#f0c040", label="nominal (n)")
    ax.barh(positions, high_pct, left=[low_pct[i] + nominal_pct[i] for i in range(len(order))],
            color="#2ca02c", label="high (h)")
    ax.set_yticks(positions)
    ax.set_yticklabels(ylabels)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("Proporsi deteksi (%)")
    ax.set_title("(a) Confidence", fontsize=11)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3,
              fontsize=7.5, frameon=False)
    ax.grid(True, axis="x", alpha=0.25)

    # (b) day/night
    ax = axes[1]
    day = [int(rows[code]["daynight_D"]) for code in order]
    night = [int(rows[code]["daynight_N"]) for code in order]
    totals = [day[i] + night[i] for i in range(len(order))]
    day_pct = [day[i] / totals[i] * 100 for i in range(len(order))]
    night_pct = [night[i] / totals[i] * 100 for i in range(len(order))]
    ax.barh(positions, day_pct, color="#e6a020", label="Siang (D)")
    ax.barh(positions, night_pct, left=day_pct, color="#304a7a", label="Malam (N)")
    ax.set_yticks(positions)
    ax.set_yticklabels(ylabels)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("Proporsi deteksi (%)")
    ax.set_title("(b) Day / Night", fontsize=11)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=2,
              fontsize=7.5, frameon=False)
    ax.grid(True, axis="x", alpha=0.25)

    # (c) mean FRP
    ax = axes[2]
    frp = [float(rows[code]["frp_mean"]) for code in order]
    bars = ax.bar(positions, frp, color=[COLORS[code] for code in order],
                  edgecolor="black", linewidth=0.4)
    ax.set_xticks(positions)
    ax.set_xticklabels([code for code in order])
    ax.set_ylabel("Rata-rata FRP (MW)")
    ax.set_xlabel("Negara")
    ax.set_title("(c) Rata-rata FRP", fontsize=11)
    ax.set_ylim(0, max(frp) * 1.18)
    for bar, value in zip(bars, frp):
        ax.text(bar.get_x() + bar.get_width() / 2, value + max(frp) * 0.02,
                format_decimal(value), ha="center", va="bottom", fontsize=8)
    ax.grid(True, axis="y", alpha=0.25)

    fig.suptitle(
        "Karakteristik Deteksi Hotspot per Negara (Archive + NRT)\n"
        "FRP adalah daya radiasi api, bukan luas area terbakar",
        fontsize=13,
    )
    fig.text(0.5, 0.01, SOURCE_TEXT, ha="center", fontsize=7.5, color="dimgray")
    fig.tight_layout(rect=[0, 0.04, 1, 0.94])
    return save(fig, "04_karakteristik.png")


def write_summary(paths: list[Path]) -> Path:
    lines = [
        "RINGKASAN VISUALISASI",
        "=" * 60,
        "",
        "1. 01_tren_tahunan.png",
        "   Tujuan: menunjukkan perubahan jumlah deteksi hotspot per negara",
        "   dari 2012 sampai 2026. Sumbu Y memakai skala logaritmik agar",
        "   negara dengan volume kecil tetap terbaca. Tahun 2012 dan 2026",
        "   diberi latar abu-abu karena merupakan tahun parsial.",
        "",
        "2. 02_pola_bulanan.png",
        "   Tujuan: menunjukkan pola musiman rata-rata per bulan.",
        "   Hanya tahun lengkap 2013-2025 yang dipakai; 2012 dan 2026",
        "   dikecualikan agar tidak bias.",
        "",
        "3. 03_top20_grid.png",
        "   Tujuan: menunjukkan 20 sel grid 0,1 derajat dengan deteksi",
        "   terbanyak. Setiap batang diberi kode negara agar lokasi",
        "   antarnegara tidak tercampur.",
        "",
        "4. 04_karakteristik.png",
        "   Tujuan: membandingkan komposisi confidence, proporsi siang/malam,",
        "   dan rata-rata FRP per negara.",
        "",
        "CATATAN",
        "- Semua angka adalah jumlah baris deteksi hotspot, bukan jumlah",
        "  kebakaran terkonfirmasi.",
        "- FRP adalah Fire Radiative Power (MW), bukan luas area terbakar.",
        "- Confidence 'n' berarti nominal, bukan high.",
        "- Sumber: NASA FIRMS SNPP VIIRS SV-C2 archive + NRT,",
        "  diolah pada outputs/analysis.",
        "",
        "FILE GAMBAR",
    ]
    for path in paths:
        lines.append(f"- {path.name} ({path.stat().st_size / 1024:.1f} KB)")
    text = "\n".join(lines) + "\n"
    target = OUTPUT_DIR / "ringkasan_visualisasi.txt"
    target.write_text(text, encoding="utf-8", newline="\n")
    return target


def main() -> int:
    ensure_dir(OUTPUT_DIR)

    order, names, yearly = load_yearly()

    paths = [
        chart_yearly(order, names, yearly),
        chart_monthly(order, names),
        chart_top_grid(order, names),
        chart_characteristics(order, names),
    ]
    summary = write_summary(paths)

    print("Visualisasi selesai:")
    for path in paths:
        print(f"  {path.name:<28} {path.stat().st_size / 1024:>8.1f} KB")
    print(f"  {summary.name:<28} {summary.stat().st_size / 1024:>8.1f} KB")
    print(f"Output: {OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
