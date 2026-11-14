# Southeast Asia Forest & Land Fire Hotspot Analysis (2012–2026)

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.5.0%20HDFS-orange.svg)](https://hadoop.apache.org/)
[![Data Source](https://img.shields.io/badge/NASA%20FIRMS-VIIRS%20S--NPP%20375m-red.svg)](https://firms.modaps.eosdis.nasa.gov/)
[![Audit Status](https://img.shields.io/badge/Data%20Integrity-100%25%20PASS-brightgreen.svg)]()
[![License](https://img.shields.io/badge/License-MIT%2FAcademic-lightgrey.svg)]()

A comprehensive multi-country Big Data analysis examining **12,416,036 satellite active fire hotspot detections** across five Southeast Asian countries (**Indonesia, Malaysia, Myanmar, Philippines, and Thailand**) over nearly 15 years (January 20, 2012 – November 2, 2026).

This project integrates **Apache Hadoop HDFS** distributed storage with a custom **single-pass streaming Python analytics engine** to investigate temporal trends, seasonal regimes, regional spatial hotspots (0.1° resolution), and physical fire characteristics (Fire Radiative Power, confidence, and diurnal cycles).

---

## Key Highlights & Findings

- **Big Data Scale:** Analyzed **12,416,036 rows** (1,003,377,955 bytes / 956.90 MiB) across 10 official NASA FIRMS VIIRS S-NPP datasets (standard archive + Near Real-Time).
- **Storage & Reliability:** 100% HEALTHY HDFS cluster validation across 16 data blocks (128 MiB block size) with zero missing or corrupt blocks.
- **Ultra-Efficient Streaming Compute:** Processed the entire 12.4M-row corpus in **125.8 seconds** with a peak memory footprint of only **39.28 MiB RAM**, demonstrating that well-designed streaming algorithms outperform heavy distributed compute frameworks for sub-gigabyte structured datasets.
- **Strong Climate Forcing (ENSO):** Interannual variability is closely tied to the El Niño–Southern Oscillation. The **2015 Super El Niño** drove the historical annual peak (**1,486,854 detections**), whereas the **2022 Triple-Dip La Niña** marked the historical minimum (**364,173 detections**).
- **Dual Seasonal Regimes:**
  - *Northern / Mekong Basin (Myanmar & Thailand):* Sharp fire peak in **February–April** (peaking in March), driven by the dry northeast monsoon and post-harvest agricultural burning.
  - *Southern / Maritime Equatorial (Indonesia & Malaysia):* Peak in **August–October** (peaking in September), driven by dry southeast monsoon winds from Australia desiccating peatland domes.
- **Geographic Distribution:** Myanmar recorded the largest share of detections (**5,146,143; 41.5%**), followed by Indonesia (**4,140,587; 33.3%**), Thailand (**2,439,229; 19.6%**), Philippines (**462,349; 3.7%**), and Malaysia (**227,728; 1.8%**).
- **Multi-Source Thermal Discrimination:** The top 20 regional 0.1° spatial grids discriminate between:
  1. *Recurrent Peat Fires:* Central Kalimantan (ex-Mega Rice Project) and Sumatra.
  2. *Continuous Industrial Sources:* Nickel smelters in Morowali (#1 regional grid, 24,204 detections), Konawe, and Halmahera; LNG gas flaring in Bintulu, Sarawak (#7); cement plants in Saraburi, Thailand.
  3. *Volcanic Activity:* Mount Semeru, Mount Karangetang, and Anak Krakatau.

---

## Visualizations

### 1. Annual Hotspot Trends (2012–2026)
*Logarithmic scale comparison across five ASEAN countries. Shaded gray regions highlight partial observation years (2012 from Jan 20; 2026 up to Nov 2).*
![Annual Hotspot Trends](outputs/visualization/01_tren_tahunan.png)

### 2. Monthly Seasonality Patterns (2013–2025)
*Long-term monthly averages (excluding partial years 2012 and 2026) showing the clear divergence between the Mekong dry season (Feb–Apr) and the Maritime peatland dry season (Aug–Oct).*
![Monthly Seasonality](outputs/visualization/02_pola_bulanan.png)

### 3. Top 20 Spatial Hotspot Concentrations (0.1° Grid Cells)
*The 20 most concentrated 0.1° × 0.1° (~11 km) grid cells in Southeast Asia, labeled by country and coordinates.*
![Top 20 Grids](outputs/visualization/03_top20_grid.png)

### 4. Detection Characteristics
*Comparison of detection confidence levels (low, nominal, high), diurnal pass ratios (daytime vs. nighttime), and mean Fire Radiative Power (MW).*
![Detection Characteristics](outputs/visualization/04_karakteristik.png)

---

## Dataset Overview & Inventory

Data was acquired from the **NASA FIRMS** (Fire Information for Resource Management System) program using the **VIIRS** (Visible Infrared Imaging Radiometer Suite) instrument aboard the **Suomi-NPP** polar-orbiting satellite. VIIRS 375m imagery provides significantly improved fire perimeter delineation and detection of smaller, lower-intensity fires compared to legacy 1 km MODIS products.

| Dataset ID | Country | Feed | Rows | Size (Bytes) | Size (MiB) | Columns | Date Range | SHA-256 |
|---|---|---|---:|---:|---:|:---:|:---:|:---:|
| `ID-archive-837816` | Indonesia | Archive | 3,568,824 | 289,294,138 | 275.89 | 15 | 2012-01-20 – 2026-07-31 | Verified |
| `ID-nrt-837816` | Indonesia | NRT | 571,763 | 48,147,253 | 45.92 | 14 | 2026-08-01 – 2026-11-02 | Verified |
| `MY-archive-838165` | Malaysia | Archive | 224,485 | 18,028,691 | 17.19 | 15 | 2012-01-20 – 2026-07-31 | Verified |
| `MY-nrt-838165` | Malaysia | NRT | 3,243 | 270,322 | 0.26 | 14 | 2026-08-01 – 2026-11-02 | Verified |
| `MM-archive-838170` | Myanmar | Archive | 5,144,894 | 413,081,183 | 393.94 | 15 | 2012-01-20 – 2026-07-30 | Verified |
| `MM-nrt-838170` | Myanmar | NRT | 1,249 | 104,108 | 0.10 | 14 | 2026-08-02 – 2026-11-01 | Verified |
| `PH-archive-838168` | Philippines | Archive | 459,570 | 37,209,509 | 35.49 | 15 | 2012-01-20 – 2026-07-31 | Verified |
| `PH-nrt-838168` | Philippines | NRT | 2,779 | 232,787 | 0.22 | 14 | 2026-08-01 – 2026-11-02 | Verified |
| `TH-archive-838094` | Thailand | Archive | 2,437,813 | 196,890,954 | 187.77 | 15 | 2012-01-20 – 2026-07-31 | Verified |
| `TH-nrt-838094` | Thailand | NRT | 1,416 | 119,010 | 0.11 | 14 | 2026-08-01 – 2026-11-01 | Verified |
| **TOTAL** | **5 Countries** | **10 Files** | **12,416,036** | **1,003,377,955** | **956.90** | - | **2012-01-20 – 2026-11-02** | **PASS** |

> **Note on Redundant Data Exclusion:** File `DL_FIRE_SV-C2_837806/fire_archive_SV-C2_837806.csv` (1,708,204 rows; Indonesia 2020–2025) was identified as a subset duplicate of `ID-archive-837816` and explicitly excluded to prevent double counting.

---

## Physical Characteristics Summary

| Country | Detections | Total FRP (MW) | Mean FRP (MW) | Confidence (L / N / H) | Diurnal (Day / Night) | Top Grid Location |
|---|---:|---:|---:|:---:|:---:|---|
| **Indonesia (ID)** | 4,140,587 | 33,495,668.45 | 8.09 | 5.9% / 90.6% / 3.4% | 64.8% / 35.2% | Morowali / Luwu Timur (-2.6, 121.4) |
| **Malaysia (MY)** | 227,728 | 1,667,364.68 | 7.32 | 4.2% / 93.5% / 2.3% | 65.5% / 34.5% | Bintulu, Sarawak (3.3, 113.1) |
| **Myanmar (MM)** | 5,146,143 | 47,874,734.30 | 9.30 | 15.1% / 80.5% / 4.4% | 75.9% / 24.1% | Kayin State / Border (16.2, 98.8) |
| **Philippines (PH)**| 462,349 | 3,074,460.02 | 6.65 | 7.8% / 89.3% / 2.9% | 84.7% / 15.3% | Batangas / Laguna (14.0, 121.0) |
| **Thailand (TH)** | 2,439,229 | 13,004,722.55 | 5.33 | 11.8% / 85.9% / 2.3% | 65.6% / 34.4% | Saraburi (14.6, 101.1) |
| **TOTAL** | **12,416,036** | **99,116,950.00** | **7.98** | **10.9% / 85.5% / 3.6%** | **70.3% / 29.7%** | **31,529 Unique 0.1° Cells** |

---

## Big Data Architecture & Infrastructure

```
+-----------------------------------------------------------------------------------+
|                            APACHE HADOOP HDFS 3.5.0                               |
|       Distributed Storage: 1.003 GB Raw CSVs | 16 Blocks (128 MiB) | HEALTHY      |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|                      PYTHON 3 STANDALONE STREAMING ENGINE                         |
|   Single-Pass Generator | csv.reader | Memory: 39.3 MiB | Runtime: 125.8 Seconds  |
+-----------------------------------------------------------------------------------+
                                         |
         +-------------------------------+-------------------------------+
         |                               |                               |
         v                               v                               v
+------------------+           +-------------------+           +--------------------+
|  TEMPORAL METRICS|           |  SPATIAL PARTITION|           | PHYSICAL ATTRIBUTES|
|  Yearly Summary  |           |  0.1 Degree Grid  |           | FRP, Confidence,   |
|  Monthly Summary |           |  Top 20 Grids     |           | Day/Night, Type    |
+------------------+           +-------------------+           +--------------------+
         |                               |                               |
         +-------------------------------+-------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|                        AUDIT RECONCILIATION (100% PASS)                           |
|       Manifest: 12,416,036 == Validation: 12,416,036 == Analyzed: 12,416,036      |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|                        OUTPUTS & PUBLICATION CHARTS                               |
|        6 Structured CSV/JSON/TXT Reports | 4 Publication-Quality PNG Charts        |
+-----------------------------------------------------------------------------------+
```

### Storage Layer (Apache Hadoop HDFS)
- **Hadoop Version:** Apache Hadoop 3.5.0 on Ubuntu Linux (WSL2), OpenJDK 17.0.20 64-Bit.
- **Topology:** Single-Node Pseudo-Distributed (`fs.defaultFS = hdfs://localhost:9000`).
- **Block Layout:** 128 MiB block size (`dfs.blocksize = 134217728`), replication factor 1.
- **Verification:** `hdfs fsck /karhutla/raw -files -blocks` confirms:
  - Total Files: 10
  - Total Validated Blocks: 16 (average block size: 62.7 MB)
  - Missing / Corrupt / Under-Replicated Blocks: 0 (100% HEALTHY).
  - Storage Used: 964.5 MB of 1,006.85 GB configured capacity (0.10% utilization).

### Compute Layer (Python 3 Standalone)
- Implements an $O(N)$ single-pass streaming algorithm using pure Python standard library modules (`csv`, `json`, `math`, `collections`, `pathlib`).
- Reads each CSV line-by-line via streaming iterators, maintaining running aggregate hash tables with $O(G + Y + M + C)$ space complexity ($G = 31,529$ grid cells, $Y = 15$ years, $M = 179$ months, $C = 10$ partitions).
- Completely eliminates out-of-memory bottlenecks, requiring only **39.28 MiB RAM** to process ~1 GB of CSV records.

---

## Repository Structure

```
southeast-asia-forest-fire-analysis/
|-- config/
|   `-- datasets.json                  # Formal manifest of all 10 datasets with SHA-256 hashes
|-- data/
|   `-- sample/                        # Schema sample CSV files (Archive & NRT formats)
|       |-- fire_archive_sample.csv
|       `-- fire_nrt_sample.csv
|-- scripts/
|   |-- common.py                      # Common helper functions, paths, and CSV stream generators
|   |-- 00_build_manifest.py           # Manifest generator and SHA-256 builder
|   |-- 01_verify_baseline.py          # Baseline recomputation and verification
|   |-- 02_validate.py                 # Multi-country data integrity & gap validation
|   |-- 03_analyze.py                  # Core single-pass multi-country streaming analytics
|   `-- 04_visualize.py                # Visualization generator for the 4 publication charts
|-- outputs/
|   |-- analysis/                      # Aggregated analytical results
|   |   |-- yearly_summary.csv         # Annual detections per country and feed
|   |   |-- monthly_summary.csv        # Monthly detections per country and feed
|   |   |-- spatial_top20.csv          # Top 20 0.1-degree spatial grid hotspots
|   |   |-- characteristics.csv        # FRP, confidence, day/night, and type distributions
|   |   |-- analysis_summary.json      # Machine-readable summary with audit metadata
|   |   `-- analysis_report.txt        # Full text summary of analytical findings
|   |-- manifest/                      # Manifest verification logs and summaries
|   |-- validation/                    # Integrity reports, gaps, and missing value logs
|   `-- visualization/                 # 4 high-resolution publication charts
|       |-- 01_tren_tahunan.png
|       |-- 02_pola_bulanan.png
|       |-- 03_top20_grid.png
|       `-- 04_karakteristik.png
|-- laporan/                           # Academic reports, pseudocode, and HDFS evidence
|   |-- Laporan_Tugas_Big_Data.md      # Comprehensive academic report (Indonesian)
|   |-- dokumentasi/
|   |   `-- pseudocode_algoritma.md    # Detailed pseudocode and algorithmic complexity guide
|   |-- bukti_hdfs/                    # Authentic terminal logs (dfsadmin, fsck, jps, ls)
|   `-- screenshot/                    # Authentic terminal screenshot captures
|-- metadata_hdfs.py                   # HDFS metadata utility
|-- requirements.txt                   # Minimal Python dependencies
|-- .gitignore                         # Strict exclusion of raw datasets (~956.90 MiB)
`-- README.md                          # Project documentation (this file)
```

---

## Getting Started

### Prerequisites
- Python 3.10 or higher.
- `matplotlib` for chart generation.

```bash
git clone https://github.com/KopalZ/southeast-asia-forest-fire-analysis.git
cd southeast-asia-forest-fire-analysis
pip install -r requirements.txt
```

### Reproducing the Analysis Pipeline
Each script operates deterministically and outputs to `outputs/`:

```bash
# 1. Validate data integrity and schema consistency across all datasets
python scripts/02_validate.py

# 2. Run the single-pass multi-country streaming analysis
python scripts/03_analyze.py

# 3. Generate the 4 publication-quality visualization charts
python scripts/04_visualize.py
```

---

## Methodological Notes

1. **Hotspots vs. Fires:** Reported figures represent individual satellite thermal detection pixels, not discrete fire incidents. A single large wildfire can produce multiple detection points across successive orbits.
2. **Fire Radiative Power (FRP):** FRP (MW) measures the instantaneous rate of radiant heat output at the moment of satellite overpass. It is a physical proxy for fire intensity, **not burned area in hectares**.
3. **Confidence Scoring:** VIIRS confidence values are categorized as `'l'` (low), `'n'` (nominal), and `'h'` (high). Over 85% of detections in this study are nominal quality.
4. **Partial Years:** Data for 2012 begins on January 20, and 2026 data concludes in early November. Both years are flagged with `is_partial_year = true` and should not be compared directly to full-year totals without temporal normalization.
5. **Zero FRP Values:** 895 records (0.007% of the corpus) recorded $FRP = 0.0\text{ MW}$, reflecting valid sensor detection limit boundary values that were preserved in calculations.

---

## References and Data Sources

- Data Provider: NASA FIRMS (Fire Information for Resource Management System) VIIRS active fire data product.
- Sensor Information: Suomi National Polar-orbiting Partnership (S-NPP) 375 m VIIRS active fire detection.
- Distributed System: Apache Hadoop HDFS distributed file system architecture.

---

## License

This project and its analytical codebase are released under the [MIT License](LICENSE) for academic and open scientific research.
