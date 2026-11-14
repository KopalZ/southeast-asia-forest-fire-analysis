"""Helper bersama untuk pipeline MVP multi-country (standard library saja).

Tidak ada dependensi eksternal. Semua pembacaan CSV dilakukan secara streaming.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
BASELINE_DIR = PROJECT_ROOT / "baseline"
MANIFEST_PATH = CONFIG_DIR / "datasets.json"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def csv_stream(path: Path, encoding: str = "utf-8"):
    """Buka CSV lokal dan hasilkan (header, reader) tanpa memuat seluruh isi."""
    with open(path, "r", newline="", encoding=encoding) as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"CSV kosong: {path}")
        yield header, reader


@contextmanager
def hdfs_csv_stream(hdfs_path: str, encoding: str = "utf-8"):
    """Buka CSV di HDFS melalui `hdfs dfs -cat` dan hasilkan (header, reader)."""
    process = subprocess.Popen(
        ["hdfs", "dfs", "-cat", hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdout is None:
        process.kill()
        raise RuntimeError(f"Gagal membuka stdout HDFS: {hdfs_path}")

    text = io.TextIOWrapper(process.stdout, encoding=encoding, newline="")
    reader = csv.reader(text)
    try:
        header = next(reader)
    except StopIteration:
        process.kill()
        process.wait()
        raise ValueError(f"CSV kosong di HDFS: {hdfs_path}")

    try:
        yield header, reader
    finally:
        try:
            text.close()
        finally:
            code = process.wait()
        if code != 0:
            stderr = process.stderr.read().decode("utf-8", "replace") if process.stderr else ""
            raise RuntimeError(f"hdfs dfs -cat gagal ({code}) untuk {hdfs_path}: {stderr.strip()}")


def load_manifest(path: Path = MANIFEST_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def format_int(value: int) -> str:
    return f"{value:,}"


def iter_local_rows(path: Path, encoding: str = "utf-8") -> Iterator[tuple[list[str], list[str]]]:
    """Iterator sederhana untuk kebutuhan ad-hoc; utamakan csv_stream untuk file besar."""
    with csv_stream(path, encoding=encoding) as (header, reader):
        for row in reader:
            yield header, row
