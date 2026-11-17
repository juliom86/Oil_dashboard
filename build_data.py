#!/usr/bin/env python3
"""Build the filtered parquet extracts for the Chubut well dashboard.

Reads directly from the petroDB Hugging Face dataset (no full download needed;
DuckDB fetches only the required row groups over HTTP Range), filters to the
Chubut province wells, sorts by well for fast client-side predicate pushdown,
and writes compact ZSTD parquet files under data/.

Run with:  uv run python build_data.py
"""

import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import duckdb

HF_BASE = "https://huggingface.co/datasets/oskrgab/petrodb/resolve/main/"
DATASET_URL = "https://huggingface.co/datasets/oskrgab/petrodb"
REPOSITORY_URL = "https://github.com/oskrgab/petrodb"

ARGENTINA_WELLS_URL = HF_BASE + "argentina/wells.parquet"
ARGENTINA_MANIFEST_URL = HF_BASE + "argentina/monthly_production/_files.json"

# Monthly production columns we keep. `prod_gas` is in *thousands* of m³
# (the petroDB convention); the dashboard multiplies by 1000 to show m³.
MONTHLY_COLUMNS = (
    "idpozo",
    "fecha",
    "prod_pet",
    "prod_gas",
    "prod_agua",
    "iny_agua",
    "iny_gas",
    "iny_co2",
    "iny_otro",
    "tef",
    "vida_util",
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def fetch_partition_urls():
    request = urllib.request.Request(
        ARGENTINA_MANIFEST_URL, headers={"User-Agent": "chubut-dashboard/1.0"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        manifest = json.load(response)
    require(isinstance(manifest, list) and manifest, "Argentina manifest is empty or invalid")
    for relative_path in manifest:
        path = PurePosixPath(relative_path) if isinstance(relative_path, str) else None
        require(
            path is not None
            and not path.is_absolute()
            and ".." not in path.parts
            and relative_path.endswith(".parquet"),
            f"Unsafe or invalid manifest path: {relative_path!r}",
        )
    return [HF_BASE + "argentina/monthly_production/" + path for path in sorted(manifest)]


def copy(connection, sql, out_path, params=None):
    """Run a COPY ... TO '<path>' query and report the resulting file size."""
    connection.execute(sql, params or [])
    size = out_path.stat().st_size
    print(f"  -> {out_path.name}: {size / (1024 * 1024):.1f} MB")
    return size


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path(__file__).resolve().with_name("data"),
        help="Output directory (default: data/ beside this script)",
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    started = time.time()
    connection = duckdb.connect()
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute("SET threads = 8")

    partition_urls = fetch_partition_urls()
    print(f"Fetched {len(partition_urls)} monthly partitions from the manifest")

    wells_path = args.output / "chubut_wells.parquet"
    monthly_path = args.output / "chubut_monthly.parquet"

    # 1. Chubut well master (drop the heavy geometry blob; keep coordinates).
    print("Building Chubut well master...")
    copy(
        connection,
        f"""
        COPY (
            SELECT * EXCLUDE (geom)
            FROM read_parquet('{ARGENTINA_WELLS_URL}')
            WHERE provincia = 'Chubut'
            ORDER BY area, yacimiento, sigla, idpozo
        ) TO '{wells_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        wells_path,
    )

    # 2. Chubut monthly production, sorted by well for fast per-well reads.
    print("Building Chubut monthly production (this is the big one)...")
    monthly_select = ", ".join(MONTHLY_COLUMNS)
    copy(
        connection,
        f"""
        COPY (
            SELECT {monthly_select}
            FROM read_parquet(?, hive_partitioning = true)
            WHERE idpozo IN (
                SELECT idpozo FROM read_parquet('{ARGENTINA_WELLS_URL}')
                WHERE provincia = 'Chubut'
            )
            ORDER BY idpozo, fecha
        ) TO '{monthly_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        monthly_path,
        [partition_urls],
    )

    # 3. Verification: report row counts.
    chubut_wells = connection.execute(
        f"SELECT count(*) FROM read_parquet('{wells_path}')"
    ).fetchone()[0]
    chubut_monthly = connection.execute(
        f"SELECT count(*) FROM read_parquet('{monthly_path}')"
    ).fetchone()[0]

    manifest = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "dataset": DATASET_URL,
        "repository": REPOSITORY_URL,
        "files": {
            "chubut_wells.parquet": chubut_wells,
            "chubut_monthly.parquet": chubut_monthly,
        },
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(
        f"\nDone in {time.time() - started:.0f}s. "
        f"Chubut wells: {chubut_wells:,}; monthly rows: {chubut_monthly:,}."
    )


if __name__ == "__main__":
    main()
