#!/usr/bin/env python3
"""Build a browser-ready extract for two verified petroDB wells."""

import argparse
import json
import math
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath

import duckdb

HF_BASE = "https://huggingface.co/datasets/oskrgab/petrodb/resolve/main/"
DATASET_URL = "https://huggingface.co/datasets/oskrgab/petrodb"
REPOSITORY_URL = "https://github.com/oskrgab/petrodb"

VOLVE_WELLS_URL = HF_BASE + "volve/wells.parquet"
VOLVE_MONTHLY_URL = HF_BASE + "volve/monthly_production.parquet"
ARGENTINA_WELLS_URL = HF_BASE + "argentina/wells.parquet"
ARGENTINA_OPERATORS_URL = HF_BASE + "argentina/well_operator_history.parquet"
ARGENTINA_MONTHLY_BASE = HF_BASE + "argentina/monthly_production/"
ARGENTINA_MANIFEST_URL = ARGENTINA_MONTHLY_BASE + "_files.json"

VOLVE_ID = 5599
VOLVE_NAME = "15/9-F-12"
ARGENTINA_ID = 120814
ARGENTINA_SIGLA = "YPF.Ch.-2204"
MAX_OUTPUT_BYTES = 200_000


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def iso_date(value):
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def finite_number_or_null(value, field_name):
    if value is None:
        return None
    number = float(value)
    require(math.isfinite(number), f"Non-finite value in {field_name}")
    return number


def normalize_rows(raw_rows, expected_count, expected_start, expected_end, label):
    require(raw_rows, f"{label}: empty production selection")
    rows = [
        {
            "date": iso_date(raw[0]),
            "hours": finite_number_or_null(raw[1], f"{label}.hours"),
            "oil": finite_number_or_null(raw[2], f"{label}.oil"),
            "gas": finite_number_or_null(raw[3], f"{label}.gas"),
            "water": finite_number_or_null(raw[4], f"{label}.water"),
        }
        for raw in raw_rows
    ]
    dates = [row["date"] for row in rows]
    require(len(rows) == expected_count, f"{label}: expected {expected_count} rows, got {len(rows)}")
    require(dates == sorted(dates), f"{label}: dates are not ordered")
    require(len(set(dates)) == len(dates), f"{label}: duplicate monthly dates")
    require(dates[0] == expected_start, f"{label}: unexpected first date {dates[0]}")
    require(dates[-1] == expected_end, f"{label}: unexpected last date {dates[-1]}")
    return rows


def fetch_partition_urls():
    request = urllib.request.Request(
        ARGENTINA_MANIFEST_URL,
        headers={"User-Agent": "well-prod-extractor/1.0"},
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
    return [ARGENTINA_MONTHLY_BASE + path for path in sorted(manifest)]


def fetch_volve(connection):
    identity = connection.execute(
        """
        SELECT npd_wellbore_code, wellbore_name, npd_field_name
        FROM read_parquet(?)
        WHERE npd_wellbore_code = ?
        """,
        [VOLVE_WELLS_URL, VOLVE_ID],
    ).fetchall()
    require(len(identity) == 1, "Volve well identity selection was not unique")
    require(identity[0][1] == VOLVE_NAME, f"Unexpected Volve well name: {identity[0][1]!r}")

    raw_rows = connection.execute(
        """
        SELECT production.date, production.on_stream_hours,
               production.oil_volume_sm3, production.gas_volume_sm3,
               production.water_volume_sm3
        FROM read_parquet(?) AS production
        JOIN read_parquet(?) AS wells
          ON production.npd_wellbore_code = wells.npd_wellbore_code
        WHERE production.npd_wellbore_code = ? AND wells.wellbore_name = ?
        ORDER BY production.date
        """,
        [VOLVE_MONTHLY_URL, VOLVE_WELLS_URL, VOLVE_ID, VOLVE_NAME],
    ).fetchall()
    rows = normalize_rows(raw_rows, 104, "2008-02-01", "2016-09-01", "Volve")
    return {
        "kind": "volve",
        "title": "Volve",
        "location": "Norwegian North Sea",
        "row_count": len(rows),
        "date_range": {"start": rows[0]["date"], "end": rows[-1]["date"]},
        "identity": {
            "npd_wellbore_code": identity[0][0],
            "wellbore_name": identity[0][1],
            "field": identity[0][2],
        },
        "units": {"oil": "m3", "gas": "m3", "water": "m3", "hours": "h"},
        "rows": rows,
    }


def fetch_argentina(connection, partition_urls):
    identity = connection.execute(
        """
        SELECT idpozo, sigla, formprod, area, yacimiento, cuenca, provincia, empresa
        FROM read_parquet(?)
        WHERE idpozo = ?
        """,
        [ARGENTINA_WELLS_URL, ARGENTINA_ID],
    ).fetchall()
    require(len(identity) == 1, "Argentina well identity selection was not unique")
    master = identity[0]
    expected_identity = (
        ARGENTINA_ID,
        ARGENTINA_SIGLA,
        "GCHU",
        "GOLFO SAN JORGE",
        "Chubut",
    )
    require(
        (master[0], master[1], master[2], master[5], master[6]) == expected_identity,
        f"Unexpected Argentina well identity: {master[:7]!r}",
    )

    operator_rows = connection.execute(
        """
        SELECT empresa, valid_from, valid_to
        FROM read_parquet(?)
        WHERE idpozo = ?
        ORDER BY valid_from DESC, valid_to DESC, empresa DESC
        """,
        [ARGENTINA_OPERATORS_URL, ARGENTINA_ID],
    ).fetchall()
    require(operator_rows, "Argentina operator history is empty")
    latest_operator = operator_rows[0]

    raw_rows = connection.execute(
        """
        SELECT fecha, tef, prod_pet, prod_gas * 1000.0 AS gas_m3, prod_agua
        FROM read_parquet(?, hive_partitioning = true)
        WHERE idpozo = ?
        ORDER BY fecha
        """,
        [partition_urls, ARGENTINA_ID],
    ).fetchall()
    rows = normalize_rows(raw_rows, 240, "2006-01-01", "2025-12-01", "Argentina")
    return {
        "kind": "argentina",
        "title": "Chubut, Argentina",
        "location": "Golfo San Jorge Basin",
        "row_count": len(rows),
        "date_range": {"start": rows[0]["date"], "end": rows[-1]["date"]},
        "identity": {
            "idpozo": master[0],
            "sigla": master[1],
            "productive_formation": master[2],
            "area": master[3],
            "field": master[4],
            "basin": master[5],
            "province": master[6],
            "master_operator": master[7],
            "latest_reported_operator": latest_operator[0],
            "latest_operator_valid_from": iso_date(latest_operator[1]),
            "latest_operator_valid_to": iso_date(latest_operator[2]),
        },
        "units": {"oil": "m3", "gas": "m3", "water": "m3", "hours": "h"},
        "unit_conversion": "Argentina prod_gas is multiplied by 1000 to convert thousands of cubic metres to cubic metres.",
        "rows": rows,
    }


def oil_total(well):
    return sum(row["oil"] for row in well["rows"] if row["oil"] is not None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().with_name("data.js"),
        help="Output JavaScript path (default: data.js beside this script)",
    )
    args = parser.parse_args()

    partition_urls = fetch_partition_urls()
    connection = duckdb.connect()
    try:
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
        volve = fetch_volve(connection)
        argentina = fetch_argentina(connection, partition_urls)
    finally:
        connection.close()

    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "sources": {
            "dataset": DATASET_URL,
            "repository": REPOSITORY_URL,
            "volve_wells": VOLVE_WELLS_URL,
            "volve_monthly": VOLVE_MONTHLY_URL,
            "argentina_manifest": ARGENTINA_MANIFEST_URL,
            "argentina_partitions": partition_urls,
            "argentina_wells": ARGENTINA_WELLS_URL,
            "argentina_operator_history": ARGENTINA_OPERATORS_URL,
        },
        "wells": [volve, argentina],
    }
    json_payload = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    document = f"window.PETRO_DATA = Object.freeze({json_payload});\n"
    output_size = len(document.encode("utf-8"))
    require(output_size <= MAX_OUTPUT_BYTES, f"Output exceeds 200 KB: {output_size} bytes")
    args.output.write_text(document, encoding="utf-8")

    for well in (volve, argentina):
        print(
            f"{well['title']}: {well['row_count']} rows, "
            f"{well['date_range']['start']} to {well['date_range']['end']}, "
            f"numeric oil total {oil_total(well):,.2f} m3"
        )
    print(f"Wrote {args.output} ({output_size:,} bytes); Argentina gas = prod_gas * 1000")


if __name__ == "__main__":
    main()
