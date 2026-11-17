#!/usr/bin/env python3
"""Export the complete YPF.Ch well-master inventory to a UTF-8 text file."""

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import duckdb

WELLS_URL = "https://huggingface.co/datasets/oskrgab/petrodb/resolve/main/argentina/wells.parquet"
DATASET_URL = "https://huggingface.co/datasets/oskrgab/petrodb"
PREFIX = "YPF.Ch."
EXACT_PREFIX = "YPF.Ch"

COLUMNS = (
    "idpozo",
    "sigla",
    "formprod",
    "area",
    "yacimiento",
    "cuenca",
    "provincia",
    "tipo_recurso",
    "sub_tipo_recurso",
    "clasificacion",
    "has_production",
)


def text(value):
    """Return a single, safe TSV cell without altering the source value."""
    if value is None:
        return "Not reported"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def tsv(headers, rows):
    """Render a tab-separated section with no source-dependent delimiters."""
    return "\n".join(
        ["\t".join(map(text, headers)), *("\t".join(map(text, row)) for row in rows)]
    )


def count_section(title, label, records, index):
    counts = Counter(text(row[index]) for row in records)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [title, tsv((label, "idpozo records"), ordered), ""]


def fetch_inventory(connection):
    rows = connection.execute(
        f"""
        SELECT {", ".join(COLUMNS)}
        FROM read_parquet(?)
        WHERE sigla = ? OR starts_with(sigla, ?)
        ORDER BY upper(area), upper(yacimiento), upper(sigla),
                 upper(formprod), idpozo
        """,
        [WELLS_URL, EXACT_PREFIX, PREFIX],
    ).fetchall()
    if not rows:
        raise RuntimeError("The YPF.Ch inventory is empty; no output was written")
    identifiers = [row[0] for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise RuntimeError("Duplicate idpozo values in the well master")
    if any(row[6] != "Chubut" for row in rows):
        raise RuntimeError("A YPF.Ch match outside Chubut requires explicit review")
    chubut_total = connection.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE upper(provincia) = 'CHUBUT'",
        [WELLS_URL],
    ).fetchone()[0]
    return rows, chubut_total


def render_inventory(rows, chubut_total):
    unique_siglas = len({row[1] for row in rows})
    duplicate_siglas = sum(count > 1 for count in Counter(row[1] for row in rows).values())
    with_production = sum(row[-1] is True for row in rows)
    without_production = sum(row[-1] is False for row in rows)
    unknown_production = len(rows) - with_production - without_production
    lines = [
        "YPF.Ch WELL INVENTORY — CHUBUT, ARGENTINA",
        "=" * 45,
        "",
        "Source: PetroDB / Argentina well master",
        f"Dataset: {DATASET_URL}",
        f"Generated (UTC): {datetime.now(timezone.utc).replace(microsecond=0).isoformat()}",
        "",
        "Scope: exact sigla 'YPF.Ch' or a sigla starting with 'YPF.Ch.'.",
        "Every match is in Chubut. The total for all Chubut wells is context only;",
        "the full list below contains only the requested YPF.Ch prefix.",
        "YPF.Ch is a historical well-code prefix, not a current operatorship filter.",
        "idpozo is the canonical identity: wellbore x producing formation. Therefore,",
        "a repeated sigla can identify separate records for different formations.",
        "",
        "SUMMARY",
        "-------",
        f"Matching idpozo records: {len(rows):,}",
        f"Unique sigla values: {unique_siglas:,}",
        f"Sigla values shared by multiple idpozo records: {duplicate_siglas:,}",
        f"Matching records with has_production = Yes: {with_production:,}",
        f"Matching records with has_production = No: {without_production:,}",
        f"Matching records with has_production not reported: {unknown_production:,}",
        f"All Chubut idpozo records (not restricted to YPF.Ch): {chubut_total:,}",
        "",
    ]
    lines += count_section("COUNT BY AREA", "area", rows, COLUMNS.index("area"))
    lines += count_section(
        "COUNT BY PRODUCING FORMATION", "formprod", rows, COLUMNS.index("formprod")
    )
    lines += [
        "FULL YPF.Ch WELL LIST",
        "---------------------",
        "Tab-separated values (UTF-8). 'Not reported' means the source field is null.",
        "has_production is the supplied master flag; it is not present-day activity.",
        tsv(COLUMNS, rows),
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().with_name("pozos_ypf_ch.txt"),
        help="Output text path (default: pozos_ypf_ch.txt beside this script)",
    )
    args = parser.parse_args()
    connection = duckdb.connect()
    try:
        connection.execute("LOAD httpfs")
        rows, chubut_total = fetch_inventory(connection)
    finally:
        connection.close()
    document = render_inventory(rows, chubut_total)
    args.output.write_text(document, encoding="utf-8")
    print(f"Wrote {len(rows):,} YPF.Ch records to {args.output}")
    print(f"Unique siglas: {len({row[1] for row in rows}):,}; all Chubut: {chubut_total:,}")


if __name__ == "__main__":
    main()
