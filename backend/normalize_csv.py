from __future__ import annotations

import argparse
import csv
import os
import re
from typing import Optional, Tuple


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOADS_DIR_DEFAULT = os.path.join(BASE_DIR, "uploads")
DATA_DIR_DEFAULT = os.path.join(BASE_DIR, "data")


def _find_latest_csv(upload_dir: str) -> Optional[str]:
    try:
        files = [
            os.path.join(upload_dir, f)
            for f in os.listdir(upload_dir)
            if f.lower().endswith(".csv") and os.path.isfile(os.path.join(upload_dir, f))
        ]
    except FileNotFoundError:
        return None
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


_NUM_RE = re.compile(r"([0-9]*\.?[0-9]+)")
_NUM_SUFFIX_RE = re.compile(r"([0-9]*\.?[0-9]+)\s*([a-zA-Z])")


def parse_money_to_int(value: str) -> str:
    """Parse currency-like strings to integer string.

    Rules:
    - Remove $, commas, spaces, and lowercase the rest.
    - Accept optional suffix T (trillion), B (billion), M (million) – use the first letter after the number.
    - Ignore any characters after the first suffix.
    - If no suffix, use factor 1.
    - Return empty string if unparsable.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() in {"nan", "na", "n/a", "—", "-"}:
        return ""
    s = s.replace("$", "").replace(",", "").replace(" ", "").lower()
    if not s:
        return ""

    # Try to capture with suffix first
    m = _NUM_SUFFIX_RE.match(s)
    factor = 1
    if m:
        num_part, suffix = m.group(1), m.group(2).lower()
        if suffix == "t":
            factor = 10**12
        elif suffix == "b":
            factor = 10**9
        elif suffix == "m":
            factor = 10**6
    else:
        m = _NUM_RE.match(s)
        if not m:
            return ""
        num_part = m.group(1)

    try:
        val = float(num_part) * factor
        return str(int(val))
    except Exception:
        return ""


def parse_employees_to_int(value: str) -> str:
    if value is None:
        return ""
    s = str(value).strip().replace(",", "")
    if not s or not re.fullmatch(r"[-+]?\d+", s):
        # Not a clean integer; return empty
        return ""
    try:
        return str(int(s))
    except Exception:
        return ""


def normalize_csv(input_path: str, output_path: str) -> Tuple[int, int]:
    """Normalize the CSV, writing to output. Returns (rows_in, rows_out)."""
    fieldnames = None
    rows_in = 0
    rows_out = 0

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(input_path, "r", newline="", encoding="utf-8") as f_in, open(output_path, "w", newline="", encoding="utf-8") as f_out:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames or []
        # Ensure we keep the same header order
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rows_in += 1
            # Overwrite columns in place when present
            if "Total Funding" in row:
                row["Total Funding"] = parse_money_to_int(row.get("Total Funding", ""))
            if "ARR" in row:
                row["ARR"] = parse_money_to_int(row.get("ARR", ""))
            if "Valuation" in row:
                row["Valuation"] = parse_money_to_int(row.get("Valuation", ""))
            if "Employees" in row:
                row["Employees"] = parse_employees_to_int(row.get("Employees", ""))

            writer.writerow(row)
            rows_out += 1

    return rows_in, rows_out


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize SaaS CSV monetary and employee fields")
    parser.add_argument(
        "--input",
        "-i",
        help="Path to input CSV (defaults to latest CSV in uploads)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Path to output CSV (defaults to <input>_normalized.csv in same directory)",
    )
    parser.add_argument("--uploads", "-u", default=UPLOADS_DIR_DEFAULT, help=f"Uploads directory to search (default: {UPLOADS_DIR_DEFAULT})")
    parser.add_argument("--data", "-d", default=DATA_DIR_DEFAULT, help=f"Output data directory (default: {DATA_DIR_DEFAULT})")
    args = parser.parse_args()

    input_path = args.input
    if not input_path:
        input_path = _find_latest_csv(args.uploads)
        if not input_path:
            raise SystemExit(f"No CSV found in uploads directory: {args.uploads}")
    if not os.path.isfile(input_path):
        raise SystemExit(f"Input CSV not found: {input_path}")

    output_path = args.output
    if not output_path:
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(args.data, base_name + "_normalized.csv")

    rows_in, rows_out = normalize_csv(input_path, output_path)
    print(f"Normalized CSV written to: {output_path} ({rows_out}/{rows_in} rows)")


if __name__ == "__main__":
    main()
