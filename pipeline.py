"""
Day 2 - Multi-File Data Processing Pipeline
Squad Python Specialization - Data Engineering

Usage (suggested):  python pipeline.py --input data/ --output output/
"""
import argparse
import csv
import logging
from datetime import datetime
from pathlib import Path

#import pandas as pd

REQUIRED_COLUMNS: set[str] = {
    "transaction_id", "date", "branch", "customer_id",
    "product", "quantity", "unit_price", "payment_method",
}

# Decision (see README): KNOWN aliases are mapped to the canonical name; a file that is
# still missing required columns after mapping is rejected as a whole.
COLUMN_ALIASES: dict[str, str] = {
    "qty": "quantity",
}

# Columns converted from text to int during cleaning (a set: fast membership checks).
INT_COLUMNS: set[str] = {"quantity", "unit_price"}

# Reason prefix (set by find_problems / run_pipeline) -> label in the summary.
REASON_CATEGORIES: dict[str, str] = {
    "missing value": "missing values",
    "invalid number": "invalid numbers",
    "duplicate ID": "duplicate IDs",
    "column issue": "column issues",
}

# First columns of output/rejected_transactions.csv; the original columns follow.
REJECT_META_COLUMNS: list[str] = ["source_file", "row_number", "transaction_id", "reasons"]

# Column order of output/consolidated_transactions.csv (from docs/expected_output.md).
OUTPUT_COLUMNS: list[str] = [
    "transaction_id", "date", "branch", "customer_id", "product",
    "quantity", "unit_price", "line_total", "payment_method", "payment_ref", "source_file",
]

# FInding for files using pathlib
def discover_files(folder: Path, pattern: str = "transactions_*.csv") -> list[Path]:
    """Return the matching CSV files in a stable (date) order using pathlib."""
    folder = Path(folder)  # accept a string too, e.g. "data/"

    if not folder.exists():
        raise FileNotFoundError(f"Input folder not found: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Input path is not a folder: {folder}")

    files = sorted(p for p in folder.glob(pattern) if p.is_file())

    if not files:
        logging.warning("No files matching %r found in %s", pattern, folder)

    return files

# Reading one file using pathlib and csv.DictReader

def read_transactions(path: Path) -> tuple[list[str], list[dict]]:
    """Read one file and return (header, rows). Every row should remember its source file."""
    path = Path(path)  # accept a string too
    rows: list[dict] = []

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = list(reader.fieldnames or [])

        for line_no, row in enumerate(reader, start=2):  # line 1 is the header
            row["source_file"] = path.name
            row["row_number"] = line_no
            rows.append(row)

    logging.info("Read %d rows from %s", len(rows), path.name)
    return header, rows


def apply_aliases(header: list[str], rows: list[dict],
                  aliases: dict[str, str] = COLUMN_ALIASES) -> list[str]:
    """Rename known alias columns (e.g. qty -> quantity) in the header and every row.

    An alias is only applied when the canonical column is not already present,
    so real data is never overwritten. Returns the new header.
    """
    present = set(header)
    renames = {old: new for old, new in aliases.items()
               if old in present and new not in present}
    if not renames:
        return header

    for row in rows:
        for old, new in renames.items():
            row[new] = row.pop(old)

    source = rows[0]["source_file"] if rows else "file"
    for old, new in renames.items():
        logging.warning("%s: mapped column alias %r -> %r", source, old, new)

    return [renames.get(col, col) for col in header]


def validate_columns(header: list[str], required: set[str] = REQUIRED_COLUMNS) -> set[str]:
    """Return the set of required columns missing from the header (empty set means valid).

    Set difference: everything in `required` that is not in the header.
    """
    present = {col.strip() for col in header}
    return set(required) - present


def find_problems(row: dict, seen_ids: set[str]) -> list[str]:
    """Return a list of human-readable reasons this row is invalid (empty list means valid).

    Must cover: blank required values, non-numeric or non-positive quantity/price,
    and transaction IDs already seen in this run.

    Side effect: the row's transaction_id is added to seen_ids, so the FIRST
    occurrence of an ID is kept and every later copy is flagged as a duplicate.
    """
    problems: list[str] = []

    # 1. Blank required values (None covers a column missing from the row entirely)
    for col in sorted(REQUIRED_COLUMNS):
        value = row.get(col)
        if value is None or str(value).strip() == "":
            problems.append(f"missing value: {col}")

    # 2. Numbers must be whole and positive (blank ones were already reported above)
    for col in ("quantity", "unit_price"):
        value = (row.get(col) or "").strip()
        if not value:
            continue
        try:
            number = int(value)
        except ValueError:
            problems.append(f"invalid number: {col}={value!r} is not a whole number")
            continue
        if number <= 0:
            problems.append(f"invalid number: {col}={number} must be positive")

    # 3. Duplicate transaction IDs across the whole run
    txn_id = (row.get("transaction_id") or "").strip()
    if txn_id:
        if txn_id in seen_ids:
            problems.append(f"duplicate ID: {txn_id} already seen")
        else:
            seen_ids.add(txn_id)

    return problems


def clean_row(row: dict) -> dict:
    """Return a normalised copy of a valid row (types converted, whitespace stripped).

    Only call this on rows that passed find_problems(), so the int() conversions are safe.
    Extra input columns (e.g. cashier) are dropped; line_total is computed.
    """
    # Dict comprehension: keep only output columns and strip whitespace from every value.
    # payment_ref is optional, so a missing value becomes "" instead of None.
    cleaned = {
        col: str(row.get(col) or "").strip()
        for col in OUTPUT_COLUMNS
        if col != "line_total"
    }

    # Loop over the INT_COLUMNS set to convert numeric fields.
    for col in INT_COLUMNS:
        cleaned[col] = int(cleaned[col])

    cleaned["line_total"] = cleaned["quantity"] * cleaned["unit_price"]

    # Return the keys in output order so the CSV columns line up.
    return {col: cleaned[col] for col in OUTPUT_COLUMNS}


def write_csv(rows: list[dict], path: Path, fieldnames: list[str]) -> None:
    """Write rows to CSV, creating parent folders as needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        # extrasaction="ignore": any keys not in fieldnames are left out of the file.
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logging.info("Wrote %d rows to %s", len(rows), path)


def build_summary(stats: dict) -> str:
    """Format the processing summary shown in the expected-output resource."""
    rejected = stats["rejected"]

    # Count ROWS per category (a row with 2 missing values counts once under
    # "missing values"). A set of prefixes per row removes repeats within a row.
    row_categories = [
        {reason.split(":", 1)[0] for reason in r["reasons"].split("; ")}
        for r in rejected
    ]
    counts = {
        prefix: sum(prefix in cats for cats in row_categories)
        for prefix in REASON_CATEGORIES
    }

    issues = stats["column_issues"]
    issue_text = str(len(issues))
    if issues:
        details = ", ".join(f"{name}: missing {', '.join(sorted(cols))}"
                            for name, cols in issues.items())
        issue_text += f"  ({details})"

    lines = [
        "=== Processing summary ===",
        f"Files discovered:        {stats['files_discovered']}",
        f"Files processed:         {stats['files_processed']}",
        f"Files with column issues:{issue_text}",
        f"Rows read:               {stats['rows_read']}",
        f"Rows accepted:           {len(stats['accepted'])}",
        f"Rows rejected:           {len(rejected)}",
        f"  - missing values:      {counts['missing value']}",
        f"  - invalid numbers:     {counts['invalid number']}",
        f"  - duplicate IDs:       {counts['duplicate ID']}",
    ]
    if counts["column issue"]:   # only shown when a whole file was rejected
        lines.append(f"  - column issues:       {counts['column issue']}")
    if sum(counts.values()) != len(rejected):
        lines.append("    (rows with problems in several categories are counted in each)")
    lines += [
        f"Total revenue (KES):     {stats['total_revenue']:,}",
        f"Run finished at:         {stats['finished_at']}",
    ]
    return "\n".join(lines)


def make_reject(row: dict, reasons: list[str]) -> dict:
    """Build one rejected_transactions.csv record: tracking fields first, then the
    original values exactly as read (not cleaned), so the row can be fixed and re-run."""
    record = {
        "source_file": row["source_file"],
        "row_number": row["row_number"],
        "transaction_id": (row.get("transaction_id") or "").strip(),
        "reasons": "; ".join(reasons),
    }
    record.update({col: val for col, val in row.items() if col not in record})
    return record


def run_pipeline(input_dir: Path, output_dir: Path) -> dict:
    """Run every step and return the statistics used by build_summary()."""
    input_dir, output_dir = Path(input_dir), Path(output_dir)

    files = discover_files(input_dir)
    seen_ids: set[str] = set()   # shared across ALL files, so cross-file repeats are caught
    accepted: list[dict] = []
    rejected: list[dict] = []    # flat dicts, ready to write to rejected_transactions.csv
    column_issues: dict[str, set[str]] = {}
    original_columns: list[str] = []   # union of every file's columns, in first-seen order
    rows_read = 0

    for path in files:
        header, rows = read_transactions(path)
        header = apply_aliases(header, rows)
        rows_read += len(rows)   # count every row read, even from files we reject
        original_columns += [col for col in header if col not in original_columns]

        missing = validate_columns(header)
        if missing:
            # Whole file rejected - but its rows still go to the rejects file,
            # so nothing that was read disappears.
            column_issues[path.name] = missing
            logging.error("%s rejected: missing columns %s", path.name, sorted(missing))
            file_reason = f"column issue: file missing {', '.join(sorted(missing))}"
            rejected += [make_reject(row, [file_reason]) for row in rows]
            continue

        for row in rows:
            problems = find_problems(row, seen_ids)
            if problems:
                rejected.append(make_reject(row, problems))
            else:
                accepted.append(clean_row(row))

    # find_problems() only lets the first copy of an ID through, so every
    # transaction_id in `accepted` is unique. Check it before writing.
    accepted_ids = [r["transaction_id"] for r in accepted]
    assert len(accepted_ids) == len(set(accepted_ids)), "duplicate IDs in accepted rows"

    # Reconciliation: every row read must end up in exactly one output file.
    if len(accepted) + len(rejected) != rows_read:
        raise RuntimeError(
            f"Row count mismatch: read {rows_read}, accepted {len(accepted)}, "
            f"rejected {len(rejected)}"
        )

    write_csv(accepted, output_dir / "consolidated_transactions.csv", OUTPUT_COLUMNS)
    write_csv(rejected, output_dir / "rejected_transactions.csv",
              REJECT_META_COLUMNS + [c for c in original_columns if c not in REJECT_META_COLUMNS])

    return {
        "files_discovered": len(files),
        "files_processed": len(files) - len(column_issues),
        "column_issues": column_issues,
        "rows_read": rows_read,
        "accepted": accepted,
        "rejected": rejected,
        "total_revenue": sum(r["line_total"] for r in accepted),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Consolidate daily transaction CSV files.")
    parser.add_argument("--input", type=Path, default=Path("data"),
                        help="folder containing transactions_*.csv (default: data/)")
    parser.add_argument("--output", type=Path, default=Path("output"),
                        help="folder for the output files (default: output/)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

    stats = run_pipeline(args.input, args.output)
    summary = build_summary(stats)

    print(summary)
    (args.output / "summary.txt").write_text(summary + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
