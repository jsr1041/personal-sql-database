"""
ingest_workout_plan.py
Ingest workout_plan rows from workout_plan_import.xlsx into PostgreSQL.

Usage:
    python ingest_workout_plan.py                  # live run
    python ingest_workout_plan.py --dry-run        # validate + preview only, no DB writes
    python ingest_workout_plan.py --xlsx PATH      # specify alternate xlsx path
"""

import argparse
import os
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

# Load .env from project root (one level above scripts/)
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_script_dir)
load_dotenv(os.path.join(_project_root, ".env"))

XLSX_DEFAULT = os.path.join(_project_root, "workout_plan_import.xlsx")

INSERT_SQL = """
    INSERT INTO public.workout_plan (
        title, type_of_activity, warmup, the_work, cooldown,
        source, url, notes
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
    RETURNING workout_plan_id, title;
"""

NS = {
    "ss": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r":  "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def load_data(xlsx_path: str) -> list[dict]:
    """Read the 'workout_plan' sheet from an xlsx using only stdlib (zipfile + xml)."""
    with zipfile.ZipFile(xlsx_path) as zf:
        # Load shared strings table
        shared_strings = []
        if "xl/sharedStrings.xml" in zf.namelist():
            sst = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in sst.findall("ss:si", NS):
                # Concatenate all <t> text nodes within each <si>
                text = "".join(t.text or "" for t in si.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"))
                shared_strings.append(text)

        # Find the sheet named "workout_plan" via workbook relationships
        wb_xml = ET.fromstring(zf.read("xl/workbook.xml"))
        wb_rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

        # Find sheet rId for "workout_plan"
        sheet_id = None
        for sheet in wb_xml.findall(".//ss:sheet", NS):
            if sheet.get("name") == "workout_plan":
                sheet_id = sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                break
        if sheet_id is None:
            raise ValueError("Sheet 'workout_plan' not found in workbook.")

        # Resolve rId → zip path; rels use package/2006 namespace
        REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
        sheet_path = None
        for rel in wb_rels.findall(f"{{{REL_NS}}}Relationship"):
            if rel.get("Id") == sheet_id:
                target = rel.get("Target")          # e.g. "/xl/worksheets/sheet1.xml"
                sheet_path = target.lstrip("/")     # "xl/worksheets/sheet1.xml"
                break
        if sheet_path is None or sheet_path not in zf.namelist():
            raise ValueError(f"Could not resolve sheet path for rId '{sheet_id}'. Got: {sheet_path}")

        ws_xml = ET.fromstring(zf.read(sheet_path))

    SST = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

    def cell_value(c):
        t = c.get("t")
        if t == "inlineStr":
            is_el = c.find(f"{SST}is/{SST}t")
            return is_el.text if is_el is not None else None
        v_el = c.find(f"{SST}v")
        if v_el is None or v_el.text is None:
            return None
        if t == "s":
            return shared_strings[int(v_el.text)]
        return v_el.text

    all_rows = []
    for row_el in ws_xml.findall(".//ss:row", NS):
        row_data = {}
        for c in row_el.findall("ss:c", NS):
            ref = c.get("r")  # e.g. "A2"
            col_letter = "".join(ch for ch in ref if ch.isalpha())
            row_data[col_letter] = cell_value(c)
        all_rows.append(row_data)

    if not all_rows:
        return []

    # First row = headers; map column letter → header name
    header_row = all_rows[0]
    col_map = {letter: (val.strip() if val else letter) for letter, val in header_row.items()}

    result = []
    for row in all_rows[1:]:
        record = {}
        for letter, header in col_map.items():
            val = row.get(letter)
            if isinstance(val, str):
                val = val.strip() or None
            record[header] = val
        # Skip entirely blank rows
        if any(v for v in record.values()):
            result.append(record)

    return result


def validate(rows: list[dict]) -> list[str]:
    errors = []
    if not rows:
        return ["No data rows found in spreadsheet."]
    for col in ["title", "type_of_activity"]:
        if col not in rows[0]:
            errors.append(f"Missing required column: {col}")
    missing = [i + 2 for i, r in enumerate(rows) if not r.get("title")]
    if missing:
        errors.append(f"{len(missing)} row(s) missing 'title': Excel rows {missing}")
    return errors


def preview(rows: list[dict]) -> None:
    print(f"\n{'='*70}")
    print(f"DRY RUN — {len(rows)} rows would be inserted")
    print(f"{'='*70}")
    print(f"{'#':<4} {'Title':<45} {'Type':<12}")
    print(f"{'-'*4} {'-'*45} {'-'*12}")
    for i, row in enumerate(rows, start=1):
        title = (row.get("title") or "")[:44]
        typ = (row.get("type_of_activity") or "")[:12]
        print(f"{i:<4} {title:<45} {typ:<12}")
    print(f"\nNo database changes made.")


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "personal_crm"),
        user=os.getenv("POSTGRES_USER", "john"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
    )


def run_ingest(rows: list[dict]) -> None:
    conn = get_connection()
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    try:
        for row in rows:
            params = (
                row.get("title"),
                row.get("type_of_activity"),
                row.get("warmup"),
                row.get("the_work"),
                row.get("cooldown"),
                row.get("source"),
                row.get("url"),
                row.get("notes"),
            )
            cur.execute(INSERT_SQL, params)
            result = cur.fetchone()
            if result:
                print(f"  [INSERT] id={result[0]:>4}  {result[1][:60]}")
                inserted += 1
            else:
                print(f"  [SKIP]   (conflict/duplicate) {(row.get('title') or '')[:60]}")
                skipped += 1

        conn.commit()
        print(f"\n{'='*70}")
        print(f"Ingestion complete: {inserted} inserted, {skipped} skipped.")
        print(f"{'='*70}")

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] Transaction rolled back: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest workout_plan from Excel to PostgreSQL")
    parser.add_argument("--dry-run", action="store_true", help="Validate and preview without writing to DB")
    parser.add_argument("--xlsx", default=XLSX_DEFAULT, metavar="PATH", help="Path to workout_plan_import.xlsx (default: ../workout_plan_import.xlsx relative to this script)")
    args = parser.parse_args()

    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Starting ingest_workout_plan")
    print(f"Source: {args.xlsx}")

    # Load
    if not os.path.exists(args.xlsx):
        print(f"[ERROR] File not found: {args.xlsx}", file=sys.stderr)
        sys.exit(1)

    rows = load_data(args.xlsx)
    print(f"Loaded {len(rows)} rows from spreadsheet.")

    # Validate
    errors = validate(rows)
    if errors:
        print("\n[VALIDATION ERRORS]")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    print("Validation passed.")

    if args.dry_run:
        preview(rows)
        sys.exit(0)

    print(f"\nConnecting to database...")
    run_ingest(rows)


if __name__ == "__main__":
    main()
