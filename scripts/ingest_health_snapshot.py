#!/usr/bin/env python3
"""
ingest_health_snapshot.py
--------------------------
Parse a Garmin Health Snapshot .FIT file and insert records into:
  - health_measurement        (one row per snapshot session)
  - health_measurement_reading (one row per second)

The script uses zero external FIT libraries — it parses the FIT binary
directly so it works in the same environment as the existing scripts
(only requires psycopg2-binary and python-dotenv).

Workflow:
  1. Parse the FIT file (record + session messages)
  2. Compute session-level summary metrics from per-second data
  3. Display a preview for confirmation
  4. Write health_measurement + health_measurement_reading in one transaction

Usage:
  python ingest_health_snapshot.py path/to/SNAPSHOT.fit [--dry-run]

Flags:
  --dry-run   Parse and preview only; skip DB writes

Dependencies:
  pip install psycopg2-binary python-dotenv

Environment variables (in .env):
  POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, PG_HOST, PG_PORT

FIT Field Mapping (Health Snapshot message type):
  record field#3   = heart_rate (bpm)
  record field#108 = rr_interval_ms (raw ms — used for HRV RMSSD)
  record field#116 = secondary RR channel (ignored for summary; used for
                     per-second rr_interval_ms when field#108 is absent)
  record field#133 = spo2_pct (UINT8, 0xFF = invalid)
  record field#143 = respiration_rate (breaths/min × 10 — divide by 10)
  session field#169 = avg_heart_rate (UINT16 little-endian)
  session field#170 = max_heart_rate (UINT16 little-endian)
  session field#178 = min_heart_rate (UINT16 little-endian)
  session field#189 = avg_spo2 × 100 (UINT16 LE — e.g. 5100 → 51.00 → divide by 100 → 97.00%)
  session field#190 = avg_rr_interval_ms (UINT16 LE)

Author: John Radcliffe
"""

import sys
import os
import struct
import math
import argparse
from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

FIT_EPOCH = 631065600  # seconds: Unix epoch (1970-01-01) to FIT epoch (1989-12-31)
GLOBAL_MSG_SESSION = 18
GLOBAL_MSG_RECORD = 20
GLOBAL_MSG_FILE_ID = 0
GLOBAL_MSG_DEVICE_INFO = 23

# UINT invalid sentinels by size
UINT8_INVALID = 0xFF
UINT16_INVALID = 0xFFFF
UINT32_INVALID = 0xFFFFFFFF

# FIT base type sizes (type_byte -> byte_size)
FIT_BASE_TYPE_SIZES = {
    0: 1, 1: 1, 2: 1, 3: 2, 4: 2, 5: 4, 6: 4,
    7: 1,  # byte array / string — size comes from field definition
    8: 4, 9: 4, 10: 1, 11: 2, 12: 4, 13: 1, 14: 4, 15: 8, 16: 8,
    # Garmin extended base types
    131: 4, 132: 2, 133: 4, 134: 4, 136: 4, 137: 4, 139: 2, 140: 4,
}


# ---------------------------------------------------------------------------
# FIT BINARY PARSER
# ---------------------------------------------------------------------------

def _fit_timestamp(raw):
    """Convert a FIT UINT32 timestamp to a UTC datetime."""
    if raw is None or raw == UINT32_INVALID:
        return None
    return datetime.fromtimestamp(raw + FIT_EPOCH, tz=timezone.utc)


def _decode_field(data, pos, ftype, fsize, endian):
    """Decode a single FIT field value from raw bytes."""
    raw = data[pos:pos + fsize]

    # Byte arrays / strings: size is the field definition size
    if ftype == 7 or fsize > 8:
        return raw.decode("utf-8", errors="replace").rstrip("\x00")

    if fsize == 1:
        if ftype == 1:  # sint8
            return struct.unpack_from("b", raw)[0]
        return struct.unpack_from("B", raw)[0]  # uint8 / enum

    if fsize == 2:
        if ftype in (3,):  # sint16
            return struct.unpack_from(endian + "h", raw)[0]
        return struct.unpack_from(endian + "H", raw)[0]  # uint16

    if fsize == 4:
        if ftype in (5, 131, 133):  # sint32
            return struct.unpack_from(endian + "i", raw)[0]
        if ftype == 136:  # float32
            return struct.unpack_from(endian + "f", raw)[0]
        return struct.unpack_from(endian + "I", raw)[0]  # uint32

    if fsize == 8:
        if ftype in (6, 7):
            return struct.unpack_from(endian + "q", raw)[0]
        return struct.unpack_from(endian + "Q", raw)[0]

    return raw.hex()


def parse_fit(fit_path):
    """
    Parse a Health Snapshot FIT file.

    Returns:
        records      list[dict]  — per-second measurement rows
        session      dict        — session-level summary fields
        file_info    dict        — file_id fields (device serial, manufacturer)
        device_info  dict        — first device_info message fields
    """
    with open(fit_path, "rb") as f:
        data = f.read()

    header_size = data[0]
    pos = header_size
    file_size = len(data)

    definitions = {}   # local_msg_number -> (global_num, fields_list, endian)
    records = []
    session = {}
    file_info = {}
    device_info = {}

    while pos < file_size - 2:  # last 2 bytes are file CRC
        record_header = data[pos]
        pos += 1

        # Compressed timestamp record
        if record_header & 0x80:
            local_num = (record_header >> 5) & 0x03
            if local_num in definitions:
                _, fields, _ = definitions[local_num]
                pos += sum(f[2] for f in fields)
            continue

        is_def = (record_header >> 6) & 0x01
        has_dev = (record_header >> 5) & 0x01
        local_num = record_header & 0x0F

        if is_def:
            pos += 1  # reserved byte
            arch = data[pos]; pos += 1
            endian = "<" if arch == 0 else ">"
            global_num = struct.unpack_from(endian + "H", data, pos)[0]; pos += 2
            num_fields = data[pos]; pos += 1
            fields = []
            for _ in range(num_fields):
                fnum, fsize, ftype = data[pos], data[pos + 1], data[pos + 2]
                fields.append((fnum, ftype, fsize))
                pos += 3
            if has_dev:
                num_dev = data[pos]; pos += 1
                pos += num_dev * 3
            definitions[local_num] = (global_num, fields, endian)

        else:
            if local_num not in definitions:
                break
            global_num, fields, endian = definitions[local_num]
            row = {}
            for fnum, ftype, fsize in fields:
                row[fnum] = _decode_field(data, pos, ftype, fsize, endian)
                pos += fsize

            if global_num == GLOBAL_MSG_RECORD:
                records.append(row)
            elif global_num == GLOBAL_MSG_SESSION:
                session = row
            elif global_num == GLOBAL_MSG_FILE_ID:
                file_info = row
            elif global_num == GLOBAL_MSG_DEVICE_INFO and not device_info:
                device_info = row

    return records, session, file_info, device_info


# ---------------------------------------------------------------------------
# METRIC EXTRACTION
# ---------------------------------------------------------------------------

def _valid_uint8(v):
    return v if (v is not None and v != UINT8_INVALID) else None


def _valid_uint16(v):
    return v if (v is not None and v != UINT16_INVALID) else None


def _valid_uint32(v):
    return v if (v is not None and v != UINT32_INVALID) else None


def _rmssd(rr_intervals_ms):
    """Compute RMSSD from a list of RR interval values (in ms).

    RMSSD = sqrt(mean of squared successive differences).
    Requires at least 2 valid values.
    """
    valid = [v for v in rr_intervals_ms if v is not None and 300 <= v <= 2000]
    if len(valid) < 2:
        return None
    diffs = [(valid[i + 1] - valid[i]) ** 2 for i in range(len(valid) - 1)]
    return round(math.sqrt(sum(diffs) / len(diffs)), 1)


def extract_records(raw_records):
    """Map raw FIT record messages to health_measurement_reading rows."""
    rows = []
    start_ts = None

    for raw in raw_records:
        ts_raw = _valid_uint32(raw.get(253))
        ts = _fit_timestamp(ts_raw)
        if ts is None:
            continue

        if start_ts is None:
            start_ts = ts

        elapsed = int((ts - start_ts).total_seconds())

        # heart_rate: field#3, confirmed bpm
        hr = _valid_uint8(raw.get(3))

        # rr_interval_ms: field#108 (primary); field#116 / 5 as fallback
        rr_raw = _valid_uint16(raw.get(108))
        if rr_raw is None:
            rr_raw_116 = _valid_uint16(raw.get(116))
            rr_raw = round(rr_raw_116 / 5) if rr_raw_116 is not None else None

        # spo2_pct: field#133 (UINT8, 0xFF = invalid)
        spo2_raw = _valid_uint8(raw.get(133))

        # respiration_rate: field#143 (value × 10 scale → divide by 10)
        resp_raw = _valid_uint8(raw.get(143))
        resp = round(resp_raw / 10, 1) if resp_raw is not None else None

        rows.append({
            "recorded_at": ts,
            "elapsed_seconds": elapsed,
            "heart_rate": hr,
            "rr_interval_ms": rr_raw,
            "spo2_pct": spo2_raw,
            "respiration_rate": resp,
            "stress_level": None,  # not present in this file type
        })

    return rows


def extract_session(session, record_rows, file_info):
    """
    Extract health_measurement fields from the session message and
    compute derived summary metrics from per-second record data.
    """
    # --- start time ---
    ts_raw = _valid_uint32(session.get(253))
    started_at = _fit_timestamp(ts_raw)
    if started_at is None and record_rows:
        started_at = record_rows[0]["recorded_at"]

    measurement_date = started_at.date() if started_at else None

    # --- duration ---
    if record_rows and len(record_rows) > 1:
        duration_seconds = record_rows[-1]["elapsed_seconds"]
    else:
        duration_seconds = None

    # --- device name ---
    # file_info field#3 = serial number (uint32), field#1 = manufacturer
    serial = _valid_uint32(file_info.get(3))
    device_name = f"Garmin:{serial}" if serial else "Garmin"

    # --- measurement type ---
    measurement_type = "health_snapshot"

    # --- HR summary from session fields ---
    # field#169 = avg_hr (UINT16 LE), field#170 = max_hr, field#178 = min_hr (UINT16)
    avg_hr_sess = _valid_uint16(session.get(169))
    max_hr_sess = _valid_uint16(session.get(170))
    min_hr_sess = _valid_uint16(session.get(178))

    # Compute from records as backup
    hr_vals = [r["heart_rate"] for r in record_rows if r["heart_rate"] is not None]
    avg_hr = avg_hr_sess or (round(sum(hr_vals) / len(hr_vals)) if hr_vals else None)
    max_hr = max_hr_sess or (max(hr_vals) if hr_vals else None)
    min_hr = min_hr_sess or (min(hr_vals) if hr_vals else None)

    # Resting HR = the sustained minimum (10th-percentile of HR values)
    if hr_vals:
        sorted_hr = sorted(hr_vals)
        p10_idx = max(0, int(len(sorted_hr) * 0.10) - 1)
        resting_hr = sorted_hr[p10_idx]
    else:
        resting_hr = min_hr

    # --- HRV RMSSD from per-second RR intervals ---
    rr_vals = [r["rr_interval_ms"] for r in record_rows]
    hrv_rmssd = _rmssd(rr_vals)

    # --- SpO2 ---
    # session field#189 = avg_spo2 encoded as UINT16 (value × 100, then ÷ 100 → %)
    # e.g. 5100 → 51.00 → actually this is RR avg; spo2 from session field is ambiguous
    # Derive from per-second readings instead for reliability
    spo2_vals = [r["spo2_pct"] for r in record_rows if r["spo2_pct"] is not None]
    avg_spo2 = round(sum(spo2_vals) / len(spo2_vals), 2) if spo2_vals else None
    min_spo2 = min(spo2_vals) if spo2_vals else None

    # --- Respiration ---
    resp_vals = [r["respiration_rate"] for r in record_rows if r["respiration_rate"] is not None]
    avg_resp = round(sum(resp_vals) / len(resp_vals), 1) if resp_vals else None

    # --- Stress (not in this file type, reserved) ---
    avg_stress = None

    return {
        "measurement_date": measurement_date,
        "measurement_type": measurement_type,
        "started_at": started_at,
        "duration_seconds": duration_seconds,
        "device_name": device_name,
        "resting_heart_rate": resting_hr,
        "avg_heart_rate": avg_hr,
        "min_heart_rate": min_hr,
        "max_heart_rate": max_hr,
        "hrv_rmssd_ms": hrv_rmssd,
        "avg_spo2_pct": avg_spo2,
        "min_spo2_pct": min_spo2,
        "avg_respiration_rate": avg_resp,
        "avg_stress_level": avg_stress,
        "source_system": "garmin_fit",
        "source_object": "ingest_health_snapshot.py",
        "source_record_id": None,
    }


# ---------------------------------------------------------------------------
# PREVIEW
# ---------------------------------------------------------------------------

def print_preview(meas, record_rows):
    print("\n" + "=" * 60)
    print("  HEALTH SNAPSHOT — PARSED PREVIEW")
    print("=" * 60)
    print(f"  Date             : {meas['measurement_date']}")
    print(f"  Started at (UTC) : {meas['started_at']}")
    print(f"  Duration         : {meas['duration_seconds']} s")
    print(f"  Device           : {meas['device_name']}")
    print(f"  Type             : {meas['measurement_type']}")
    print()
    print(f"  Resting HR       : {meas['resting_heart_rate']} bpm")
    print(f"  Avg HR           : {meas['avg_heart_rate']} bpm")
    print(f"  Min HR           : {meas['min_heart_rate']} bpm")
    print(f"  Max HR           : {meas['max_heart_rate']} bpm")
    print(f"  HRV RMSSD        : {meas['hrv_rmssd_ms']} ms")
    print(f"  Avg SpO2         : {meas['avg_spo2_pct']} %")
    print(f"  Min SpO2         : {meas['min_spo2_pct']} %")
    print(f"  Avg Respiration  : {meas['avg_respiration_rate']} br/min")
    print(f"  Avg Stress       : {meas['avg_stress_level']}")
    print()
    print(f"  Per-second rows  : {len(record_rows)}")

    # Show first and last row
    if record_rows:
        r0 = record_rows[0]
        rn = record_rows[-1]
        print(f"\n  First reading  : t={r0['elapsed_seconds']}s  "
              f"hr={r0['heart_rate']}  rr={r0['rr_interval_ms']}ms  "
              f"spo2={r0['spo2_pct']}%  resp={r0['respiration_rate']}br/min")
        print(f"  Last reading   : t={rn['elapsed_seconds']}s  "
              f"hr={rn['heart_rate']}  rr={rn['rr_interval_ms']}ms  "
              f"spo2={rn['spo2_pct']}%  resp={rn['respiration_rate']}br/min")
    print("=" * 60)


# ---------------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "personal_crm"),
        user=os.getenv("POSTGRES_USER", "john"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
    )


def resolve_day_id(conn, measurement_date):
    """Look up day_id for a given date. Returns None if not found."""
    with conn.cursor() as cur:
        cur.execute("SELECT day_id FROM day WHERE date = %s", (measurement_date,))
        row = cur.fetchone()
    return row[0] if row else None


def insert_health_measurement(cur, meas, day_id):
    """Insert into health_measurement, return health_measurement_id."""
    sql = """
        INSERT INTO health_measurement (
            day_id, measurement_date, measurement_type,
            started_at, duration_seconds, device_name,
            resting_heart_rate, avg_heart_rate, min_heart_rate, max_heart_rate,
            hrv_rmssd_ms, avg_spo2_pct, min_spo2_pct,
            avg_respiration_rate, avg_stress_level,
            source_system, source_object, source_record_id
        ) VALUES (
            %(day_id)s, %(measurement_date)s, %(measurement_type)s,
            %(started_at)s, %(duration_seconds)s, %(device_name)s,
            %(resting_heart_rate)s, %(avg_heart_rate)s, %(min_heart_rate)s, %(max_heart_rate)s,
            %(hrv_rmssd_ms)s, %(avg_spo2_pct)s, %(min_spo2_pct)s,
            %(avg_respiration_rate)s, %(avg_stress_level)s,
            %(source_system)s, %(source_object)s, %(source_record_id)s
        )
        ON CONFLICT (started_at) DO UPDATE SET
            avg_heart_rate      = EXCLUDED.avg_heart_rate,
            min_heart_rate      = EXCLUDED.min_heart_rate,
            max_heart_rate      = EXCLUDED.max_heart_rate,
            hrv_rmssd_ms        = EXCLUDED.hrv_rmssd_ms,
            avg_spo2_pct        = EXCLUDED.avg_spo2_pct,
            min_spo2_pct        = EXCLUDED.min_spo2_pct,
            avg_respiration_rate= EXCLUDED.avg_respiration_rate,
            updated_at          = now()
        RETURNING health_measurement_id;
    """
    cur.execute(sql, {"day_id": day_id, **meas})
    return cur.fetchone()[0]


def insert_readings(cur, health_measurement_id, record_rows):
    """Bulk insert health_measurement_reading rows."""
    sql = """
        INSERT INTO health_measurement_reading (
            health_measurement_id, recorded_at, elapsed_seconds,
            heart_rate, rr_interval_ms, spo2_pct,
            respiration_rate, stress_level
        ) VALUES (
            %(health_measurement_id)s, %(recorded_at)s, %(elapsed_seconds)s,
            %(heart_rate)s, %(rr_interval_ms)s, %(spo2_pct)s,
            %(respiration_rate)s, %(stress_level)s
        )
        ON CONFLICT (health_measurement_id, recorded_at) DO NOTHING;
    """
    rows = [{"health_measurement_id": health_measurement_id, **r} for r in record_rows]
    execute_batch(cur, sql, rows, page_size=500)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest a Garmin Health Snapshot FIT file into PostgreSQL."
    )
    parser.add_argument("fit_path", help="Path to the .fit file")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and preview only — do not write to the database"
    )
    args = parser.parse_args()

    if not os.path.exists(args.fit_path):
        print(f"Error: file not found — {args.fit_path}")
        sys.exit(1)

    # ── Step 1: Parse FIT file ───────────────────────────────
    print(f"\nParsing {args.fit_path} ...")
    raw_records, session, file_info, device_info = parse_fit(args.fit_path)
    print(f"  Found {len(raw_records)} per-second records in FIT file.")

    if not raw_records:
        print("  Error: no record messages found. Is this a Health Snapshot FIT file?")
        sys.exit(1)

    # ── Step 2: Extract structured data ─────────────────────
    record_rows = extract_records(raw_records)
    meas = extract_session(session, record_rows, file_info)

    # ── Step 3: Preview ──────────────────────────────────────
    print_preview(meas, record_rows)

    if args.dry_run:
        print("\n  [DRY RUN] — no database writes performed.")
        sys.exit(0)

    # ── Step 4: Confirm ──────────────────────────────────────
    confirm = input("\n  Write to database? (y/n): ").strip().lower()
    if confirm != "y":
        print("  Aborted — nothing written.")
        sys.exit(0)

    # ── Step 5: Write to Postgres ────────────────────────────
    print("\n  Connecting to database ...")
    conn = get_connection()

    try:
        measurement_date = meas.get("measurement_date")
        if not measurement_date:
            print("  Error: could not determine measurement date from FIT file.")
            sys.exit(1)

        day_id = resolve_day_id(conn, measurement_date)
        if not day_id:
            print(f"  Warning: no day record found for {measurement_date}. "
                  "health_measurement.day_id will be NULL.")

        with conn:
            with conn.cursor() as cur:
                hm_id = insert_health_measurement(cur, meas, day_id)
                print(f"  ✓ health_measurement inserted — id = {hm_id}")

                insert_readings(cur, hm_id, record_rows)
                print(f"  ✓ {len(record_rows)} reading rows inserted")

        print("\n" + "=" * 60)
        print(f"  DONE — Health Snapshot #{hm_id} logged.")
        print(f"  {measurement_date}  |  HRV {meas['hrv_rmssd_ms']} ms  "
              f"|  Resting HR {meas['resting_heart_rate']} bpm  "
              f"|  SpO2 {meas['avg_spo2_pct']} %")
        print("=" * 60 + "\n")

    except Exception as e:
        conn.rollback()
        print(f"\n  Error during database write: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
