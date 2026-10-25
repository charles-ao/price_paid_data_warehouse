import os
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

print("1. Begin script...")

load_dotenv()

# --- Settings for this run -------------------------------------------------
VERSION_ID = 20260828   # Land Registry release these files came from
SOURCE_TYPE = "yearly"  # "yearly" or "monthly" - applies to every file in the folder
SOURCE_DIR = Path(__file__).resolve().parent.parent / "local_stage"
STAGE_PATH = f"@PPD_STAGE/{VERSION_ID}/"  # version folder keeps names unique

COPY_SQL = """
COPY INTO PPD_RAW (
    transaction_unique_identifier, price, date_of_transfer, postcode,
    property_type, old_new, duration, paon, saon, street, locality,
    town, district, county, ppd_category_type, record_status,
    _file_name, _loaded_at, _source_type, _version_id
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME, CURRENT_TIMESTAMP(), '{source_type}', {version_id}
    FROM {stage_path}
)
FILES = ('{staged_name}')
ON_ERROR = ABORT_STATEMENT
"""

CHECK_AUDIT_SQL = """
SELECT COUNT(*) FROM PPD_LOAD_AUDIT
WHERE file_name = %s AND status = 'LOADED'
"""

INSERT_AUDIT_SQL = """
INSERT INTO PPD_LOAD_AUDIT
    (file_name, source_type, version_id, source_row_count, rows_loaded, status, error_message)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# --- Find the files --------------------------------------------------------
files = sorted(SOURCE_DIR.glob("*.csv"))
if not files:
    raise SystemExit(f"No CSV files found in {SOURCE_DIR}")

print(f"2. Found {len(files)} file(s) in {SOURCE_DIR}:")
for file in files:
    print(f"   - {file.name}")

# --- Connect to Snowflake ---------------------------------------------------
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    private_key_file_pwd=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)
cur = conn.cursor()
failed_files = []

try:
    print(f"3. Loading files via {STAGE_PATH}")
    for file in files:
        # Name as it will appear in the stage, e.g. 20260828/pp-2020.csv.gz
        audit_name = f"{VERSION_ID}/{file.name}.gz"

        # --- Step 1: skip files already recorded as loaded -------------------
        cur.execute(CHECK_AUDIT_SQL, (audit_name,))
        if cur.fetchone()[0] > 0:
            print(f"   SKIP {audit_name}: already LOADED in audit table")
            continue

        # Count lines in the local file for reconciliation with rows_loaded
        with open(file, "rb") as fh:
            line_count = sum(1 for _ in fh)

        try:
            # --- Step 2: upload one file to the internal stage ---------------
            cur.execute(
                f"PUT 'file://{file.as_posix()}' {STAGE_PATH} "
                "AUTO_COMPRESS = TRUE OVERWRITE = FALSE"
            )
            put_result = cur.fetchone()
            # PUT returns: source, target, source_size, target_size,
            #              source_compression, target_compression, status, message
            staged_name = put_result[1]  # e.g. pp-2020.csv.gz
            print(f"   PUT  {put_result[0]} -> {staged_name}: {put_result[6]}")

            # --- Step 3: copy that file into the raw table -------------------
            cur.execute(COPY_SQL.format(
                source_type=SOURCE_TYPE,
                version_id=VERSION_ID,
                stage_path=STAGE_PATH,
                staged_name=staged_name,
            ))
            copy_result = cur.fetchone()

            if len(copy_result) == 1:
                # Snowflake has loaded this file into the table before: no audit row
                print(f"   COPY {staged_name}: {copy_result[0]}")
                continue

            # COPY returns: file, status, rows_parsed, rows_loaded, ...
            rows_loaded = copy_result[3]
            print(f"   COPY {staged_name}: {copy_result[1]}, "
                  f"{rows_loaded:,} of {line_count:,} lines loaded")
            status, error_message = "LOADED", None

        except Exception as e:
            # PUT or COPY failed: record the failure and move on to the next file
            rows_loaded, status, error_message = None, "FAILED", str(e)
            failed_files.append(file.name)
            print(f"   FAILED {file.name}: {error_message}")

        # --- Step 4: record the outcome in the audit table -------------------
        cur.execute(INSERT_AUDIT_SQL, (
            audit_name, SOURCE_TYPE, VERSION_ID, line_count,
            rows_loaded, status, error_message,
        ))

finally:
    cur.close()
    conn.close()

if failed_files:
    raise SystemExit(f"4. Finished with {len(failed_files)} failed file(s): {failed_files}")

print("4. Done.")