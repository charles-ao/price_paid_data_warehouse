import os
import sys
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
import snowflake.connector

print("1. Begin script...")

load_dotenv()

# --- Settings for this run -------------------------------------------------
SOURCE_URL = "https://price-paid-data.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv"
SOURCE_TYPE = "monthly"
LOCAL_STAGE_DIR = Path(__file__).resolve().parent.parent / "local_stage"
FILE_NAME = SOURCE_URL.rsplit("/", 1)[-1]  # pp-monthly-update-new-version.csv

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

# --- Work out which release is published -----------------------------------
head = requests.head(SOURCE_URL, timeout=30, allow_redirects=True)
head.raise_for_status()
# Last-Modified looks like: "Thu, 28 Aug 2026 10:15:00 GMT"
last_modified = head.headers.get("Last-Modified")
if not last_modified:
    raise SystemExit("No Last-Modified header: cannot determine the release version")
VERSION_ID = int(parsedate_to_datetime(last_modified).strftime("%Y%m%d"))
STAGE_PATH = f"@PPD_STAGE/{VERSION_ID}/"  # version folder keeps names unique
audit_name = f"{VERSION_ID}/{FILE_NAME}.gz"  # name as it will appear in the stage
print(f"2. Latest release on the server is {VERSION_ID}")

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
file = LOCAL_STAGE_DIR / FILE_NAME
failed = False

try:
    # --- Step 1: skip if this release is already recorded as loaded ---------
    cur.execute(CHECK_AUDIT_SQL, (audit_name,))
    if cur.fetchone()[0] > 0:
        print(f"   SKIP {audit_name}: already LOADED in audit table")
        sys.exit(0)

    # --- Step 2: download the file into local_stage -------------------------
    print(f"3. Downloading {SOURCE_URL}")
    LOCAL_STAGE_DIR.mkdir(parents=True, exist_ok=True)
    with requests.get(SOURCE_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(file, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                fh.write(chunk)
    print(f"   Saved file to {file}")

    # Count lines in the local file for reconciliation with rows_loaded
    with open(file, "rb") as fh:
        line_count = sum(1 for _ in fh)

    print(f"4. Loading via {STAGE_PATH}")
    try:
        # --- Step 3: upload the file to the internal stage ------------------
        cur.execute(
            f"PUT 'file://{file.as_posix()}' {STAGE_PATH} "
            "AUTO_COMPRESS = TRUE OVERWRITE = FALSE"
        )
        put_result = cur.fetchone()
        # PUT returns: source, target, source_size, target_size,
        #              source_compression, target_compression, status, message
        staged_name = put_result[1]  # e.g. pp-monthly-update-new-version.csv.gz
        print(f"   PUT  {put_result[0]} -> {staged_name}: {put_result[6]}")

        # --- Step 4: copy that file into the raw table ----------------------
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
            sys.exit(0)

        # COPY returns: file, status, rows_parsed, rows_loaded, ...
        rows_loaded = copy_result[3]
        print(f"   COPY {staged_name}: {copy_result[1]}, "
              f"{rows_loaded:,} of {line_count:,} lines loaded")
        status, error_message = "LOADED", None

    except snowflake.connector.Error as e:
        # PUT or COPY failed: record the failure
        rows_loaded, status, error_message = None, "FAILED", str(e)
        failed = True
        print(f"   FAILED {FILE_NAME}: {error_message}")

    # --- Step 5: record the outcome in the audit table ----------------------
    cur.execute(INSERT_AUDIT_SQL, (
        audit_name, SOURCE_TYPE, VERSION_ID, line_count,
        rows_loaded, status, error_message,
    ))

finally:
    cur.close()
    conn.close()
    # --- Step 6: remove the downloaded file ---------------------------------
    if file.exists():
        file.unlink()
        print(f"   Deleted {file}")

if failed:
    print(f"5. Finished with a failed load: {audit_name}")
    sys.exit(1)

print("5. Done.")
