import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

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

try:
    cur = conn.cursor()
    cur.execute("""
        SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(),
               CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION()
    """)
    user, role, wh, db, schema, version = cur.fetchone()
    print("Connected to Snowflake ✅")
    print(f"  User:      {user}")
    print(f"  Role:      {role}")
    print(f"  Warehouse: {wh}")
    print(f"  Database:  {db}")
    print(f"  Schema:    {schema}")
    print(f"  Version:   {version}")

    # Check the role can see the stage
    cur.execute("LIST @PPD_STAGE")
    print(f"Stage accessible ✅ ({len(cur.fetchall())} files)")
finally:
    conn.close()