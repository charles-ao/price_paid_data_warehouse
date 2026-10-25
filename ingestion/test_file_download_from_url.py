import os
import requests
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv


load_dotenv()

url = "https://price-paid-data.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv"
LOCAL_STAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local_stage")

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
try:
    req = requests.head(url)
    url_date_format = '%d %b %Y'
    last_modified = req.headers['Last-Modified']
    url_date = last_modified[5:16]
    date_obj = datetime.strptime(url_date, url_date_format)
    VERSION_ID = date_obj.strftime('%Y%m%d') 

    res = cur.execute(
        f"""
            SELECT COUNT(*) FROM PPDW.BRONZE.PPD_LOAD_AUDIT
            WHERE STATUS = 'LOADED' AND FILE_NAME = '{VERSION_ID}/pp-monthly-update-new-version.csv.gz'
        """
    ).fetchone()

    if res[0] == 0:
        print("Okay to Load.....")
        file_payload = requests.get(url)
        file_payload.raise_for_status()

        os.makedirs(LOCAL_STAGE_DIR, exist_ok=True)
        file_path = os.path.join(LOCAL_STAGE_DIR, "pp-monthly-update-new-version.csv")
        with open(file_path, "wb") as f:
            f.write(file_payload.content)
        print(f"Saved file to {file_path}")


except Exception as e:
    print(e)

finally:
    cur.close()
