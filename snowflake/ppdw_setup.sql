------ Setup -----------
CREATE DATABASE IF NOT EXISTS PPDW;

USE DATABASE PPDW;

CREATE SCHEMA IF NOT EXISTS PPDW.BRONZE;
CREATE SCHEMA IF NOT EXISTS PPDW.SILVER;
CREATE SCHEMA IF NOT EXISTS PPDW.GOLD;

DROP FILE FORMAT IF EXISTS PPDW.BRONZE.CSV_FILES;
CREATE FILE FORMAT IF NOT EXISTS PPDW.BRONZE.CSV_FILES
  TYPE = 'CSV'
  SKIP_HEADER = 0
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('');
  COMMENT = 'PPD CSV files with no header row';


CREATE STAGE IF NOT EXISTS PPDW.BRONZE.PPD_STAGE
  ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL')
  DIRECTORY = (
    ENABLE = TRUE
    AUTO_REFRESH = TRUE
    )
  FILE_FORMAT = PPDW.BRONZE.CSV_FILES
  COMMENT = 'Price Paid Data Internal Stage';
    

--------ROLES, USERS AND PERMISSIONS ---------

USE ROLE ACCOUNTADMIN;

DROP ROLE IF EXISTS PPDW_ROLE;
CREATE ROLE IF NOT EXISTS PPDW_ROLE; --- One role for the whole pipeline

GRANT ROLE PPDW_ROLE TO ROLE ACCOUNTADMIN; --- Make PPDW_ROLE A child of ACCOUNTADMIN

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE PPDW_ROLE;
GRANT ALL ON DATABASE PPDW to ROLE PPDW_ROLE;

GRANT ALL ON ALL SCHEMAS IN DATABASE PPDW to ROLE PPDW_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE PPDW to ROLE PPDW_ROLE;

GRANT READ, WRITE ON STAGE PPDW.BRONZE.PPD_STAGE TO ROLE PPDW_ROLE;
GRANT USAGE ON FILE FORMAT PPDW.BRONZE.CSV_FILES TO ROLE PPDW_ROLE;

GRANT ALL ON ALL TABLES IN SCHEMA PPDW.BRONZE to ROLE PPDW_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA PPDW.BRONZE to ROLE PPDW_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA PPDW.SILVER TO ROLE PPDW_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PPDW.SILVER TO ROLE PPDW_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA PPDW.SILVER TO ROLE PPDW_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA PPDW.SILVER TO ROLE PPDW_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA PPDW.GOLD TO ROLE PPDW_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PPDW.GOLD TO ROLE PPDW_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA PPDW.GOLD TO ROLE PPDW_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA PPDW.GOLD TO ROLE PPDW_ROLE;


---- CREATE SERVICE USER
DROP USER IF EXISTS PPDW_USER;
CREATE USER IF NOT EXISTS PPDW_USER
  TYPE = SERVICE
  DEFAULT_ROLE = PPDW_ROLE
  DEFAULT_WAREHOUSE = 'COMPUTE_WH'
  RSA_PUBLIC_KEY="-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAm45imOk0VoYPtYZst2rf
GRWycW3zJhDIux0Ox4h7jzAPrvwMqrChR4Fg7lgXAC/QpptFfVpY29PloLLRFcJy
JHZ2OLs9ikuBNOVA7ajJHe7+Pjy4Ux38oEcn3QwCOnxpx1mmH1aCkuPor8eCWEce
Sk8htQqv8dXrxChyazJ6oHpySKSkYLIuycOqKBJOMHYp0J6Ewmb1UL/we3dGS693
UH/JUOEZa77qHtDoKgsAvx7VfGOCk0YR1y4nedJVKjm1hQ0sL99wmXps823p+ltV
CqThSbj3GXfBJX4s5m2oAccESeJvRs3Olvz0rEdoH/cOoYMOeRP3Iax8Ywke6S2t
aQIDAQAB
-----END PUBLIC KEY-----
"
  COMMENT = 'Service user for Python loader and dbt runs';


GRANT ROLE PPDW_ROLE TO USER PPDW_USER;


------- TABLE CREATION -------------

CREATE TABLE IF NOT EXISTS BRONZE.PPD_RAW (
    transaction_unique_identifier varchar,
    price                         varchar,
    date_of_transfer              varchar,
    postcode                      varchar,
    property_type                 varchar,
    old_new                       varchar,
    duration                      varchar,
    paon                          varchar,
    saon                          varchar,
    street                        varchar,
    locality                      varchar,
    town                          varchar,
    district                      varchar,
    county                        varchar,
    ppd_category_type             varchar,
    record_status                 varchar,
    _file_name                    varchar,
    _loaded_at                    timestamp_ntz,
    _source_type                  varchar,
    _version_id                   number
);

CREATE TABLE IF NOT EXISTS  BRONZE.PPD_LOAD_AUDIT (
    file_name        varchar not null,   -- e.g. pp-2020_20260828.csv
    source_type      varchar,            -- yearly / monthly
    version_id       number,             -- e.g. 20260828
    source_row_count number,             -- lines counted by Python
    rows_loaded      number,             -- reported by COPY INTO
    status           varchar,            -- LOADED / FAILED
    error_message    varchar
);