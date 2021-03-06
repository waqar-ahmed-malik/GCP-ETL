CREATE OR REPLACE TABLE LANDING.WORK_QUOTES_DIM(
QUOTE_NUM STRING,
POLICY_NUM STRING,
CREATE_DT DATE,
UPDATE_DT DATE,
EFFECTIVE_DT DATE,
QUOTE_ID INT64,
OPPORTUNITY_ID INT64,
CUSTOMER_ID INT64,
SOURCE_CUSTOMER_ID STRING,
POLICY_STATUS STRING,
PRODUCT_TYPE STRING,
PRODUCT_CD STRING,
POLICY_STATE STRING,
PREMIUM INT64,
POLICY_TERM_MONTHS INT64,
POLICY_CARRIER STRING,
SOURCE_SYSTEM STRING,
CURRENT_IMAGE_IND INT64,
AGE_OF_QUOTE STRING,
QUOTE_VERSION_AGE STRING,
QUOTE_COUNT INT64,
UPLOAD_COUNT INT64,
QUOTE_STATUS STRING,
TRANSACTION_TYPE STRING,
BINDING_AGENT STRING,
AGENT_OF_RECORD STRING,
PRODUCER_EMAIL STRING,
EMPLOYEE_ID STRING,
CHANNEL_TYPE STRING,
AGENCY_NM STRING,
SALES_CHANNEL STRING,
AGENCY_LOCATION_NM STRING,
AGENCY_LOCATION_ID INT64,
LEAD_SOURCE STRING,
QUOTE_TYPE STRING,
CONVERSION_DT DATE,
IMPORTED_FLAG STRING,
PRIOR_POLICY_NUM STRING,
PRIOR_INSURANCE_COMPANY STRING,
PRIOR_CARRIER_BI_LIMIT STRING,
DAYS_LAPSED INT64,
HOME_OWNER_TYPE STRING,
ASSOCIATION STRING,
MEMBERSHIP_NUM STRING,
TIER STRING,
AGENT_NOTE STRING,
UW_NOTE STRING,
LICENSE_STATUS_INFORMATION STRING,
CLUE_INFORMATION STRING,
VEHICLE_USAGE STRING,
OTHER_NOTES STRING,
FR_LEVEL STRING,
GENDER STRING,
TOLL_FREE_NUM STRING,
INSERT_DT DATE,
INSERT_BY STRING,
LAST_UPDATED_AGENT STRING,
LAST_UPDATED_DT DATE,
VOICE_BIND STRING,
PACKAGE_TYPE STRING,
TXN_TYPE_IND STRING,
MDM_CUSTOMER_KEY INT64,
JOB_RUN_ID INT64,
SOURCE_SYSTEM_CD STRING,
CREATED_DT DATE,
CREATE_BY STRING,
SELLING_LOCATION_ID INT64,
REVENUE_LOCATION_ID INT64,
COMPENSATION_EMPLOYEE_ID INT64,
PROCESSED_EMPLOYEE_ID STRING,
MD5_VALUE BYTES
)