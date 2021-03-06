INSERT INTO `CUSTOMER_PRODUCT.QUOTES_DIM` (
QUOTE_NUM,
POLICY_NUM,
CREATE_DT ,
UPDATE_DT,
EFFECTIVE_DT,
QUOTE_ID,
OPPORTUNITY_ID,
CUSTOMER_ID,
SOURCE_CUSTOMER_ID,
POLICY_STATUS,
PRODUCT_TYPE,
PRODUCT_CD,
POLICY_STATE,
PREMIUM,
POLICY_TERM_MONTHS,
POLICY_CARRIER,
SOURCE_SYSTEM,
CURRENT_IMAGE_IND,
AGE_OF_QUOTE,
QUOTE_VERSION_AGE,
QUOTE_COUNT,
UPLOAD_COUNT,
QUOTE_STATUS,
TRANSACTION_TYPE,
BINDING_AGENT,
AGENT_OF_RECORD,
PRODUCER_EMAIL,
EMPLOYEE_ID,
CHANNEL_TYPE,
AGENCY_NM,
SALES_CHANNEL,
AGENCY_LOCATION_NM,
AGENCY_LOCATION_ID,
LEAD_SOURCE,
QUOTE_TYPE,
CONVERSION_DT,
IMPORTED_FLAG,
PRIOR_POLICY_NUM,
PRIOR_INSURANCE_COMPANY,
PRIOR_CARRIER_BI_LIMIT,
DAYS_LAPSED,
HOME_OWNER_TYPE,
ASSOCIATION,
MEMBERSHIP_NUM,
TIER,
AGENT_NOTE,
UW_NOTE,
LICENSE_STATUS_INFORMATION,
CLUE_INFORMATION,
VEHICLE_USAGE,
OTHER_NOTES,
FR_LEVEL,
GENDER,
TOLL_FREE_NUM,
INSERT_DT,
INSERT_BY,
LAST_UPDATED_AGENT,
LAST_UPDATED_DT,
VOICE_BIND,
PACKAGE_TYPE,
TXN_TYPE_IND,
MDM_CUSTOMER_KEY,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATED_DT,
CREATE_BY,
SELLING_LOCATION_ID,
REVENUE_LOCATION_ID,
COMPENSATION_EMPLOYEE_ID,
PROCESSED_EMPLOYEE_ID,
HASH_VALUE,
ROW_START_DT,
ROW_END_DT)
SELECT 
QUOTE_NUM,
POLICY_NUM,
CREATE_DT ,
UPDATE_DT,
EFFECTIVE_DT,
QUOTE_ID,
OPPORTUNITY_ID,
CUSTOMER_ID,
SOURCE_CUSTOMER_ID,
POLICY_STATUS,
PRODUCT_TYPE,
PRODUCT_CD,
POLICY_STATE,
PREMIUM,
POLICY_TERM_MONTHS,
POLICY_CARRIER,
SOURCE_SYSTEM,
CURRENT_IMAGE_IND,
AGE_OF_QUOTE,
QUOTE_VERSION_AGE,
QUOTE_COUNT,
UPLOAD_COUNT,
QUOTE_STATUS,
TRANSACTION_TYPE,
BINDING_AGENT,
AGENT_OF_RECORD,
PRODUCER_EMAIL,
EMPLOYEE_ID,
CHANNEL_TYPE,
AGENCY_NM,
SALES_CHANNEL,
AGENCY_LOCATION_NM,
AGENCY_LOCATION_ID,
LEAD_SOURCE,
QUOTE_TYPE,
CONVERSION_DT,
IMPORTED_FLAG,
PRIOR_POLICY_NUM,
PRIOR_INSURANCE_COMPANY,
PRIOR_CARRIER_BI_LIMIT,
DAYS_LAPSED,
HOME_OWNER_TYPE,
ASSOCIATION,
MEMBERSHIP_NUM,
TIER,
AGENT_NOTE,
UW_NOTE,
LICENSE_STATUS_INFORMATION,
CLUE_INFORMATION,
VEHICLE_USAGE,
OTHER_NOTES,
FR_LEVEL,
GENDER,
TOLL_FREE_NUM,
INSERT_DT,
INSERT_BY,
LAST_UPDATED_AGENT,
LAST_UPDATED_DT,
VOICE_BIND,
PACKAGE_TYPE,
TXN_TYPE_IND,
MDM_CUSTOMER_KEY,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DT,
CREATE_BY,
SELLING_LOCATION_ID,
REVENUE_LOCATION_ID,
COMPENSATION_EMPLOYEE_ID,
PROCESSED_EMPLOYEE_ID,
MD5_VALUE,
EFF_START_DT,
PARSE_DATE("%Y-%m-%d",'9999-12-31')
from `LANDING.WORK_QUOTES_DIM` S 
WHERE S.QUOTE_NUM NOT IN (SELECT QUOTE_NUM FROM CUSTOMER_PRODUCT.QUOTES_DIM WHERE ROW_END_DT ='9999-12-31')
OR(
S.QUOTE_NUM IN (SELECT QUOTE_NUM FROM CUSTOMER_PRODUCT.QUOTES_DIM DIM where DIM.ROW_END_DT ='9999-12-31')
AND S.MD5_VALUE NOT IN ( SELECT HASH_VALUE FROM CUSTOMER_PRODUCT.QUOTES_DIM WHERE ROW_END_DT ='9999-12-31')
)