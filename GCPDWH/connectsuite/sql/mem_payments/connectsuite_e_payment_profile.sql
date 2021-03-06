CREATE OR REPLACE TABLE OPERATIONAL.CONNECTSUITE_E_PAYMENT_PROFILE
AS (SELECT
CAST(E_PMT_KY AS INT64) AS E_PMT_KY,
CC_TYPE_CD,
ACCOUNT_NUMBER,
CAST(CC_EXPIRE_DATE AS DATETIME) AS CC_EXPIRE_DATE,
CAST(REJECT_DATE AS DATETIME) AS REJECT_DATE,
REJECT_REASON_CODE,
OWNER_INDICATOR,
CAST(OWNER_KY AS INT64) AS OWNER_KY,
CAST(CANCEL_DATE AS DATETIME) AS CANCEL_DATE,
CAST(ENCRYPTION_KEY_ID AS INT64) AS ENCRYPTION_KEY_ID,
CONTRACT_ID,
E_PMT_METHOD,
PRIOR_ACCOUNT_NUMBER,
ACCOUNT_UPDATE_RESPONSE_CODE,
CAST(ACCOUNT_UPDATE_DATE AS DATETIME) AS ACCOUNT_UPDATE_DATE,
NEEDS_PRE_AUTH,
CAST(ACCOUNT_UPDATE_SENT_DATE AS DATETIME) AS ACCOUNT_UPDATE_SENT_DATE,
"jobrunid" as JOB_RUN_ID,
'CONNECT SUITE' as SOURCE_SYSTEM_CD,
CURRENT_DATETIME() as CREATE_DT
from
LANDING.WORK_CS_E_PAYMENT_PROFILE)