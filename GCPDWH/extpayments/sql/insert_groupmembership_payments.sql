INSERT INTO OPERATIONAL.EXTERNAL_GROUPMEMBERSHIP_PAYMENTS
(MEMBERSHIP,
ACCOUNT_NUM,
LAST_NM,
FIRST_NM,
ROLE,
TYPE,
PRICE,
GROUP_NM,
REC_DT,
SUB_TOTAL,
GRAND_TOTAL,
SOURCE_FILE_NM,
SOURCE_FILE_DT,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DTTIME
)
SELECT
TRIM(MEMBERSHIP),
TRIM(ACCOUNT_NUM),
TRIM(LAST_NM),
TRIM(FIRST_NM),
TRIM(ROLE),
TRIM(TYPE),
TRIM(PRICE),
TRIM(GROUP_NM),
PARSE_DATE("%m/%d/%Y",TRIM(REC_DT)),
TRIM(SUB_TOTAL),
TRIM(GRAND_TOTAL),
'v_file_name',
PARSE_DATE("%Y-%m-%d",'v_file_date'),
SAFE_CAST('v_job_run_id' AS INT64),
'v_job_name',
CURRENT_DATETIME()
FROM LANDING.WORK_EXTERNAL_GROUPMEMBERSHIP_PAYMENTS