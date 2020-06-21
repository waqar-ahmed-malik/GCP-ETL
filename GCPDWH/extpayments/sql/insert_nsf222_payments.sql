INSERT INTO OPERATIONAL.EXTERNAL_NSF222_PAYMENTS
(SR_NUM,
RECEIVED_DT,
PROCESSOR_NM,
TYPE_OF_ACTIVITY,
MEMBERSHIP_NUM,
MEMBER_NM,
AMOUNT,
STATUS,
PROCESSED_DT,
SOURCE_FILE_NM,
SOURCE_FILE_DT,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DTTIME
)
SELECT
SAFE_CAST(TRIM(SR_NUM) AS INT64),
PARSE_DATE('%m/%d/%Y',TRIM(RECEIVED_DT)),
TRIM(PROCESSOR_NM),
TRIM(TYPE_OF_ACTIVITY),
TRIM(MEMBERSHIP_NUM),
TRIM(MEMBER_NM),
TRIM(AMOUNT),
TRIM(STATUS),
PARSE_DATE('%m/%d/%Y',TRIM(PROCESSED_DT)),
'v_file_name',
PARSE_DATE("%Y-%m-%d",'v_file_date'),
SAFE_CAST('v_job_run_id' AS INT64),
'v_job_name',
CURRENT_DATETIME()
FROM LANDING.WORK_EXTERNAL_NSF222_PAYMENTS