
MERGE OPERATIONAL.GLOBALWARE_CASH_RECIEPTS A
USING LANDING.WORK_GW_CASH_RECIPETS  B
ON A.RECIEPT_NUM = SAFE_CAST(B.RECIEPT_NUM AS INT64)
WHEN MATCHED THEN
UPDATE SET
A.CTRL = SAFE_CAST(B.CTRL AS INT64),
A.POST_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.POST_DT,0,10) ),
A.CASH_ACCOUNT = B.CASH_ACCOUNT,
A.ACCOUNT_ID = B.ACCOUNT_ID,
A.CASH_RECIPETS_FROM = B.CASH_RECIPETS_FROM,
A.CHECK_NUM = B.CHECK_NUM,
A.AMT = SAFE_CAST(B.AMT AS FLOAT64),
A.APPLIED = SAFE_CAST(B.APPLIED AS INT64),
A.BRANCH = B.BRANCH,
A.PREV_BILLED = B.PREV_BILLED,
A.TYPE = B.TYPE,
A.REMAINING = SAFE_CAST(B.REMAINING AS FLOAT64),
A.IN_USE_FLG = SAFE_CAST(B.IN_USE_FLG AS INT64),
A.STATUS = B.STATUS




WHEN NOT MATCHED THEN
  INSERT (
CTRL,
POST_DT,
CASH_ACCOUNT,
ACCOUNT_ID,
CASH_RECIPETS_FROM,
CHECK_NUM,
RECIEPT_NUM,
AMT,
APPLIED,
BRANCH,
PREV_BILLED,
TYPE,
REMAINING,
IN_USE_FLG,
STATUS,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
LAST_UPDATE_DTTIME
)
VALUES(
SAFE_CAST(CTRL AS INT64),
PARSE_DATE("%Y-%m-%d",SUBSTR(POST_DT ,0,10) ),
CASH_ACCOUNT,
ACCOUNT_ID,
CASH_RECIPETS_FROM,
CHECK_NUM,
SAFE_CAST(RECIEPT_NUM AS INT64),
SAFE_CAST(AMT AS FLOAT64),
SAFE_CAST(APPLIED AS INT64),
BRANCH,
PREV_BILLED,
TYPE,
SAFE_CAST(REMAINING AS FLOAT64),
SAFE_CAST(IN_USE_FLG AS INT64),
STATUS,
"jobrunid",
'GLOBALWARE' ,
CURRENT_DATETIME());