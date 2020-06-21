MERGE INTO OPERATIONAL.CONNECTSUITE_CARD_REQUEST_HISTORY M
USING LANDING.WORK_CS_CARD_REQUEST_HISTORY W
ON M.CRH_KEY =SAFE_CAST(W.CRH_KEY AS INT64)
WHEN NOT MATCHED THEN
INSERT
(
CRH_KEY,
MEMBERSHIP_KEY,
MEMBER_KEY,
REQUEST_DTTIME,
ISSUE_DTTIME,
REQUEST_REASON,
MODIFIED_DTTIME,
MODIFIED_BY,
STATUS,
REQUEST_TYPE_CD,
SKIP_DT,
ORIGIN,
ETL_JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DTTIME,
CREATED_BY,
UPDATE_DTTIME
)
VALUES
(
SAFE_CAST(CRH_KEY AS INT64),
SAFE_CAST(MEMBERSHIP_KEY AS INT64),
SAFE_CAST(MEMBER_KEY AS INT64),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',REQUEST_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',ISSUE_DT),
REQUEST_REASON,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',MODIFIED_DT),
MODIFIED_BY,
STATUS,
REQUEST_TYPE_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SKIP_DATE),
ORIGIN,
CAST('jobrunid' AS INT64),
'CONNECT SUITE',
CURRENT_DATETIME(),
'jobname',
CURRENT_DATETIME()
)
WHEN MATCHED THEN 
UPDATE
SET
MEMBERSHIP_KEY = SAFE_CAST(W.MEMBERSHIP_KEY AS INT64),
MEMBER_KEY = SAFE_CAST(W.MEMBER_KEY AS INT64),
REQUEST_DTTIME = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',W.REQUEST_DT),
ISSUE_DTTIME = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',W.ISSUE_DT),
REQUEST_REASON = W.REQUEST_REASON,
MODIFIED_DTTIME =PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',W.MODIFIED_DT),
MODIFIED_BY = W.MODIFIED_BY,
STATUS = W.STATUS,
REQUEST_TYPE_CD = W.REQUEST_TYPE_CD,
SKIP_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',W.SKIP_DATE),
ORIGIN = W.ORIGIN,
ETL_JOB_RUN_ID = CAST('jobrunid' AS INT64),
SOURCE_SYSTEM_CD = 'CONNECT SUITE',
CREATED_BY = 'jobname',
UPDATE_DTTIME = CURRENT_DATETIME()