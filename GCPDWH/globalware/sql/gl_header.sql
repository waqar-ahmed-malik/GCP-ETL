
MERGE OPERATIONAL.GLOBALWARE_GL_HEADER A
USING LANDING.WORK_GW_GL_HEADER  B
ON A.CTRL = SAFE_CAST(B.CTRL AS INT64)
WHEN MATCHED THEN
UPDATE SET
A.GL_SOURCE = B.GL_SOURCE,
A.POST_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.POST_DT,0,10) ),
A.CASH_ACCT = B.CASH_ACCT,
A.GL_COMMENTS = B.GL_COMMENTS
WHEN NOT MATCHED THEN
  INSERT (
CTRL,
GL_SOURCE,
POST_DT,
CASH_ACCT,
GL_COMMENTS,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
LAST_UPDATE_DTTIME
)
VALUES(
SAFE_CAST(CTRL AS INT64),
GL_SOURCE,
PARSE_DATE("%Y-%m-%d",SUBSTR(POST_DT,0,10)),
CASH_ACCT,
GL_COMMENTS,
"jobrunid",
'GLOBALWARE' ,
CURRENT_DATETIME());
