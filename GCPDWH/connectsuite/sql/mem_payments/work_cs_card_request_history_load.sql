SELECT 
TRIM(CONVERT(CHAR,CRH_KY)) AS CRH_KEY,
TRIM(CONVERT(CHAR,MBRS_KY)) AS MEMBERSHIP_KEY,
TRIM(CONVERT(CHAR,MBR_KY)) AS MEMBER_KEY,
TRIM(CONVERT(CHAR,REQ_DATE,121)) AS REQUEST_DT,
TRIM(CONVERT(CHAR,ISSUE_DATE,121)) AS ISSUE_DT, 
TRIM(CONVERT(CHAR,REQ_REASON)) AS REQUEST_REASON,
TRIM(CONVERT(CHAR,MODIFIED_DATE,121)) AS MODIFIED_DT,
TRIM(CONVERT(CHAR,MODIFIED_BY)) AS MODIFIED_BY,
TRIM(CONVERT(CHAR,STATUS)) AS STATUS, 
TRIM(CONVERT(CHAR,REQ_TYPE_CD)) AS REQUEST_TYPE_CD,
TRIM(CONVERT(CHAR,SKIP_DATE,121)) AS SKIP_DATE,
TRIM(CONVERT(CHAR,ORIGIN)) AS ORIGIN
FROM
 "v_database_name".dbo.CARD_REQUEST_HIST
 WHERE CONVERT(date,REQ_DATE) >= DATEADD(day,-1,convert(date, 'v_inputdate'))
AND CONVERT(date,REQ_DATE) < CONVERT(date,'v_inputdate');
