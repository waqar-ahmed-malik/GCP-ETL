 SELECT 
TO_CHAR (STAT_GRP.START_DTM,'YYYY-MM-DD HH24:MI:SS') AS START_DTM,
TO_CHAR (STAT_GRP.END_DTM,'YYYY-MM-DD HH24:MI:SS') AS END_DTM,
TO_CHAR(STAT_GRP.GROUP_ID) GROUP_ID, 
TO_CHAR(STAT_GRP.FACIL_INT_ID) FACIL_INT_ID, 
TO_CHAR(STAT_GRP.CATEGORY) CATEGORY, 
TO_CHAR(STAT_GRP.ACCT) ACCT, 
TO_CHAR(STAT_GRP.ACCT_NAME) ACCT_NAME, 
TO_CHAR(STAT_GRP.NUM_CALLS) NUM_CALLS, 
TO_CHAR(STAT_GRP.AMOUNT) AMOUNT, 
TO_CHAR(STAT_GRP.CPC) CPC, 
TO_CHAR(STAT_GRP.F1099) F1099,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM
 NCA_APD.STAT_GRP STAT_GRP WHERE START_DTM > TO_DATE('facil_id','yyyy-mm-dd hh24:mi:ss')
