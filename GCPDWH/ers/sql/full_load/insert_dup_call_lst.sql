SELECT
TO_CHAR(translate(COMM_CTR_ID,chr(10)||chr(11)||chr(13),' ')) as COMM_CTR_ID,
TO_CHAR(ORG_SC_DT,'YYYY-MM-DD') as ORG_SC_DT ,
TO_CHAR(translate(ORG_SC_ID,chr(10)||chr(11)||chr(13),' ')) as ORG_SC_ID,
TO_CHAR(DUP_SC_DT,'YYYY-MM-DD') as DUP_SC_DT ,
TO_CHAR(translate(DUP_SC_ID,chr(10)||chr(11)||chr(13),' ')) as DUP_SC_ID,
TO_CHAR(translate(DUPLICATION_REASON_CODE,chr(10)||chr(11)||chr(13),' ')) as DUPLICATION_REASON_CODE,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_ERS_HIST.DUP_CALL_LST