SELECT
TO_CHAR(translate(TD_COMM_CTR_ID,chr(10)||chr(11)||chr(13),' ')) as TD_COMM_CTR_ID,
TO_CHAR(TD_SC_DT,'YYYY-MM-DD') as TD_SC_DT ,
TO_CHAR(translate(TD_SC_ID,chr(10)||chr(11)||chr(13),' ')) as TD_SC_ID,
TO_CHAR(translate(TD_TOW_DEST_ID,chr(10)||chr(11)||chr(13),' ')) as TD_TOW_DEST_ID,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_ERS_HIST.AAR_REFERRAL
WHERE TD_SC_DT  >= TO_DATE('incr_date_aaa_referral','yyyy-mm-dd hh24:mi:ss') 
