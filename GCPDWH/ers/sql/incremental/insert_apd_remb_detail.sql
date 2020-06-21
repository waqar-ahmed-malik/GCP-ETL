SELECT
TO_CHAR(translate(RD.COMM_CTR_ID,chr(10)||chr(11)||chr(13),' ')) as COMM_CTR_ID,
TO_CHAR(RD.SC_DT,'YYYY-MM-DD') as SC_DT ,
TO_CHAR(translate(RD.SC_ID,chr(10)||chr(11)||chr(13),' ')) as SC_ID,
TO_CHAR(translate(RD.ACCOUNT,chr(10)||chr(11)||chr(13),' ')) as ACCOUNT,
TO_CHAR(translate(RD.APPROVED_AMOUNT,chr(10)||chr(11)||chr(13),' ')) as APPROVED_AMOUNT,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_APD.REMB_DETAIL RD,
NCA_APD.REIMBURSEMENT R
WHERE R.COMM_CTR_ID = RD.COMM_CTR_ID
AND R.SC_DT = RD.SC_DT
AND R.SC_ID = RD.SC_ID
AND (R.RECEIVED_DATE >= TO_DATE('incr_date_reim','yyyy-mm-dd hh24:mi:ss')
OR R.FOLLOWUP_DATE >= TO_DATE('incr_date_reim','yyyy-mm-dd hh24:mi:ss')
OR R.APPROVED_DATE >= TO_DATE('incr_date_reim','yyyy-mm-dd hh24:mi:ss')
OR R.PAY_DATE >= TO_DATE('incr_date_reim','yyyy-mm-dd hh24:mi:ss')) 
