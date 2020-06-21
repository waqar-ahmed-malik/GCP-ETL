SELECT
TO_CHAR(translate(MODULE_ID,chr(10)||chr(11)||chr(13),' ')) as MODULE_ID,
TO_CHAR(translate(FUNCTION_ID,chr(10)||chr(11)||chr(13),' ')) as FUNCTION_ID,
TO_CHAR(translate(EMPLOYEE_ID,chr(10)||chr(11)||chr(13),' ')) as EMPLOYEE_ID,
TO_CHAR(TIMESTAMP_DT,'YYYY-MM-DD') as TIMESTAMP_DT ,
TO_CHAR(translate(MACHINE_NM_TX,chr(10)||chr(11)||chr(13),' ')) as MACHINE_NM_TX,
TO_CHAR(translate(TRANSACTION_CD,chr(10)||chr(11)||chr(13),' ')) as TRANSACTION_CD,
TO_CHAR(translate(KEY1_DA,chr(10)||chr(11)||chr(13),' ')) as KEY1_DA,
TO_CHAR(translate(KEY2_DA,chr(10)||chr(11)||chr(13),' ')) as KEY2_DA,
TO_CHAR(translate(KEY3_DA,chr(10)||chr(11)||chr(13),' ')) as KEY3_DA,
TO_CHAR(translate(TRANS_DTL_TX,chr(10)||chr(11)||chr(13),' ')) as TRANS_DTL_TX,
TO_CHAR(translate(TRANSACTION_ID,chr(10)||chr(11)||chr(13),' ')) as TRANSACTION_ID,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_APD.D2K_AUDIT
WHERE TIMESTAMP_DT  >=  TO_DATE('incr_date_d2k_audit','yyyy-mm-dd hh24:mi:ss')