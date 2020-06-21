SELECT
TO_CHAR(INSERT_TIME,'YYYY-MM-DD HH24:MI:SS') as INSERT_TIME ,
TO_CHAR(LOG_TIME,'YYYY-MM-DD HH24:MI:SS') as LOG_TIME ,
TO_CHAR(translate(SESSION_ID,chr(10)||chr(11)||chr(13),' ')) as SESSION_ID,
TO_CHAR(translate(IP_ADDRESS,chr(10)||chr(11)||chr(13),' ')) as IP_ADDRESS,
TO_CHAR(translate(FACILITY,chr(10)||chr(11)||chr(13),' ')) as FACILITY,
TO_CHAR(translate(TRUCK,chr(10)||chr(11)||chr(13),' ')) as TRUCK,
TO_CHAR(translate(DRIVER,chr(10)||chr(11)||chr(13),' ')) as DRIVER,
TO_CHAR(translate(DEVICE_TYPE,chr(10)||chr(11)||chr(13),' ')) as DEVICE_TYPE,
TO_CHAR(translate(DEVICE_VERSION,chr(10)||chr(11)||chr(13),' ')) as DEVICE_VERSION,
TO_CHAR(translate(STAT_NAME,chr(10)||chr(11)||chr(13),' ')) as STAT_NAME,
TO_CHAR(translate(STAT_VALUE,chr(10)||chr(11)||chr(13),' ')) as STAT_VALUE,
TO_CHAR(translate(OS_VERSION,chr(10)||chr(11)||chr(13),' ')) as OS_VERSION,
TO_CHAR(translate(MODEL_NAME,chr(10)||chr(11)||chr(13),' ')) as MODEL_NAME,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD	
FROM NCA_CAD.TRUCK_SESSION_STATS
WHERE INSERT_TIME  >=  TO_DATE('incr_date_session_stats','yyyy-mm-dd hh24:mi:ss')