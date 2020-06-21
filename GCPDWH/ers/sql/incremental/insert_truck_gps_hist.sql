SELECT TO_CHAR(TRUCK_GPS_HIST.SVC_FACL_ID) SVC_FACL_ID,
TO_CHAR(TRUCK_GPS_HIST.TRK_ID) TRK_ID, 
TO_CHAR(TRUCK_GPS_HIST.TRK_DRIVR_ID) TRK_DRIVR_ID, 
TO_CHAR(TRUCK_GPS_HIST.NEW_LAT) NEW_LAT, 
TO_CHAR(TRUCK_GPS_HIST.NEW_LONG) NEW_LONG, 
TO_CHAR (TRUCK_GPS_HIST.UPDAT_DT,'YYYY-MM-DD HH24:MI:SS') AS UPDAT_DT,
TO_CHAR(TRUCK_GPS_HIST.TRK_STS_CD) TRK_STS_CD, 
TO_CHAR(TRUCK_GPS_HIST.TRK_DRIVR_STS_CD) TRK_DRIVR_STS_CD, 
TO_CHAR(TRUCK_GPS_HIST.SPEED) SPEED, 
TO_CHAR(TRUCK_GPS_HIST.CHECK_ENGINE) CHECK_ENGINE ,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM
 NCA_ERS_HIST.TRUCK_GPS_HIST TRUCK_GPS_HIST
 WHERE UPDAT_DT>=to_date('incr_date_gpshist','yyyy-mm-dd hh24:mi:ss')