SELECT 
TO_CHAR(SERVICE_TRUCK.SVC_FACL_ID) SVC_FACL_ID,
TO_CHAR(translate(SERVICE_TRUCK.TRK_ID, chr(10)||chr(11)||chr(13),' ')) TRK_ID,
TO_CHAR(SERVICE_TRUCK.TRK_DRIVR_ID) TRK_DRIVR_ID, 
TO_CHAR(SERVICE_TRUCK.TRK_CMNCT_TYP_CD) TRK_CMNCT_TYP_CD, 
TO_CHAR(SERVICE_TRUCK.TRK_EST_AVAIL_TM,'YYYY-MM-DD HH24:MI:SS') AS TRK_EST_AVAIL_TM,
TO_CHAR(SERVICE_TRUCK.TRK_DRIVR_S_END_TM,'YYYY-MM-DD HH24:MI:SS') AS TRK_DRIVR_S_END_TM,
TO_CHAR(SERVICE_TRUCK.TRK_TERM) TRK_TERM, 
TO_CHAR(SERVICE_TRUCK.TRK_NETWORK) TRK_NETWORK, 
TO_CHAR(translate(SERVICE_TRUCK.TRK_MDT_ID, chr(10)||chr(11)||chr(13),' ')) TRK_MDT_ID,
TO_CHAR(SERVICE_TRUCK.TRK_STS_CD) TRK_STS_CD, 
TO_CHAR(SERVICE_TRUCK.TRK_DRIVR_STS_CD) TRK_DRIVR_STS_CD, 
TO_CHAR(SERVICE_TRUCK.TRK_STS_TM,'YYYY-MM-DD HH24:MI:SS') AS TRK_STS_TM,
TO_CHAR(SERVICE_TRUCK.STATUS_CHG_TIME,'YYYY-MM-DD HH24:MI:SS') AS STATUS_CHG_TIME,
TO_CHAR(SERVICE_TRUCK.TRK_TYP_CD) TRK_TYP_CD, 
TO_CHAR(translate(SERVICE_TRUCK.TRK_COMM_TX, chr(10)||chr(11)||chr(13),' ')) TRK_COMM_TX,
TO_CHAR(SERVICE_TRUCK.TRK_GRID_CD) TRK_GRID_CD, 
TO_CHAR(SERVICE_TRUCK.TRANSACT) TRANSACT, 
TO_CHAR(SERVICE_TRUCK.CHANNEL) CHANNEL, 
TO_CHAR(SERVICE_TRUCK.TRK_VHBROAD_1) TRK_VHBROAD_1, 
TO_CHAR(SERVICE_TRUCK.TRK_VHBROAD_2) TRK_VHBROAD_2, 
TO_CHAR(SERVICE_TRUCK.TRK_VHBROAD_3) TRK_VHBROAD_3, 
TO_CHAR(SERVICE_TRUCK.TRK_ZONE) TRK_ZONE, 
TO_CHAR(SERVICE_TRUCK.LAT) LAT, 
TO_CHAR(SERVICE_TRUCK.LONGX) LONGX, 
TO_CHAR(SERVICE_TRUCK.ALTITUDE) ALTITUDE, 
TO_CHAR(SERVICE_TRUCK.SPEED) SPEED, 
TO_CHAR(SERVICE_TRUCK.DIRECTION) DIRECTION, 
TO_CHAR(SERVICE_TRUCK.TRK_PRIORITY) TRK_PRIORITY, 
TO_CHAR(SERVICE_TRUCK.TRK_MAIN_SPOT_TYPE) TRK_MAIN_SPOT_TYPE, 
TO_CHAR(SERVICE_TRUCK.TRK_2ND_SPOT_TYPE) TRK_2ND_SPOT_TYPE, 
TO_CHAR(SERVICE_TRUCK.TRK_3RD_SPOT_TYPE) TRK_3RD_SPOT_TYPE, 
TO_CHAR(SERVICE_TRUCK.FLEET_INDIC) FLEET_INDIC, 
TO_CHAR(translate(SERVICE_TRUCK.TRK_STATUS_COMMENT, chr(10)||chr(11)||chr(13),' ')) TRK_STATUS_COMMENT,
TO_CHAR(SERVICE_TRUCK.TRK_RV_INDIC) TRK_RV_INDIC, 
TO_CHAR(SERVICE_TRUCK.TRK_RV_CLASS) TRK_RV_CLASS, 
TO_CHAR(SERVICE_TRUCK.MOTORCYCLE_INDIC) MOTORCYCLE_INDIC, 
TO_CHAR(SERVICE_TRUCK.BATTERY_INDIC) BATTERY_INDIC, 
TO_CHAR(SERVICE_TRUCK.MDT_STS_CD) MDT_STS_CD,
TO_CHAR(SERVICE_TRUCK.TRK_UNAVAIL_MINS) TRK_UNAVAIL_MINS, 
TO_CHAR(SERVICE_TRUCK.DRV_CHG_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS') AS DRV_CHG_TIMESTAMP,
TO_CHAR(SERVICE_TRUCK.POS_ASSOC_CODE) POS_ASSOC_CODE, 
TO_CHAR(SERVICE_TRUCK.POS_CLB_CODE) POS_CLB_CODE, 
TO_CHAR(SERVICE_TRUCK.POS_OFC_ID) POS_OFC_ID, 
TO_CHAR(SERVICE_TRUCK.POS_USER) POS_USER, 
TO_CHAR(SERVICE_TRUCK.POS_PASSWORD) POS_PASSWORD, 
TO_CHAR(SERVICE_TRUCK.POS_LOCATION_CODE) POS_LOCATION_CODE, 
TO_CHAR(SERVICE_TRUCK.TECH_LEVEL) TECH_LEVEL, 
TO_CHAR(SERVICE_TRUCK.SAFE_DRIVING_ON) SAFE_DRIVING_ON, 
TO_CHAR(SERVICE_TRUCK.ONBOARD_PRINTER_INDIC) ONBOARD_PRINTER_INDIC, 
TO_CHAR(SERVICE_TRUCK.LAST_UPDT_TS) LAST_UPDT_TS ,
TO_CHAR(translate(SERVICE_TRUCK.TRK_DRIVER_NOTE, chr(10)||chr(11)||chr(13),' ')) TRK_DRIVER_NOTE ,
TO_CHAR(SERVICE_TRUCK.LAST_GPS_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS') AS LAST_GPS_TIMESTAMP ,
TO_CHAR(translate(SERVICE_TRUCK.LAST_GPS_PROVIDER, chr(10)||chr(11)||chr(13),' ')) LAST_GPS_PROVIDER ,
TO_CHAR(translate(SERVICE_TRUCK.CLOUD_TRK, chr(10)||chr(11)||chr(13),' ')) CLOUD_TRK ,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM
 NCA_CAD.SERVICE_TRUCK SERVICE_TRUCK