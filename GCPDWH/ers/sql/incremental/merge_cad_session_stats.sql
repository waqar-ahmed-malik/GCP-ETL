-- INSERT only

 INSERT INTO OPERATIONAL.ERS_STAGE_CAD_TRUCK_SESSION_STATS
(
INSERT_TM,
LOG_TM,
SESSION_ID,
IP_ADDR,
FACILITY,
TRUCK,
DRIVER,
DEVICE_TYPE,
DEVICE_VERSION,
STAT_NM,
STAT_VALUE,
OS_VERSION,
MODEL_NM,
ETL_JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATED_BY,
CREATED_DTTIME)
SELECT
CAST(SRC.INSERT_TIME AS DATETIME),
CAST(SRC.LOG_TIME AS DATETIME),
SRC.SESSION_ID,
SRC.IP_ADDRESS,
SRC.FACILITY,
SRC.TRUCK,
SRC.DRIVER,
SRC.DEVICE_TYPE,
SRC.DEVICE_VERSION,
SRC.STAT_NAME,
SRC.STAT_VALUE,
SRC.OS_VERSION,
SRC.MODEL_NAME,
CAST(SRC.JOB_RUN_ID AS INT64),
SRC.SOURCE_SYSTEM_CD,
SRC.CREATED_BY,
current_datetime()
FROM LANDING.WORK_ERS_STG_CAD_TRUCK_SESSION_STATS SRC
