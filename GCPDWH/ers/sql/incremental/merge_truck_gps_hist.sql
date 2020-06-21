-- UPDATE(MERGE)

--MERGE <target_table> [AS TARGET]
--USING <table_source> [AS SOURCE]
--ON <search_condition>
--[WHEN MATCHED 
--THEN <merge_matched> ]
--[WHEN NOT MATCHED [BY TARGET]
--THEN <merge_not_matched> ]
--[WHEN NOT MATCHED BY SOURCE
--THEN <merge_matched> ];
  
--ERS_STAGE_TRUCK_GPS_HIST
--WORK_ERS_STG_TRUCK_GPS_HIST

MERGE OPERATIONAL.ERS_STAGE_TRUCK_GPS_HIST AS TGT
USING LANDING.WORK_ERS_STG_TRUCK_GPS_HIST AS SRC
ON TGT.UPDAT_DT = SAFE_CAST(SRC.UPDAT_DT AS DATETIME) AND
TGT.SVC_FACL_ID = SRC.SVC_FACL_ID AND
TGT.TRK_ID = SRC.TRK_ID
WHEN MATCHED
THEN UPDATE SET
TGT.TRK_DRIVR_ID = CAST(SRC.TRK_DRIVR_ID AS INT64), 
TGT.NEW_LAT = SAFE_CAST(SRC.NEW_LAT AS FLOAT64) , 
TGT.NEW_LONG = SAFE_CAST(SRC.NEW_LONG AS FLOAT64) ,  
TGT.TRK_STS_CD = SRC.TRK_STS_CD , 
TGT.TRK_DRIVR_STS_CD = SRC.TRK_DRIVR_STS_CD , 
TGT.SPEED = SAFE_CAST(SRC.SPEED AS INT64), 
TGT.CHECK_ENGINE = SRC.CHECK_ENGINE , 
TGT.ETL_JOB_RUN_ID = SAFE_CAST(SRC.JOB_RUN_ID AS INT64) , 
TGT.SOURCE_SYSTEM_CD = SRC.SOURCE_SYSTEM_CD ,  
TGT.CREATE_DTTIME = current_datetime() ,
TGT.CREATE_BY = SRC.CREATED_BY 

WHEN NOT MATCHED THEN INSERT 
(
SVC_FACL_ID , 
TRK_ID , 
TRK_DRIVR_ID , 
NEW_LAT , 
NEW_LONG , 
UPDAT_DT , 
TRK_STS_CD , 
TRK_DRIVR_STS_CD , 
SPEED , 
CHECK_ENGINE , 
ETL_JOB_RUN_ID , 
SOURCE_SYSTEM_CD , 
CREATE_DTTIME , 
CREATE_BY )

VALUES

(
SRC.SVC_FACL_ID , 
SRC.TRK_ID , 
SAFE_CAST(SRC.TRK_DRIVR_ID AS INT64), 
SAFE_CAST(SRC.NEW_LAT AS FLOAT64) , 
SAFE_CAST(SRC.NEW_LONG AS FLOAT64) , 
SAFE_CAST(SRC.UPDAT_DT AS DATETIME) , 
SRC.TRK_STS_CD , 
SRC.TRK_DRIVR_STS_CD , 
SAFE_CAST(SRC.SPEED AS INT64), 
SRC.CHECK_ENGINE , 
SAFE_CAST(SRC.JOB_RUN_ID AS INT64) , 
SRC.SOURCE_SYSTEM_CD , 
current_datetime(),
SRC.CREATED_BY 
)