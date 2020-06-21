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
  
--ERS_STAGE_MILEAGE_HIST
--WORK_ERS_STG_MILEAGE_HIST

MERGE OPERATIONAL.ERS_STAGE_MILEAGE_HIST AS TGT
USING LANDING.WORK_ERS_STG_MILEAGE_HIST AS SRC
ON TGT.COMM_CTR_ID = SRC.COMM_CTR_ID and
CAST(TGT.SC_DT AS STRING) = SRC.SC_DT and
CAST(TGT.SC_ID AS STRING) = SRC.SC_ID
WHEN MATCHED
THEN UPDATE SET
TGT.MILES_ER_DISP = CAST(SRC.MILES_ER_DISP AS FLOAT64) , 
TGT.MILES_TW_DISP = CAST(SRC.MILES_TW_DISP AS FLOAT64), 
TGT.MILES_ER_CALC = CAST(SRC.MILES_ER_CALC AS FLOAT64) , 
TGT.MILES_TW_CALC = CAST(SRC.MILES_TW_CALC AS FLOAT64) , 
TGT.MILES_ER_CPMS = CAST(SRC.MILES_ER_CPMS AS FLOAT64), 
TGT.MILES_TW_CPMS = CAST(SRC.MILES_TW_CPMS AS FLOAT64) , 
TGT.ADJUSTED_ER = SRC.ADJUSTED_ER , 
TGT.ADJUSTED_TW = SRC.ADJUSTED_TW , 
TGT.ER_TOLERANCE = CAST(SRC.ER_TOLERANCE AS INT64) , 
TGT.TW_TOLERANCE =CAST( SRC.TW_TOLERANCE AS INT64) , 
TGT.MILEAGE_COMMENT = SRC.MILEAGE_COMMENT , 
TGT.ER_LAT = CAST(SRC.ER_LAT AS FLOAT64), 
TGT.ER_LONG = CAST(SRC.ER_LONG AS FLOAT64), 
TGT.MILEAGE_CALC_TYPE = SRC.MILEAGE_CALC_TYPE , 
TGT.ETL_JOB_RUN_ID = CAST(SRC.JOB_RUN_ID AS INT64), 
TGT.SOURCE_SYSTEM_CD = SRC.SOURCE_SYSTEM_CD ,  
TGT.CREATE_DTTIME = current_datetime() ,
TGT.CREATE_BY = SRC.CREATED_BY 
WHEN NOT MATCHED THEN INSERT 
(
COMM_CTR_ID , 
SC_DT , 
SC_ID , 
MILES_ER_DISP , 
MILES_TW_DISP , 
MILES_ER_CALC , 
MILES_TW_CALC , 
MILES_ER_CPMS , 
MILES_TW_CPMS , 
ADJUSTED_ER , 
ADJUSTED_TW , 
ER_TOLERANCE , 
TW_TOLERANCE , 
MILEAGE_COMMENT , 
ER_LAT , 
ER_LONG , 
MILEAGE_CALC_TYPE , 
ETL_JOB_RUN_ID , 
SOURCE_SYSTEM_CD , 
CREATE_DTTIME , 
CREATE_BY  )
VALUES
(
SRC.COMM_CTR_ID , 
SAFE_CAST(SRC.SC_DT AS DATETIME), 
SAFE_CAST(SRC.SC_ID AS INT64), 
SAFE_CAST(SRC.MILES_ER_DISP AS FLOAT64) , 
SAFE_CAST(SRC.MILES_TW_DISP AS FLOAT64) , 
SAFE_CAST(SRC.MILES_ER_CALC AS FLOAT64) , 
SAFE_CAST(SRC.MILES_TW_CALC AS FLOAT64) , 
SAFE_CAST(SRC.MILES_ER_CPMS AS FLOAT64) , 
SAFE_CAST(SRC.MILES_TW_CPMS AS FLOAT64) , 
SRC.ADJUSTED_ER , 
SRC.ADJUSTED_TW , 
SAFE_CAST(SRC.ER_TOLERANCE AS INT64) , 
SAFE_CAST(SRC.TW_TOLERANCE AS INT64), 
SRC.MILEAGE_COMMENT , 
SAFE_CAST(SRC.ER_LAT AS FLOAT64) , 
SAFE_CAST(SRC.ER_LONG AS FLOAT64) , 
SRC.MILEAGE_CALC_TYPE , 
SAFE_CAST(SRC.JOB_RUN_ID AS INT64) , 
SRC.SOURCE_SYSTEM_CD , 
current_datetime(),
SRC.CREATED_BY 
)