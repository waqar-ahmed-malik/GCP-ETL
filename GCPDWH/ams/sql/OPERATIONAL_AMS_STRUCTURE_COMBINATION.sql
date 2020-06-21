MERGE `OPERATIONAL.AMS_STRUCTURE_COMBINATION` AS TGT
USING `LANDING.WORK_AMS_STRUCTURE_COMBINATION` AS SRC
ON TGT.UNIQUE_STRUCTURE = SAFE_CAST(SRC.UNIQSTRUCTURE AS INT64)

WHEN MATCHED
THEN UPDATE SET
TGT.UNIQUE_STRUCTURE = SAFE_CAST(SRC.UNIQSTRUCTURE AS INT64)
,TGT.UNIQUE_AGENCY = SAFE_CAST(SRC.UNIQAGENCY AS INT64)
,TGT.UNIQUE_BRANCH = SAFE_CAST(SRC.UNIQBRANCH AS INT64)
,TGT.UNIQUE_DEPARTMENT = SAFE_CAST(SRC.UNIQDEPARTMENT AS INT64)
,TGT.UNIQUE_PROFIT_CENTER = SAFE_CAST(SRC.UNIQPROFITCENTER AS INT64)
,TGT.FLGS = SAFE_CAST(SRC.FLAGS AS INT64)
,TGT.INSERTED_BY_CD = SAFE_CAST(SRC.INSERTEDBYCODE AS STRING)
,TGT.INSERTED_DT = SAFE_CAST(SRC.INSERTEDDATE AS DATETIME)
,TGT.UPDATED_BY_CD = SAFE_CAST(SRC.UPDATEDBYCODE AS STRING)
,TGT.UPDATED_DT = SAFE_CAST(SRC.UPDATEDDATE AS DATETIME)
,TGT.JOB_RUN_ID = SAFE_CAST(SRC.JOBRUNID AS STRING)
--,TGT.CREATE_DTTIME = CURRENT_DATETIME()
,TGT.UPDATE_DTTIME = CURRENT_DATETIME()
--,TGT.SOURCE_SYSTEM_CD = 'AMS'

WHEN NOT MATCHED
THEN INSERT ( --Target column names
UNIQUE_STRUCTURE
,UNIQUE_AGENCY
,UNIQUE_BRANCH
,UNIQUE_DEPARTMENT
,UNIQUE_PROFIT_CENTER
,FLGS
,INSERTED_BY_CD
,INSERTED_DT
,UPDATED_BY_CD
,UPDATED_DT
,JOB_RUN_ID
,CREATE_DTTIME
,UPDATE_DTTIME
,SOURCE_SYSTEM_CD
)

VALUES ( --Source values
SAFE_CAST(UNIQSTRUCTURE AS INT64)
,SAFE_CAST(UNIQAGENCY AS INT64)
,SAFE_CAST(UNIQBRANCH AS INT64)
,SAFE_CAST(UNIQDEPARTMENT AS INT64)
,SAFE_CAST(UNIQPROFITCENTER AS INT64)
,SAFE_CAST(FLAGS AS INT64)
,SAFE_CAST(INSERTEDBYCODE AS STRING)
,SAFE_CAST(INSERTEDDATE AS DATETIME)
,SAFE_CAST(UPDATEDBYCODE AS STRING)
,SAFE_CAST(UPDATEDDATE AS DATETIME)
,SAFE_CAST(JOBRUNID AS STRING)
,CURRENT_DATETIME()
,CURRENT_DATETIME()
,'AMS'
)