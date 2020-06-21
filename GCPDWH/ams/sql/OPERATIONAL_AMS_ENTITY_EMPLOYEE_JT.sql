MERGE `OPERATIONAL.AMS_ENTITY_EMPLOYEE_JT` AS TGT
USING `LANDING.WORK_AMS_ENTITY_EMPLOYEE_JT` AS SRC
ON TGT.UNIQUE_ENTITY = SAFE_CAST(SRC.UNIQENTITY AS INT64)
AND TGT.UNIQUE_CD_SERVICING_ROLE = SAFE_CAST(SRC.UNIQCDSERVICINGROLE AS INT64)

WHEN MATCHED
THEN UPDATE SET
TGT.UNIQUE_ENTITY = SAFE_CAST(SRC.UNIQENTITY AS INT64)
,TGT.UNIQUE_CD_SERVICING_ROLE = SAFE_CAST(SRC.UNIQCDSERVICINGROLE AS INT64)
,TGT.UNIQUE_EMPLOYEE = SAFE_CAST(SRC.UNIQEMPLOYEE AS INT64)
,TGT.ROLE_DESC = SAFE_CAST(SRC.ROLEDESCRIPTION AS STRING)
,TGT.UPDATED_BY_PROCESS = SAFE_CAST(SRC.UPDATEDBYPROCESS AS STRING)
,TGT.INSERTED_BY_PROCESS = SAFE_CAST(SRC.INSERTEDBYPROCESS AS STRING)
,TGT.JOB_RUN_ID = SAFE_CAST(SRC.JOBRUNID AS STRING)
--,TGT.CREATE_DTTIME = CURRENT_DATETIME()
,TGT.UPDATE_DTTIME = CURRENT_DATETIME()
--,TGT.SOURCE_SYSTEM_CD = 'AMS'

WHEN NOT MATCHED
THEN INSERT ( --Target column names
UNIQUE_ENTITY
,UNIQUE_CD_SERVICING_ROLE
,UNIQUE_EMPLOYEE
,ROLE_DESC
,UPDATED_BY_PROCESS
,INSERTED_BY_PROCESS
,JOB_RUN_ID
,CREATE_DTTIME
,UPDATE_DTTIME
,SOURCE_SYSTEM_CD
)

VALUES ( --Source values
SAFE_CAST(UNIQENTITY AS INT64)
,SAFE_CAST(UNIQCDSERVICINGROLE AS INT64)
,SAFE_CAST(UNIQEMPLOYEE AS INT64)
,SAFE_CAST(ROLEDESCRIPTION AS STRING)
,SAFE_CAST(UPDATEDBYPROCESS AS STRING)
,SAFE_CAST(INSERTEDBYPROCESS AS STRING)
,SAFE_CAST(JOBRUNID AS STRING)
,CURRENT_DATETIME()
,CURRENT_DATETIME()
,'AMS'
)
