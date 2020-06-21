MERGE `OPERATIONAL.AMS_CONTACT_NUMBER` AS TGT
USING `LANDING.WORK_AMS_CONTACT_NUMBER` AS SRC
ON TGT.UNIQUE_CONTACT_NUM = SAFE_CAST(SRC.UNIQCONTACTNUMBER AS INT64)

WHEN MATCHED
THEN UPDATE SET
TGT.UNIQUE_CONTACT_NUM = SAFE_CAST(SRC.UNIQCONTACTNUMBER AS INT64)
,TGT.UNIQUE_FIXED_CONTACT_NUM = SAFE_CAST(SRC.UNIQFIXEDCONTACTNUMBER AS INT64)
,TGT.UNIQUE_CONTACT_NAME = SAFE_CAST(SRC.UNIQCONTACTNAME AS INT64)
,TGT.UNIQUE_ENTITY = SAFE_CAST(SRC.UNIQENTITY AS INT64)
,TGT.DESC_OF = SAFE_CAST(SRC.DESCRIPTIONOF AS STRING)
,TGT.NUM = SAFE_CAST(SRC.NUMBER AS STRING)
,TGT.EXTENSION = SAFE_CAST(SRC.EXTENSION AS STRING)
,TGT.EMAIL_WEB = SAFE_CAST(SRC.EMAILWEB AS STRING)
,TGT.TYPE_CD = SAFE_CAST(SRC.TYPECODE AS STRING)
,TGT.SCREEN_ORDER = SAFE_CAST(SRC.SCREENORDER AS INT64)
,TGT.CALL_PERMISSION = SAFE_CAST(SRC.CALLPERMISSION AS STRING)
,TGT.INSERTED_BY_CD = SAFE_CAST(SRC.INSERTEDBYCODE AS STRING)
,TGT.INSERTED_DT = SAFE_CAST(SRC.INSERTEDDATE AS DATETIME)
,TGT.UPDATED_BY_CD = SAFE_CAST(SRC.UPDATEDBYCODE AS STRING)
,TGT.UPDATED_DT = SAFE_CAST(SRC.UPDATEDDATE AS DATETIME)
,TGT.FLGS = SAFE_CAST(SRC.FLAGS AS INT64)
,TGT.PHONE_COUNTRY_CD = SAFE_CAST(SRC.PHONECOUNTRYCODE AS STRING)
,TGT.JOB_RUN_ID = SAFE_CAST(SRC.JOBRUNID AS STRING)
--,TGT.CREATE_DTTIME = CURRENT_DATETIME()
,TGT.UPDATE_DTTIME = CURRENT_DATETIME()
--,TGT.SOURCE_SYSTEM_CD = 'AMS'

WHEN NOT MATCHED
THEN INSERT ( --Target column names
UNIQUE_CONTACT_NUM
,UNIQUE_FIXED_CONTACT_NUM
,UNIQUE_CONTACT_NAME
,UNIQUE_ENTITY
,DESC_OF
,NUM
,EXTENSION
,EMAIL_WEB
,TYPE_CD
,SCREEN_ORDER
,CALL_PERMISSION
,INSERTED_BY_CD
,INSERTED_DT
,UPDATED_BY_CD
,UPDATED_DT
,FLGS
,PHONE_COUNTRY_CD
,JOB_RUN_ID
,CREATE_DTTIME
,UPDATE_DTTIME
,SOURCE_SYSTEM_CD
)

VALUES ( --Source values
SAFE_CAST(UNIQCONTACTNUMBER AS INT64)
,SAFE_CAST(UNIQFIXEDCONTACTNUMBER AS INT64)
,SAFE_CAST(UNIQCONTACTNAME AS INT64)
,SAFE_CAST(UNIQENTITY AS INT64)
,SAFE_CAST(DESCRIPTIONOF AS STRING)
,SAFE_CAST(NUMBER AS STRING)
,SAFE_CAST(EXTENSION AS STRING)
,SAFE_CAST(EMAILWEB AS STRING)
,SAFE_CAST(TYPECODE AS STRING)
,SAFE_CAST(SCREENORDER AS INT64)
,SAFE_CAST(CALLPERMISSION AS STRING)
,SAFE_CAST(INSERTEDBYCODE AS STRING)
,SAFE_CAST(INSERTEDDATE AS DATETIME)
,SAFE_CAST(UPDATEDBYCODE AS STRING)
,SAFE_CAST(UPDATEDDATE AS DATETIME)
,SAFE_CAST(FLAGS AS INT64)
,SAFE_CAST(PHONECOUNTRYCODE AS STRING)
,SAFE_CAST(JOBRUNID AS STRING)
,CURRENT_DATETIME()
,CURRENT_DATETIME()
,'AMS'
)
