MERGE `OPERATIONAL.AMS_TRANS_CODE` AS TGT
USING `LANDING.WORK_AMS_TRANS_CODE` AS SRC
ON TGT.UNIQUE_TRANS_CD = SAFE_CAST(SRC.UNIQTRANSCODE AS INT64)

WHEN MATCHED
THEN UPDATE SET
TGT.UNIQUE_TRANS_CD = SAFE_CAST(SRC.UNIQTRANSCODE AS INT64)
,TGT.CD = SAFE_CAST(SRC.CODE AS STRING)
,TGT.CLASS_CD = SAFE_CAST(SRC.CLASSCODE AS STRING)
,TGT.BILL_CD = SAFE_CAST(SRC.BILLCODE AS STRING)
,TGT.UNIQUE_GL_ACCOUNT = SAFE_CAST(SRC.UNIQGLACCOUNT AS INT64)
,TGT.AMT_CD = SAFE_CAST(SRC.AMOUNTCODE AS STRING)
,TGT.AR_DUE_DT_CD = SAFE_CAST(SRC.ARDUEDATECODE AS STRING)
,TGT.AR_DUE_DT_DAYS = SAFE_CAST(SRC.ARDUEDATEDAYS AS INT64)
,TGT.POLICY_BILLED_CD = SAFE_CAST(SRC.POLICYBILLEDCODE AS STRING)
,TGT.POLICY_ANNUALIZED_CD = SAFE_CAST(SRC.POLICYANNUALIZEDCODE AS STRING)
,TGT.INSERTED_BY_CD = SAFE_CAST(SRC.INSERTEDBYCODE AS STRING)
,TGT.INSERTED_DT = SAFE_CAST(SRC.INSERTEDDATE AS DATETIME)
,TGT.UPDATED_BY_CD = SAFE_CAST(SRC.UPDATEDBYCODE AS STRING)
,TGT.UPDATED_DT = SAFE_CAST(SRC.UPDATEDDATE AS DATETIME)
,TGT.FLGS = SAFE_CAST(SRC.FLAGS AS INT64)
,TGT.CONFIGURE_LK_LANGUAGE_RESOURCE_ID = SAFE_CAST(SRC.CONFIGURELKLANGUAGERESOURCEID AS STRING)
,TGT.JOB_RUN_ID = SAFE_CAST(SRC.JOBRUNID AS STRING)
--,TGT.CREATE_DTTIME = CURRENT_DATETIME()
,TGT.UPDATE_DTTIME = CURRENT_DATETIME()
--,TGT.SOURCE_SYSTEM_CD = 'AMS'

WHEN NOT MATCHED
THEN INSERT ( --Target column names
UNIQUE_TRANS_CD
,CD
,CLASS_CD
,BILL_CD
,UNIQUE_GL_ACCOUNT
,AMT_CD
,AR_DUE_DT_CD
,AR_DUE_DT_DAYS
,POLICY_BILLED_CD
,POLICY_ANNUALIZED_CD
,INSERTED_BY_CD
,INSERTED_DT
,UPDATED_BY_CD
,UPDATED_DT
,FLGS
,CONFIGURE_LK_LANGUAGE_RESOURCE_ID
,JOB_RUN_ID
,CREATE_DTTIME
,UPDATE_DTTIME
,SOURCE_SYSTEM_CD
)

VALUES ( --Source values
SAFE_CAST(UNIQTRANSCODE AS INT64)
,SAFE_CAST(CODE AS STRING)
,SAFE_CAST(CLASSCODE AS STRING)
,SAFE_CAST(BILLCODE AS STRING)
,SAFE_CAST(UNIQGLACCOUNT AS INT64)
,SAFE_CAST(AMOUNTCODE AS STRING)
,SAFE_CAST(ARDUEDATECODE AS STRING)
,SAFE_CAST(ARDUEDATEDAYS AS INT64)
,SAFE_CAST(POLICYBILLEDCODE AS STRING)
,SAFE_CAST(POLICYANNUALIZEDCODE AS STRING)
,SAFE_CAST(INSERTEDBYCODE AS STRING)
,SAFE_CAST(INSERTEDDATE AS DATETIME)
,SAFE_CAST(UPDATEDBYCODE AS STRING)
,SAFE_CAST(UPDATEDDATE AS DATETIME)
,SAFE_CAST(FLAGS AS INT64)
,SAFE_CAST(CONFIGURELKLANGUAGERESOURCEID AS STRING)
,SAFE_CAST(JOBRUNID AS STRING)
,CURRENT_DATETIME()
,CURRENT_DATETIME()
,'AMS'
)