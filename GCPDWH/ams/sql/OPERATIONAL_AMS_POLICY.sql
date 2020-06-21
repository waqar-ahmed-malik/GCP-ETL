MERGE `OPERATIONAL.AMS_POLICY` AS TGT
USING `LANDING.WORK_AMS_POLICY` AS SRC
ON TGT.UNIQUE_POLICY = SAFE_CAST(SRC.UNIQPOLICY AS INT64)

WHEN MATCHED
THEN UPDATE SET
TGT.UNIQUE_POLICY = SAFE_CAST(SRC.UNIQPOLICY AS INT64)
,TGT.UNIQUE_ORIGINAL_POLICY = SAFE_CAST(SRC.UNIQORIGINALPOLICY AS INT64)
,TGT.UNIQUE_ENTITY = SAFE_CAST(SRC.UNIQENTITY AS INT64)
,TGT.UNIQUE_AGENCY = SAFE_CAST(SRC.UNIQAGENCY AS INT64)
,TGT.UNIQUE_BRANCH = SAFE_CAST(SRC.UNIQBRANCH AS INT64)
,TGT.UNIQUE_DEPARTMENT = SAFE_CAST(SRC.UNIQDEPARTMENT AS INT64)
,TGT.DESC_OF = SAFE_CAST(SRC.DESCRIPTIONOF AS STRING)
,TGT.UNIQUE_CD_POLICY_LINE_TYPE = SAFE_CAST(SRC.UNIQCDPOLICYLINETYPE AS INT64)
,TGT.POLICY_NUM = SAFE_CAST(SRC.POLICYNUMBER AS STRING)
,TGT.EFFECTIVE_DT = SAFE_CAST(SRC.EFFECTIVEDATE AS DATETIME)
,TGT.EXPIRATION_DT = SAFE_CAST(SRC.EXPIRATIONDATE AS DATETIME)
,TGT.CONTRACTED_EXPIRATION_DT = SAFE_CAST(SRC.CONTRACTEDEXPIRATIONDATE AS DATETIME)
,TGT.BILLED_COMMISSION = SAFE_CAST(SRC.BILLEDCOMMISSION AS FLOAT64)
,TGT.ANNUALIZED_COMMISSION = SAFE_CAST(SRC.ANNUALIZEDCOMMISSION AS FLOAT64)
,TGT.ESTIMATED_COMMISSION = SAFE_CAST(SRC.ESTIMATEDCOMMISSION AS FLOAT64)
,TGT.BILLED_PREMIUM = SAFE_CAST(SRC.BILLEDPREMIUM AS FLOAT64)
,TGT.ANNUALIZED_PREMIUM = SAFE_CAST(SRC.ANNUALIZEDPREMIUM AS FLOAT64)
,TGT.ESTIMATED_PREMIUM = SAFE_CAST(SRC.ESTIMATEDPREMIUM AS FLOAT64)
,TGT.INSERTED_BY_CD = SAFE_CAST(SRC.INSERTEDBYCODE AS STRING)
,TGT.INSERTED_DT = SAFE_CAST(SRC.INSERTEDDATE AS DATETIME)
,TGT.UPDATED_BY_CD = SAFE_CAST(SRC.UPDATEDBYCODE AS STRING)
,TGT.UPDATED_DT = SAFE_CAST(SRC.UPDATEDDATE AS DATETIME)
,TGT.FLGS = SAFE_CAST(SRC.FLAGS AS INT64)
,TGT.TS = SAFE_CAST(SRC.TS AS DATETIME)
,TGT.LAST_DOWNLOADED_PREMIUM = SAFE_CAST(SRC.LASTDOWNLOADEDPREMIUM AS FLOAT64)
,TGT.STANDARDS_BODY_CD = SAFE_CAST(SRC.STANDARDSBODYCODE AS STRING)
,TGT.ESTIMATED_MONTHLY_PREMIUM = SAFE_CAST(SRC.ESTIMATEDMONTHLYPREMIUM AS FLOAT64)
,TGT.ESTIMATED_MONTHLY_COMMISSION = SAFE_CAST(SRC.ESTIMATEDMONTHLYCOMMISSION AS FLOAT64)
,TGT.MIGRATED_BILLED_PREMIUM = SAFE_CAST(SRC.MIGRATEDBILLEDPREMIUM AS FLOAT64)
,TGT.MIGRATED_BILLED_COMMISSION = SAFE_CAST(SRC.MIGRATEDBILLEDCOMMISSION AS FLOAT64)
,TGT.POLICY_ID = SAFE_CAST(SRC.POLICYID AS STRING)
,TGT.UNIQUE_LK_POLICY_SOURCE = SAFE_CAST(SRC.UNIQLKPOLICYSOURCE AS INT64)
,TGT.UNIQUE_CD_RENEWAL_STAGE = SAFE_CAST(SRC.UNIQCDRENEWALSTAGE AS INT64)
,TGT.RENEWAL_PREMIUM = SAFE_CAST(SRC.RENEWALPREMIUM AS FLOAT64)
,TGT.QUOTE_ID = SAFE_CAST(SRC.QUOTEID AS STRING)
,TGT.RENEWAL_INVITE_RECEIVED_DT = SAFE_CAST(SRC.RENEWALINVITERECEIVEDDATE AS DATETIME)
,TGT.JOB_RUN_ID = SAFE_CAST(SRC.JOBRUNID AS STRING)
--,TGT.CREATE_DTTIME = CURRENT_DATETIME()
,TGT.UPDATE_DTTIME = CURRENT_DATETIME()
--,TGT.SOURCE_SYSTEM_CD = 'AMS'

WHEN NOT MATCHED
THEN INSERT ( --Target column names
UNIQUE_POLICY
,UNIQUE_ORIGINAL_POLICY
,UNIQUE_ENTITY
,UNIQUE_AGENCY
,UNIQUE_BRANCH
,UNIQUE_DEPARTMENT
,DESC_OF
,UNIQUE_CD_POLICY_LINE_TYPE
,POLICY_NUM
,EFFECTIVE_DT
,EXPIRATION_DT
,CONTRACTED_EXPIRATION_DT
,BILLED_COMMISSION
,ANNUALIZED_COMMISSION
,ESTIMATED_COMMISSION
,BILLED_PREMIUM
,ANNUALIZED_PREMIUM
,ESTIMATED_PREMIUM
,INSERTED_BY_CD
,INSERTED_DT
,UPDATED_BY_CD
,UPDATED_DT
,FLGS
,TS
,LAST_DOWNLOADED_PREMIUM
,STANDARDS_BODY_CD
,ESTIMATED_MONTHLY_PREMIUM
,ESTIMATED_MONTHLY_COMMISSION
,MIGRATED_BILLED_PREMIUM
,MIGRATED_BILLED_COMMISSION
,POLICY_ID
,UNIQUE_LK_POLICY_SOURCE
,UNIQUE_CD_RENEWAL_STAGE
,RENEWAL_PREMIUM
,QUOTE_ID
,RENEWAL_INVITE_RECEIVED_DT
,JOB_RUN_ID
,CREATE_DTTIME
,UPDATE_DTTIME
,SOURCE_SYSTEM_CD
)

VALUES ( --Source values
SAFE_CAST(UNIQPOLICY AS INT64)
,SAFE_CAST(UNIQORIGINALPOLICY AS INT64)
,SAFE_CAST(UNIQENTITY AS INT64)
,SAFE_CAST(UNIQAGENCY AS INT64)
,SAFE_CAST(UNIQBRANCH AS INT64)
,SAFE_CAST(UNIQDEPARTMENT AS INT64)
,SAFE_CAST(DESCRIPTIONOF AS STRING)
,SAFE_CAST(UNIQCDPOLICYLINETYPE AS INT64)
,SAFE_CAST(POLICYNUMBER AS STRING)
,SAFE_CAST(EFFECTIVEDATE AS DATETIME)
,SAFE_CAST(EXPIRATIONDATE AS DATETIME)
,SAFE_CAST(CONTRACTEDEXPIRATIONDATE AS DATETIME)
,SAFE_CAST(BILLEDCOMMISSION AS FLOAT64)
,SAFE_CAST(ANNUALIZEDCOMMISSION AS FLOAT64)
,SAFE_CAST(ESTIMATEDCOMMISSION AS FLOAT64)
,SAFE_CAST(BILLEDPREMIUM AS FLOAT64)
,SAFE_CAST(ANNUALIZEDPREMIUM AS FLOAT64)
,SAFE_CAST(ESTIMATEDPREMIUM AS FLOAT64)
,SAFE_CAST(INSERTEDBYCODE AS STRING)
,SAFE_CAST(INSERTEDDATE AS DATETIME)
,SAFE_CAST(UPDATEDBYCODE AS STRING)
,SAFE_CAST(UPDATEDDATE AS DATETIME)
,SAFE_CAST(FLAGS AS INT64)
,SAFE_CAST(TS AS DATETIME)
,SAFE_CAST(LASTDOWNLOADEDPREMIUM AS FLOAT64)
,SAFE_CAST(STANDARDSBODYCODE AS STRING)
,SAFE_CAST(ESTIMATEDMONTHLYPREMIUM AS FLOAT64)
,SAFE_CAST(ESTIMATEDMONTHLYCOMMISSION AS FLOAT64)
,SAFE_CAST(MIGRATEDBILLEDPREMIUM AS FLOAT64)
,SAFE_CAST(MIGRATEDBILLEDCOMMISSION AS FLOAT64)
,SAFE_CAST(POLICYID AS STRING)
,SAFE_CAST(UNIQLKPOLICYSOURCE AS INT64)
,SAFE_CAST(UNIQCDRENEWALSTAGE AS INT64)
,SAFE_CAST(RENEWALPREMIUM AS FLOAT64)
,SAFE_CAST(QUOTEID AS STRING)
,SAFE_CAST(RENEWALINVITERECEIVEDDATE AS DATETIME)
,SAFE_CAST(JOBRUNID AS STRING)
,CURRENT_DATETIME()
,CURRENT_DATETIME()
,'AMS'
)
