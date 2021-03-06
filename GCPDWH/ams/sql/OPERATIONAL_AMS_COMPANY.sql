MERGE `OPERATIONAL.AMS_COMPANY` AS TGT
USING `LANDING.WORK_AMS_COMPANY` AS SRC
ON TGT.UNIQUE_ENTITY = SAFE_CAST(SRC.UNIQENTITY AS INT64)

WHEN MATCHED
THEN UPDATE SET
TGT.UNIQUE_ENTITY = SAFE_CAST(SRC.UNIQENTITY AS INT64)
,TGT.UNIQUE_ENTITY_MASTER = SAFE_CAST(SRC.UNIQENTITYMASTER AS INT64)
,TGT.LOOKUP_CD = SAFE_CAST(SRC.LOOKUPCODE AS STRING)
,TGT.NAME_OF = SAFE_CAST(SRC.NAMEOF AS STRING)
,TGT.COMMENTS = SAFE_CAST(SRC.COMMENTS AS STRING)
,TGT.UNIQUE_CONTACT_NAME_PRIMARY = SAFE_CAST(SRC.UNIQCONTACTNAMEPRIMARY AS INT64)
,TGT.UNIQUE_CONTACT_ADDRESS_ACCOUNT = SAFE_CAST(SRC.UNIQCONTACTADDRESSACCOUNT AS INT64)
,TGT.UNIQUE_CONTACT_NUM_ACCOUNT = SAFE_CAST(SRC.UNIQCONTACTNUMBERACCOUNT AS INT64)
,TGT.UNIQUE_CONTACT_NUM_FAX_ACCOUNT = SAFE_CAST(SRC.UNIQCONTACTNUMBERFAXACCOUNT AS INT64)
,TGT.UNIQUE_CONTACT_NUM_WEB_ACCOUNT = SAFE_CAST(SRC.UNIQCONTACTNUMBERWEBACCOUNT AS INT64)
,TGT.UNIQUE_ENTITY_BILLING_COMPANY = SAFE_CAST(SRC.UNIQENTITYBILLINGCOMPANY AS INT64)
,TGT.NAIC_CD = SAFE_CAST(SRC.NAICCODE AS STRING)
,TGT.BILL_MODE_CD = SAFE_CAST(SRC.BILLMODECODE AS STRING)
,TGT.DBILL_RECONCILIATION_METHOD_CD = SAFE_CAST(SRC.DBILLRECONCILIATIONMETHODCODE AS STRING)
,TGT.BATCH_PAYMENT_METHOD_CD = SAFE_CAST(SRC.BATCHPAYMENTMETHODCODE AS STRING)
,TGT.UNIQUE_GL_ACCOUNT_PAY = SAFE_CAST(SRC.UNIQGLACCOUNTPAY AS INT64)
,TGT.UNIQUE_GL_ACCOUNT_PAY_WRITE_OFF = SAFE_CAST(SRC.UNIQGLACCOUNTPAYWRITEOFF AS INT64)
,TGT.UNIQUE_GL_ACCOUNT_BINDER_BILL = SAFE_CAST(SRC.UNIQGLACCOUNTBINDERBILL AS INT64)
,TGT.UNIQUE_GL_ACCOUNT_ABILL = SAFE_CAST(SRC.UNIQGLACCOUNTABILL AS INT64)
,TGT.UNIQUE_GL_ACCOUNT_DBILL = SAFE_CAST(SRC.UNIQGLACCOUNTDBILL AS INT64)
,TGT.UNIQUE_GL_ACCOUNT_DBILL_RECV = SAFE_CAST(SRC.UNIQGLACCOUNTDBILLRECV AS INT64)
,TGT.UNIQUE_GL_ACCOUNT_CASH_ON_ACCT = SAFE_CAST(SRC.UNIQGLACCOUNTCASHONACCT AS INT64)
,TGT.INACTIVE_DT = SAFE_CAST(SRC.INACTIVEDATE AS DATETIME)
,TGT.INACTIVE_REASON = SAFE_CAST(SRC.INACTIVEREASON AS STRING)
,TGT.EXPIRATION_DT = SAFE_CAST(SRC.EXPIRATIONDATE AS DATETIME)
,TGT.CONVERSION_DT = SAFE_CAST(SRC.CONVERSIONDATE AS DATETIME)
,TGT.CONVERSION_PRIOR_ACCOUNT_ID = SAFE_CAST(SRC.CONVERSIONPRIORACCOUNTID AS STRING)
,TGT.INSERTED_BY_CD = SAFE_CAST(SRC.INSERTEDBYCODE AS STRING)
,TGT.INSERTED_DT = SAFE_CAST(SRC.INSERTEDDATE AS DATETIME)
,TGT.UPDATED_BY_CD = SAFE_CAST(SRC.UPDATEDBYCODE AS STRING)
,TGT.UPDATED_DT = SAFE_CAST(SRC.UPDATEDDATE AS DATETIME)
,TGT.FLGS = SAFE_CAST(SRC.FLAGS AS INT64)
,TGT.TS = SAFE_CAST(SRC.TS AS DATETIME)
,TGT.UNIQUE_GL_ACCOUNT_DEFER_COMPANY_PAYABLES = SAFE_CAST(SRC.UNIQGLACCOUNTDEFERCOMPANYPAYABLES AS INT64)
,TGT.INACTIVATED_BY_CD = SAFE_CAST(SRC.INACTIVATEDBYCODE AS STRING)
,TGT.UNIQUE_GL_ACCOUNT_PREMIUM_PAYABLE_CASH_ON_ACCT = SAFE_CAST(SRC.UNIQGLACCOUNTPREMIUMPAYABLECASHONACCT AS INT64)
,TGT.CD_COUNTRY_CD = SAFE_CAST(SRC.CDCOUNTRYCODE AS STRING)
,TGT.IBC_CD = SAFE_CAST(SRC.IBCCODE AS STRING)
,TGT.UNIQUE_GL_ACCOUNT_BINDER_PAYABLE = SAFE_CAST(SRC.UNIQGLACCOUNTBINDERPAYABLE AS INT64)
,TGT.FATCA_COMPLIANCE_STATUS_CD = SAFE_CAST(SRC.FATCACOMPLIANCESTATUSCODE AS STRING)
,TGT.COMPANY_ID = SAFE_CAST(SRC.COMPANYID AS STRING)
,TGT.JOB_RUN_ID = SAFE_CAST(SRC.JOBRUNID AS STRING)
--,TGT.CREATE_DTTIME = CURRENT_DATETIME()
,TGT.UPDATE_DTTIME = CURRENT_DATETIME()
--,TGT.SOURCE_SYSTEM_CD = 'AMS'

WHEN NOT MATCHED
THEN INSERT ( --Target column names
UNIQUE_ENTITY
,UNIQUE_ENTITY_MASTER
,LOOKUP_CD
,NAME_OF
,COMMENTS
,UNIQUE_CONTACT_NAME_PRIMARY
,UNIQUE_CONTACT_ADDRESS_ACCOUNT
,UNIQUE_CONTACT_NUM_ACCOUNT
,UNIQUE_CONTACT_NUM_FAX_ACCOUNT
,UNIQUE_CONTACT_NUM_WEB_ACCOUNT
,UNIQUE_ENTITY_BILLING_COMPANY
,NAIC_CD
,BILL_MODE_CD
,DBILL_RECONCILIATION_METHOD_CD
,BATCH_PAYMENT_METHOD_CD
,UNIQUE_GL_ACCOUNT_PAY
,UNIQUE_GL_ACCOUNT_PAY_WRITE_OFF
,UNIQUE_GL_ACCOUNT_BINDER_BILL
,UNIQUE_GL_ACCOUNT_ABILL
,UNIQUE_GL_ACCOUNT_DBILL
,UNIQUE_GL_ACCOUNT_DBILL_RECV
,UNIQUE_GL_ACCOUNT_CASH_ON_ACCT
,INACTIVE_DT
,INACTIVE_REASON
,EXPIRATION_DT
,CONVERSION_DT
,CONVERSION_PRIOR_ACCOUNT_ID
,INSERTED_BY_CD
,INSERTED_DT
,UPDATED_BY_CD
,UPDATED_DT
,FLGS
,TS
,UNIQUE_GL_ACCOUNT_DEFER_COMPANY_PAYABLES
,INACTIVATED_BY_CD
,UNIQUE_GL_ACCOUNT_PREMIUM_PAYABLE_CASH_ON_ACCT
,CD_COUNTRY_CD
,IBC_CD
,UNIQUE_GL_ACCOUNT_BINDER_PAYABLE
,FATCA_COMPLIANCE_STATUS_CD
,COMPANY_ID
,JOB_RUN_ID
,CREATE_DTTIME
,UPDATE_DTTIME
,SOURCE_SYSTEM_CD
)

VALUES ( --Source values
SAFE_CAST(UNIQENTITY AS INT64)
,SAFE_CAST(UNIQENTITYMASTER AS INT64)
,SAFE_CAST(LOOKUPCODE AS STRING)
,SAFE_CAST(NAMEOF AS STRING)
,SAFE_CAST(COMMENTS AS STRING)
,SAFE_CAST(UNIQCONTACTNAMEPRIMARY AS INT64)
,SAFE_CAST(UNIQCONTACTADDRESSACCOUNT AS INT64)
,SAFE_CAST(UNIQCONTACTNUMBERACCOUNT AS INT64)
,SAFE_CAST(UNIQCONTACTNUMBERFAXACCOUNT AS INT64)
,SAFE_CAST(UNIQCONTACTNUMBERWEBACCOUNT AS INT64)
,SAFE_CAST(UNIQENTITYBILLINGCOMPANY AS INT64)
,SAFE_CAST(NAICCODE AS STRING)
,SAFE_CAST(BILLMODECODE AS STRING)
,SAFE_CAST(DBILLRECONCILIATIONMETHODCODE AS STRING)
,SAFE_CAST(BATCHPAYMENTMETHODCODE AS STRING)
,SAFE_CAST(UNIQGLACCOUNTPAY AS INT64)
,SAFE_CAST(UNIQGLACCOUNTPAYWRITEOFF AS INT64)
,SAFE_CAST(UNIQGLACCOUNTBINDERBILL AS INT64)
,SAFE_CAST(UNIQGLACCOUNTABILL AS INT64)
,SAFE_CAST(UNIQGLACCOUNTDBILL AS INT64)
,SAFE_CAST(UNIQGLACCOUNTDBILLRECV AS INT64)
,SAFE_CAST(UNIQGLACCOUNTCASHONACCT AS INT64)
,SAFE_CAST(INACTIVEDATE AS DATETIME)
,SAFE_CAST(INACTIVEREASON AS STRING)
,SAFE_CAST(EXPIRATIONDATE AS DATETIME)
,SAFE_CAST(CONVERSIONDATE AS DATETIME)
,SAFE_CAST(CONVERSIONPRIORACCOUNTID AS STRING)
,SAFE_CAST(INSERTEDBYCODE AS STRING)
,SAFE_CAST(INSERTEDDATE AS DATETIME)
,SAFE_CAST(UPDATEDBYCODE AS STRING)
,SAFE_CAST(UPDATEDDATE AS DATETIME)
,SAFE_CAST(FLAGS AS INT64)
,SAFE_CAST(TS AS DATETIME)
,SAFE_CAST(UNIQGLACCOUNTDEFERCOMPANYPAYABLES AS INT64)
,SAFE_CAST(INACTIVATEDBYCODE AS STRING)
,SAFE_CAST(UNIQGLACCOUNTPREMIUMPAYABLECASHONACCT AS INT64)
,SAFE_CAST(CDCOUNTRYCODE AS STRING)
,SAFE_CAST(IBCCODE AS STRING)
,SAFE_CAST(UNIQGLACCOUNTBINDERPAYABLE AS INT64)
,SAFE_CAST(FATCACOMPLIANCESTATUSCODE AS STRING)
,SAFE_CAST(COMPANYID AS STRING)
,SAFE_CAST(JOBRUNID AS STRING)
,CURRENT_DATETIME()
,CURRENT_DATETIME()
,'AMS'
)
