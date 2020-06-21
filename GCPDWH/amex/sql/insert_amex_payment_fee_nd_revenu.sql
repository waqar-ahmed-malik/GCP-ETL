INSERT INTO OPERATIONAL.AMEX_PAYMENT_FEES_AND_REVENUES
(
RECORD_TYPE,
PAYEE_MERCHANT_ID,
AMERICAN_EXPRESS_PAYMENT_NUM,
PAYMENT_DT,
PAYMENT_CURRENCY,
SUBMISSION_MERCHANT_ID,
MERCHANT_LOCATION_ID,
FEE_OR_REVENUE_AMT,
FEE_OR_REVENUE_DESC,
ASSET_BILLING_AMT,
ASSET_BILLING_DESC ,
ASSET_BILLING_TAX,
PAY_IN_GROSS_IND,
BATCH_CD,
BILL_CD,
FILLER,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATED_BY,
CREATE_DTTIME
)
SELECT 
RECORD_TYPE,
PAYEE_MERCHANT_ID,
AMERICAN_EXPRESS_PAYMENT_NUM,
PARSE_DATE('%Y%m%d',PAYMENT_DT) AS PAYMENT_DT,
PAYMENT_CURRENCY,
SUBMISSION_MERCHANT_ID,
MERCHANT_LOCATION_ID,
CASE WHEN FEE_OR_REVENUE_AMT='' THEN NULL ELSE
CAST(FEE_OR_REVENUE_AMT AS FLOAT64) END AS FEE_OR_REVENUE_AMT,
FEE_OR_REVENUE_DESC,
CASE WHEN ASSET_BILLING_AMT='' THEN NULL ELSE
CAST(ASSET_BILLING_AMT AS FLOAT64) END AS ASSET_BILLING_AMT,
ASSET_BILLING_DESC ,
CASE WHEN ASSET_BILLING_TAX = '' THEN NULL ELSE
CAST( ASSET_BILLING_TAX AS FLOAT64) END  AS ASSET_BILLING_TAX,
CASE WHEN PAY_IN_GROSS_IND ='' THEN NULL ELSE
CAST( PAY_IN_GROSS_IND AS INT64) END AS PAY_IN_GROSS_IND,
BATCH_CD,
BILL_CD,
FILLER,
'v_job_run_id' AS JOB_RUN_ID,
'amex' AS SOURCE_SYSTEM_CD,
'v_job_name' AS CREATED_BY,
CURRENT_DATETIME() AS CREATE_DTTIME
FROM LANDING.WORK_AMEX_FEES_AND_REVENUES_RECORD