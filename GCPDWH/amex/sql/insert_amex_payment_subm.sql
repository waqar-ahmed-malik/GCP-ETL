INSERT INTO OPERATIONAL.AMEX_PAYMENT_SUBMISSION
(
RECORD_TYPE,
PAYEE_MERCHANT_ID,
SETTLEMENT_ACCOUNT_TYPE_CD,
AMERICAN_EXPRESS_PAYMENT_NUM,
PAYMENT_DT,
PAYMENT_CURRENCY,
SUBMISSION_MERCHANT_ID,
BUSINESS_SUBMISSION_DT,
AMERICAN_EXPRESS_PROCESSING_DT,
SUBMISSION_INVOICE_NUM,
SUBMISSION_CURRENCY,
FILLER,
SUBMISSION_EXCHANGE_RT,
SUBMISSION_GROSS_AMT_IN_SUBMISSION_CURRENCY,
SUBMISSION_GROSS_AMT_IN_PAYMENT_CURRENCY,
SUBMISSION_DISCOUNT_AMT,
SUBMISSION_SERVICE_FEE_AMT,
SUBMISSION_TAX_AMT,
SUBMISSION_NET_AMT,
SUBMISSION_DISCOUNT_RT,
SUBMISSION_TAX_RT,
TRANSACTION_CNT,
TRACKING_ID,
INSTALLMENT_NUM,
ACCELERATION_NUM,
ORIGINAL_SETTLEMENT_DT,
ACCELERATION_DT,
NUM_OF_DAYS_IN_ADVANCE,
SUBMISSION_ACCELERATION_FEE_AMT,
SUBMISSION_ACCELERATION_FEE_NET_AMT,
SUBMISSION_DEBIT_GROSS_AMT,
SUBMISSION_CREDIT_GROSS_AMT,
FILLER_1,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATED_BY,
CREATE_DTTIME
)
SELECT 
RECORD_TYPE,
PAYEE_MERCHANT_ID,
SETTLEMENT_ACCOUNT_TYPE_CD,
AMERICAN_EXPRESS_PAYMENT_NUM,
PARSE_DATE('%Y%m%d',PAYMENT_DT) AS PAYMENT_DT,
PAYMENT_CURRENCY,
SUBMISSION_MERCHANT_ID,
PARSE_DATE('%Y%m%d',BUSINESS_SUBMISSION_DT) AS BUSINESS_SUBMISSION_DT,
PARSE_DATE('%Y%m%d',AMERICAN_EXPRESS_PROCESSING_DT) AS AMERICAN_EXPRESS_PROCESSING_DT,
SUBMISSION_INVOICE_NUM,
SUBMISSION_CURRENCY,
FILLER,
CASE WHEN SUBMISSION_EXCHANGE_RATE='' THEN NULL ELSE
CAST(SUBMISSION_EXCHANGE_RATE AS FLOAT64) END AS  SUBMISSION_EXCHANGE_RT ,
CASE WHEN SUBMISSION_GROSS_AMT_IN_SUBMISSION_CURRENCY='' THEN NULL ELSE
CAST(SUBMISSION_GROSS_AMT_IN_SUBMISSION_CURRENCY AS FLOAT64) END AS  SUBMISSION_GROSS_AMT_IN_SUBMISSION_CURRENCY,
CASE WHEN SUBMISSION_GROSS_AMT_IN_PAYMENT_CURRENCY='' THEN NULL ELSE
CAST(SUBMISSION_GROSS_AMT_IN_PAYMENT_CURRENCY AS FLOAT64) END AS  SUBMISSION_GROSS_AMT_IN_PAYMENT_CURRENCY,
CASE WHEN SUBMISSION_DISCOUNT_AMT='' THEN NULL ELSE
CAST(SUBMISSION_DISCOUNT_AMT AS FLOAT64) END AS SUBMISSION_DISCOUNT_AMT,
CASE WHEN SUBMISSION_SERVICE_FEE_AMT='' THEN NULL ELSE
CAST(SUBMISSION_SERVICE_FEE_AMT AS FLOAT64) END AS SUBMISSION_SERVICE_FEE_AMT,
CASE WHEN SUBMISSION_TAX_AMT='' THEN NULL ELSE
CAST(SUBMISSION_TAX_AMT AS FLOAT64) END AS SUBMISSION_TAX_AMT,
CASE WHEN SUBMISSION_NET_AMT='' THEN NULL ELSE
CAST(SUBMISSION_NET_AMT AS FLOAT64) END AS SUBMISSION_NET_AMT,
CASE WHEN SUBMISSION_DISCOUNT_RATE='' THEN NULL ELSE
CAST(SUBMISSION_DISCOUNT_RATE AS FLOAT64) END AS SUBMISSION_DISCOUNT_RT,
CASE WHEN SUBMISSION_TAX_RATE='' THEN NULL ELSE
CAST(SUBMISSION_TAX_RATE AS FLOAT64) END AS SUBMISSION_TAX_RT,
TRANSACTION_COUNT AS TRANSACTION_CNT, 
TRACKING_ID, 
INSTALLMENT_NUM,
ACCELERATION_NUM,
CASE WHEN ORIGINAL_SETTLEMENT_DT='' THEN NULL ELSE
PARSE_DATE('%Y%m%d',ORIGINAL_SETTLEMENT_DT) END AS ORIGINAL_SETTLEMENT_DT, 
CASE WHEN ACCELERATION_DT='' THEN NULL ELSE
PARSE_DATE('%Y%m%d',ACCELERATION_DT) END AS ACCELERATION_DT,
NUM_OF_DAYS_IN_ADVANCE,
CASE WHEN SUBMISSION_ACCELERATION_FEE_AMT='' THEN NULL ELSE
CAST(SUBMISSION_ACCELERATION_FEE_AMT AS FLOAT64) END AS SUBMISSION_ACCELERATION_FEE_AMT,
CASE WHEN SUBMISSION_ACCELERATION_FEE_NET_AMT='' THEN NULL ELSE
CAST(SUBMISSION_ACCELERATION_FEE_NET_AMT AS FLOAT64) END AS SUBMISSION_ACCELERATION_FEE_NET_AMT,
CASE WHEN SUBMISSION_DEBIT_GROSS_AMT='' THEN NULL ELSE
CAST(SUBMISSION_DEBIT_GROSS_AMT AS FLOAT64) END AS SUBMISSION_DEBIT_GROSS_AMT, 
CASE WHEN SUBMISSION_CREDIT_GROSS_AMT='' THEN NULL ELSE
CAST(SUBMISSION_CREDIT_GROSS_AMT AS FLOAT64) END AS SUBMISSION_CREDIT_GROSS_AMT,
FILLER_1,
'v_job_run_id' AS JOB_RUN_ID,
'amex' AS SOURCE_SYSTEM_CD,
'v_job_name' AS CREATED_BY,
CURRENT_DATETIME() AS CREATE_DTTIME
FROM LANDING.WORK_AMEX_SUBMISSION_RECORD