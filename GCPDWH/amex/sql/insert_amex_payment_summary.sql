INSERT INTO OPERATIONAL.AMEX_PAYMENT_SUMMARY
(
RECORD_TYPE,
PAYEE_MERCHANT_ID,
SETTLEMENT_ACCOUNT_TYPE_CD,
AMERICAN_EXPRESS_PAYMENT_NUM,
PAYMENT_DT,
PAYMENT_CURRENCY,
UNIQUE_PAYMENT_REFERENCE_NUM,
PAYMENT_NET_AMT,
PAYMENT_GROSS_AMT,
PAYMENT_DISCOUNT_AMT,
PAYMENT_SERVICE_FEE_AMT,
PAYMENT_ADJUSTMENT_AMT,
PAYMENT_TAX_AMT,
OPENING_DEBIT_BALANCE_AMT,
PAYEE_DIRECT_DEPOSIT_NUM,
BANK_ACCOUNT_NUM,
INTERNATIONAL_BANK_ACCOUNT_NUM,
BANK_IDENTIFIER_CD,
FILLER,
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
UNIQUE_PAYMENT_REFERENCE_NUM ,
CASE WHEN PAYMENT_NET_AMT ='' THEN NULL ELSE
CAST(PAYMENT_NET_AMT AS FLOAT64) END AS  PAYMENT_NET_AMT,
CASE WHEN PAYMENT_GROSS_AMT ='' THEN NULL ELSE
CAST(PAYMENT_GROSS_AMT AS FLOAT64) END AS  PAYMENT_GROSS_AMT,
CASE WHEN PAYMENT_DISCOUNT_AMT ='' THEN NULL ELSE
CAST( PAYMENT_DISCOUNT_AMT AS FLOAT64) END AS PAYMENT_DISCOUNT_AMT,
CASE WHEN PAYMENT_SERVICE_FEE_AMT ='' THEN NULL ELSE
CAST(PAYMENT_SERVICE_FEE_AMT AS FLOAT64) END AS PAYMENT_SERVICE_FEE_AMT,
CASE WHEN PAYMENT_ADJUSTMENT_AMT ='' THEN NULL ELSE
CAST(PAYMENT_ADJUSTMENT_AMT AS FLOAT64) END AS PAYMENT_ADJUSTMENT_AMT,
CASE WHEN PAYMENT_TAX_AMT ='' THEN NULL ELSE
CAST(PAYMENT_TAX_AMT AS FLOAT64) END AS PAYMENT_TAX_AMT,
CASE WHEN OPENING_DEBIT_BALANCE_AMT ='' THEN NULL ELSE
CAST(OPENING_DEBIT_BALANCE_AMT AS FLOAT64) END AS OPENING_DEBIT_BALANCE_AMT,
PAYEE_DIRECT_DEPOSIT_NUM, 
BANK_ACCOUNT_NUM,
INTERNATIONAL_BANK_ACCOUNT_NUM,
BANK_IDENTIFIER_CD, 
FILLER,
'v_job_run_id' AS JOB_RUN_ID,
'amex' AS SOURCE_SYSTEM_CD,
'v_job_name' AS CREATED_BY,
CURRENT_DATETIME() AS CREATE_DTTIME
FROM LANDING.WORK_AMEX_SUMMARY_RECORD