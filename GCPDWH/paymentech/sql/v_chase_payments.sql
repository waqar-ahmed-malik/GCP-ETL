(SELECT
RECORD_TYPE,
SUBMISSION_DT,
PID,
PID_SHORT_NM AS PID_SHORT_NM,
SUBMISSION_NUM,
RECORD_NUM,
ENTITY_TYPE,
ENTITY_NUM,
SETTLEMENT_CURRENCY,
MERCHANT_ORDER_NUM,
(case when MERCHANT_ORDER_NUM like 'M%'
THEN 'CONNECTSUITE'
ELSE 'OTHER'
END) as MERCHANT_ORDER_SOURCE,
RDFI_NUM,
ACCOUNT_NUM,
EXPIRATION_DT,
AMOUNT,
MOP,
ACTION_CD,
AUTH_DT,
AUTH_CD,
AUTH_RESPONSE_CD,
TRACE_NUM,
RESERVED,
MCC,
TOKEN_IND,
CASH_BACK_AMT,
SURCHARGE_AMT,
VOUCHER_NUM,
EBT_ACCOUNT_TYPE,
INTERCHANGE_QUALIFICATION_CD,
DURBIN_REGULATED,
INTERCHANGE_UNIT_FEE,
INTERCHANGE_PERCENTAGE_FEE,
TOTAL_INTERCHANGE_AMT,
TOTAL_ASSESSMENT_AMT,
OTHR_DEBIT_PASSTHROUGH_FEE AS OTHER_DEBIT_PASSTHROUGH_FEE,
PRESENTMENT_CURRENCY,
CONSUMER_COUNTRY_CD,
"" AS CATEGORY,
"" AS STATUS_FLAG,
SAFE_CAST(NULL AS INT64) AS SEQUENCE_NUM,
"" AS REASON_CD,
SAFE_CAST(NULL AS DATE) AS TRANSACTION_DT,
SAFE_CAST(NULL AS DATE) AS ECP_RETURN_DT,
SAFE_CAST(NULL AS DATE) AS ACTIVITY_DT,
NULL AS ECP_RETURN_AMT,
"" AS USAGE_CD,
"" AS CONSUMER_NM,
"" AS IBAN,
"N" AS PROCESSED_IND,
HPID,
'' AS MEMBER_NUM,
NULL AS MEMBERSHIP_PAYMENT_KEY,
"CHASE"	AS SOURCE_SYSTEM_CD,
CURRENT_DATE() AS CREATE_DTTIME,
"ETL" AS CREATE_BY,
"PAYMENTS" AS DATA_SOURCE
FROM `aaadata-181822.CUSTOMER_PRODUCT.CHASE_PAYMENTS`)
UNION ALL
(SELECT
RECORD_TYPE,
SAFE_CAST(NULL AS DATE) AS SUBMISSION_DT,
SAFE_CAST(NULL  AS INT64) AS PID,
"" AS PID_SHORT_NM,
"" AS SUBMISSION_NUM,
SAFE_CAST(NULL  AS INT64) AS RECORD_NUM,
ENTITY_TYPE,
SAFE_CAST(NULL  AS INT64) AS ENTITY_NUM,
"" AS SETTLEMENT_CURRENCY,
MERCHANT_ORDER_NUM,
(case when MERCHANT_ORDER_NUM like 'M%'
THEN 'CONNECTSUITE'
ELSE 'OTHER'
END) as MERCHANT_ORDER_SOURCE,
"" AS RDFI_NUM,
ACCOUNT_NUM,
"" AS EXPIRATION_DT,
SAFE_CAST(NULL  AS FLOAT64) AS AMOUNT,
MOP,
"" AS ACTION_CD,
SAFE_CAST(NULL AS DATE) AS AUTH_DT,
"" AS AUTH_CD,
SAFE_CAST(NULL  AS INT64) AS AUTH_RESPONSE_CD,
"" AS TRACE_NUM,
"" AS RESERVED,
SAFE_CAST(NULL  AS INT64) AS MCC,
"" AS TOKEN_IND,
SAFE_CAST(NULL  AS FLOAT64) AS CASH_BACK_AMT,
SAFE_CAST(NULL  AS FLOAT64) AS SURCHARGE_AMT,
"" AS VOUCHER_NUM,
"" AS EBT_ACCOUNT_TYPE,
"" AS INTERCHANGE_QUALIFICATION_CD,
"" AS DURBIN_REGULATED,
SAFE_CAST(NULL  AS FLOAT64) AS INTERCHANGE_UNIT_FEE,
SAFE_CAST(NULL  AS FLOAT64) AS INTERCHANGE_PERCENTAGE_FEE,
SAFE_CAST(NULL  AS FLOAT64) AS TOTAL_INTERCHANGE_AMT,
SAFE_CAST(NULL  AS FLOAT64) AS TOTAL_ASSESSMENT_AMT,
"" AS OTHR_DEBIT_PASSTHROUGH_FEE,
CURRENCY,
CONSUMER_BANK_COUNTRY_CD,
CATEGORY,
STATUS_FLAG,
SEQUENCE_NUM,
REASON_CD,
TRANSACTION_DT,
ECP_RETURN_DT,
ACTIVITY_DT,
ECP_RETURN_AMT,
USAGE_CD,
CONSUMER_NM,
IBAN,
"N" AS PROCESSED_IND,
"" AS HPID,
M.MEMBER_NUM AS MEMBER_NUM,
M.MEMBERSHIP_PAYMENT_KEY AS MEMBERSHIP_PAYMENT_KEY,
"CHASE"	AS SOURCE_SYSTEM_CD,
CURRENT_DATE() AS CREATE_DTTIME,
"ETL" AS CREATE_BY,
"FAILURES" AS DATA_SOURCE
FROM `aaadata-181822.CUSTOMER_PRODUCT.CHASE_PAYMENT_FAILURES` F
LEFT OUTER JOIN
(SELECT ORDER_NUMBER,
MEMBERSHIP_PAYMENT_KEY,
CONCAT('429',LPAD(CAST(MEMBERSHIP_ID AS STRING),8,'0'), ASSOCIATE_ID) AS MEMBER_NUM
FROM `aaadata-181822.LANDING.CONNECTSUITE_E_PAYMENT_HISTORY` E,
`aaadata-181822.CUSTOMERS.CONNECTSUITE_MEMBER_DIM` M
WHERE MBRS_KY = MEMBERSHIP_KEY
AND SUBSTR(ASSOCIATE_ID,1,1)='1') M
ON F.MERCHANT_ORDER_NUM = M.ORDER_NUMBER)