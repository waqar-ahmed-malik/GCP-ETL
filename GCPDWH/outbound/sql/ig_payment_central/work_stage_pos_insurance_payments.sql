SELECT DISTINCT POS_TRANSACTION_ID AS POS_TRANSACTION_ID,
max(RECORD_IDENTIFIER) AS RECORD_IDENTIFIER,
MAX(STORE_NUM) AS BRANCH_NUMBER,
MAX(BRANCH_HUB_NUMBER) AS BRANCH_HUB_NUMBER,
'001' AS BRANCH_DEVICE_NUMBER,
MAX(CLUB_CODE) AS CLUB_CODE,
MAX(EMPLOYEE_ID) AS USER_ID,
MAX(TRANSACTION_DATE) AS TRANSACTION_DATE,
MAX(TRANSACTION_TIME) AS TRANSACTION_TIME,
NULL as TRANSACTION_POSTMARK_DATE,
ROUND(SUM(TOTAL_ITEM_SALES_AMOUNT),2) AS TRANSACTION_TOTAL_AMOUNT,
CASE WHEN MAX(CASH_AMOUNT)<>0 THEN 'CASH' ELSE NULL END AS CASH_TRANSACTION_FOP,
MAX(CASH_AMOUNT) AS CASH_FOP_AMOUNT,
'' AS CASH_PAYMENT_CARD_TYPE,
'' AS CASH_PAYMENT_CARD_LAST4,
'' AS CASH_PYMT_CARD_EXPIRATION_MON,
'' AS CASH_PYMT_CARD_EXPIRATION_YR,
'' AS CASH_ORDER_NUMBER,
'' AS CASH_ORDER_SEQUENCE_NUMBER,
'' AS CASH_CHECK_NUMBER,
CASE WHEN MAX(CHECK_AMOUNT)<>0 THEN 'CHCK' ELSE NULL END AS CHCK_TRANSACTION_FOP,
MAX(CHECK_AMOUNT) AS CHCK_FOP_AMOUNT,
'' AS CHCK_PAYMENT_CARD_TYPE,
'' AS CHCK_PAYMENT_CARD_LAST4,
'' AS CHCK_PYMT_CARD_EXPIRATION_MON,
'' AS CHCK_PYMT_CARD_EXPIRATION_YR,
'' AS CHCK_ORDER_NUMBER,
'' AS CHCK_ORDER_SEQUENCE_NUMBER,
'' AS CHCK_CHECK_NUMBER,
CASE WHEN MAX(VISA_AMOUNT)<>0 THEN 'VISA' ELSE NULL END AS  VISA_TRANSACTION_FOP,
MAX(VISA_AMOUNT) AS VISA_FOP_AMOUNT,
CASE WHEN MAX(VISA_AMOUNT)<>0 THEN 'VISA' ELSE NULL END AS VISA_PAYMENT_CARD_TYPE,
'' AS VISA_PAYMENT_CARD_LAST4,
'' AS VISA_PYMT_CARD_EXPIRATION_MON,
'' AS VISA_PYMT_CARD_EXPIRATION_YR,
'' AS VISA_ORDER_NUMBER,
'' AS VISA_ORDER_SEQUENCE_NUMBER,
'' AS VISA_CHECK_NUMBER,
CASE WHEN MAX(MASTERCARD_AMOUNT)<>0 THEN 'MSTR' ELSE NULL END  AS MASTER_TRANSACTION_FOP,
MAX(MASTERCARD_AMOUNT) AS MASTER_FOP_AMOUNT,
CASE WHEN MAX(MASTERCARD_AMOUNT)<>0 THEN 'MSTR' ELSE NULL END  AS MASTER_PAYMENT_CARD_TYPE,
'' AS MASTER_PAYMENT_CARD_LAST4,
'' AS MSTR_PYMT_CARD_EXPIRATION_MON,
'' AS MSTR_PYMT_CARD_EXPIRATION_YR,
'' AS MASTER_ORDER_NUMBER,
'' AS MASTER_ORDER_SEQUENCE_NUMBER,
'' AS MASTER_CHECK_NUMBER,
CASE WHEN MAX(DEBIT_AMOUNT)<>0 THEN 'DEBIT' ELSE NULL END AS DEBIT_TRANSACTION_FOP,
MAX(DEBIT_AMOUNT) AS DEBIT_FOP_AMOUNT,
CASE WHEN MAX(DEBIT_AMOUNT)<>0 THEN 'DEBIT' ELSE NULL END AS DEBIT_PAYMENT_CARD_TYPE,
'' AS DEBIT_PAYMENT_CARD_LAST4,
'' AS DEBIT_PYMT_CARD_EXPIRATION_MON,
'' AS DEBIT_PYMT_CARD_EXPIRATION_YR,
'' AS DEBIT_ORDER_NUMBER,
'' AS DEBIT_ORDER_SEQUENCE_NUMBER,
'' AS DEBIT_CHECK_NUMBER,
MAX(CASE WHEN ITEM_NUM=1 then AGMT_SOURCE_SYSTEM else null end) AS LI1_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=1 then COMPANY_ID else null end) AS LI1_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=1 then PRODUCT_TYPE else null end) AS LI1_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=1 then POLICY_PREFIX else null end) AS LI1_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=1 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI1_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=1 then AGMT_STATE_CODE else null end) AS LI1_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=1 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI1_LINE_ITEM_AMOUNT, -- Need to look
MAX(CASE WHEN ITEM_NUM=2 then AGMT_SOURCE_SYSTEM else null end) AS LI2_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=2 then COMPANY_ID else null end) AS LI2_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=2 then PRODUCT_TYPE else null end) AS LI2_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=2 then POLICY_PREFIX else null end) AS LI2_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=2 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI2_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=2 then AGMT_STATE_CODE else null end) AS LI2_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=2 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI2_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=3 then AGMT_SOURCE_SYSTEM else null end) AS LI3_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=3 then COMPANY_ID else null end) AS LI3_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=3 then PRODUCT_TYPE else null end) AS LI3_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=3 then POLICY_PREFIX else null end) AS LI3_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=3 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI3_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=3 then AGMT_STATE_CODE else null end) AS LI3_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=3 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI3_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=4 then AGMT_SOURCE_SYSTEM else null end) AS LI4_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=4 then COMPANY_ID else null end) AS LI4_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=4 then PRODUCT_TYPE else null end) AS LI4_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=4 then POLICY_PREFIX else null end) AS LI4_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=4 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI4_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=4 then AGMT_STATE_CODE else null end) AS LI4_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=4 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI4_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=5 then AGMT_SOURCE_SYSTEM else null end) AS LI5_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=5 then COMPANY_ID else null end) AS LI5_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=5 then PRODUCT_TYPE else null end) AS LI5_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=5 then POLICY_PREFIX else null end) AS LI5_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=5 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI5_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=5 then AGMT_STATE_CODE else null end) AS LI5_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=5 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI5_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=6 then AGMT_SOURCE_SYSTEM else null end) AS LI6_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=6 then COMPANY_ID else null end) AS LI6_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=6 then PRODUCT_TYPE else null end) AS LI6_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=6 then POLICY_PREFIX else null end) AS LI6_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=6 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI6_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=6 then AGMT_STATE_CODE else null end) AS LI6_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=6 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI6_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=7 then AGMT_SOURCE_SYSTEM else null end) AS LI7_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=7 then COMPANY_ID else null end) AS LI7_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=7 then PRODUCT_TYPE else null end) AS LI7_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=7 then POLICY_PREFIX else null end) AS LI7_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=7 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI7_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=7 then AGMT_STATE_CODE else null end) AS LI7_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=7 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI7_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=8 then AGMT_SOURCE_SYSTEM else null end) AS LI8_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=8 then COMPANY_ID else null end) AS LI8_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=8 then PRODUCT_TYPE else null end) AS LI8_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=8 then POLICY_PREFIX else null end) AS LI8_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=8 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI8_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=8 then AGMT_STATE_CODE else null end) AS LI8_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=8 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI8_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=9 then AGMT_SOURCE_SYSTEM else null end) AS LI9_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=9 then COMPANY_ID else null end) AS LI9_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=9 then PRODUCT_TYPE else null end) AS LI9_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=9 then POLICY_PREFIX else null end) AS LI9_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=9 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI9_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=9 then AGMT_STATE_CODE else null end) AS LI9_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=9 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI9_LINE_ITEM_AMOUNT,
MAX(CASE WHEN ITEM_NUM=10 then AGMT_SOURCE_SYSTEM else null end) AS LI10_AGMT_SOURCE_SYSTEM,
MAX(CASE WHEN ITEM_NUM=10 then COMPANY_ID else null end) AS LI10_COMPANY_ID,
MAX(CASE WHEN ITEM_NUM=10 then PRODUCT_TYPE else null end) AS LI10_PRODUCT_TYPE,
MAX(CASE WHEN ITEM_NUM=10 then POLICY_PREFIX else null end) AS LI10_POLICY_PREFIX,
MAX(CASE WHEN ITEM_NUM=10 then ITEM_SPECIAL_ORDER_NUM else null end) AS LI10_AGMT_NUMBER,
MAX(CASE WHEN ITEM_NUM=10 then AGMT_STATE_CODE else null end) AS LI10_AGMT_STATE_CODE,
MAX(CASE WHEN ITEM_NUM=10 then TOTAL_ITEM_SALES_AMOUNT else null end) AS LI10_LINE_ITEM_AMOUNT,
max(CHECK_AMOUNT) as ADJUSTMENT_CHECK_AMT,
max(CASH_AMOUNT) as POLICYADJUSTMENT_CASH_AMT,
0 as POLICYADJUSTMENT_CRDT_AMT
from
(SELECT 
        POS_TRANSACTION_DETAIL.POS_TRANSACTION_ID AS POS_TRANSACTION_ID,
        'DTL' AS RECORD_IDENTIFIER,
        NULL AS BRANCH_HUB_NUMBER, -- Need to map this column
        '005' AS CLUB_CODE,
         POS_TRANSACTION_DETAIL.TRANSACTION_NUM AS TRANSACTION_NUM,
        ROW_NUMBER() OVER (PARTITION BY POS_TRANSACTION_DETAIL.POS_TRANSACTION_ID ORDER BY POS_TRANSACTION_DETAIL.ITEM_NUM) AS ITEM_NUM,
        POS_TRANSACTION_DETAIL.LOCATION_ID AS STORE_NUM,
        POS_TRANSACTION_DETAIL.REGISTER_NUM AS REGISTER_NUM,
        POS_TRANSACTION_DETAIL.EMPLOYEE_ID AS EMPLOYEE_ID,
        FORMAT_DATETIME('%Y%m%d',POS_TRANSACTION_DETAIL.TRANSACTION_DTTIME) AS TRANSACTION_DATE,
        FORMAT_DATETIME('%H%M%S',POS_TRANSACTION_DETAIL.TRANSACTION_DTTIME) AS TRANSACTION_TIME,
        POS_TRANSACTION_DETAIL.TRANSACTION_DTTIME AS TRANSACTION_DTTIME,
        POS_TRANSACTION_DETAIL.TOTAL_ITEM_SALES_AMT AS TOTAL_ITEM_SALES_AMOUNT,
        POS_TRANSACTION_DETAIL.ITEM_PASSTHROUGH_DATA AS ITEM_PASSTHROUGH_DATA,
        POS_TRANSACTION_DETAIL.ITEM_SPECIAL_ORDER_NUM AS ITEM_SPECIAL_ORDER_NUM,
        POS_TRANSACTION_DETAIL.CREATE_DTTIME AS LAST_UPDATE_DTTIME,
        POS_TENDER_SUMMARY.POS_TRANSACTION_ID AS POS_TRANSACTION_ID_SUMMARY,
        POS_TENDER_SUMMARY.VISA_AMT AS VISA_AMOUNT,
        POS_TENDER_SUMMARY.MASTERCARD_AMT AS MASTERCARD_AMOUNT,
        POS_TENDER_SUMMARY.DEBIT_AMT AS DEBIT_AMOUNT,
        POS_TENDER_SUMMARY.CASH_AMT AS CASH_AMOUNT,
        POS_TENDER_SUMMARY.CHECK_AMT AS CHECK_AMOUNT,
        POS_TRANSACTION_DETAIL.CREATE_DTTIME,
        --POS_TENDER_SUMMARY.CREATE_DTTIME AS LAST_UPDATE_DTTIME_SUMMARY,
        REGEXP_EXTRACT(ITEM_PASSTHROUGH_DATA,r"insurerName=(\w+)") AS COMPANY_ID,
        REGEXP_EXTRACT(ITEM_PASSTHROUGH_DATA,r"riskState=(\w+)") AS AGMT_STATE_CODE,
        REGEXP_EXTRACT(ITEM_PASSTHROUGH_DATA,r"productTypeCode=(\w+)") AS PRODUCT_TYPE,
        REGEXP_EXTRACT(ITEM_PASSTHROUGH_DATA,r"sourceSystem=(\w+)") AS AGMT_SOURCE_SYSTEM,
          REGEXP_EXTRACT(ITEM_PASSTHROUGH_DATA,r"policyPrefix=(\w+)") AS POLICY_PREFIX       
FROM CUSTOMER_PRODUCT.POS_TRANSACTION_DETAIL_DIM POS_TRANSACTION_DETAIL,CUSTOMER_PRODUCT.POS_TENDER_SUMMARY_DIM POS_TENDER_SUMMARY,REFERENCE.CONNECTSUITE_PRODUCT_DIM  PC
WHERE POS_TRANSACTION_DETAIL.POS_TRANSACTION_ID=POS_TENDER_SUMMARY.POS_TRANSACTION_ID 
AND UPPER(TRIM(POS_TRANSACTION_DETAIL.ITEM_SKU))=UPPER(TRIM(PC.SKU)) 
AND PC.PAYMENT_CENTRAL_FLAG='Y' 
AND POS_TRANSACTION_DETAIL.LINE_ITEM_VOID_FLAG = 'N' 
AND POS_TRANSACTION_DETAIL.FULL_TICKET_VOID_FLAG = 'N' 
AND POS_TRANSACTION_DETAIL.RETURN_FLAG='N'
AND POS_TENDER_SUMMARY.SKU_COMPANY='IG'
)
WHERE  CREATE_DTTIME > '{{ prev_execution_date }}'
group by POS_TRANSACTION_ID