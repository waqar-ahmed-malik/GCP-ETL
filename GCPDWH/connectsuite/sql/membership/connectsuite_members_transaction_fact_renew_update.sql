UPDATE  CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_TRANSACTIONS_FACT A
SET A.MAQ_IND = 'Service'
WHERE SOURCE_SYSTEM_CD <> 'ARIA_SFDC'
AND A.MAQ_IND = 'Renew'
AND TRANSACTION_TYPE_CD  = 'RENEW'
AND exists (select 1 from  LANDING.WORK_CS_MEMBER_TRANSACTIONS_FACT B
WHERE A.MEMBERSHIP_NUM = B.MEMBERSHIP_NUM
AND CAST(A.ASSOCIATE_ID AS STRING) =  B.ASSOCIATE_ID
AND TRANSACTION_TYPE_CD = 'REVERSE PAYMENT'
and A.TERM_EXPIRATION_DT = DATE(PARSE_DATETIME('%Y-%m-%d %T',   TRIM(B.TERM_EXPIRATION_DT)))
and A.TRANSACTION_DUE_AMT = CAST(B.TRANSACTION_DUE_AMT AS FLOAT64)
AND cast(B.SOURCE_TRANSACTION_ID as int64) > cast(A.SOURCE_TRANSACTION_ID as int64));