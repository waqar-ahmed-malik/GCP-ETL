MERGE OPERATIONAL.HOUSE_MANAGER_PAYMENTS AS TGT
USING (select * from LANDING.WORK_ZUORA_PAYMENTS WHERE DATE(PAYMENT_UPDATED_DT) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS SRC
ON TGT.PAYMENT_NUM = SRC.PAYMENT_NUM and
 TGT.PAYMENT_ID = SRC.ID 
 WHEN MATCHED THEN UPDATE SET 
 TGT.REFERENCED_PAYMENT_ID = SRC.REFERENCED_PAYMENT_ID
,TGT.REFERENCE_ID = SRC.REFERENCE_ID
,TGT.ACCOUNTING_CD = SRC.ACCOUNTING_CD
,TGT.PAYMENT_AMT = SAFE_CAST(SRC.AMOUNT AS FLOAT64)
,TGT.APPLIED_AMT = SAFE_CAST(SRC.APPLIED_AMT AS FLOAT64)
,TGT.APPLIED_CREDIT_BALANCE_AMT = SAFE_CAST(SRC.APPLIED_CREDIT_BALANCE_AMT AS FLOAT64)
,TGT.AUTHORIZED_TRANSACTION_ID = SRC.AUTHORIZED_TRANSACTION_ID
,TGT.BANK_IDENTIFICATION_NUM = SRC.BANK_IDENTIFICATION_NUM
,TGT.CANCELLED_DT = DATE(SRC.CANCELLED_DT)
,TGT.COMMENT = SRC.COMMENT
,TGT.CURRENCY = SRC.CURRENCY
,TGT.EFFECTIVE_DT = DATE(SRC.EFFECTIVE_DT)
,TGT.GATEWAY = SRC.GATEWAY
,TGT.GATEWAY_ORDER_ID = SRC.GATEWAY_ORDER_ID
,TGT.GATEWAY_RESPONSE = SRC.GATEWAY_RESPONSE
,TGT.GATEWAY_RESPONSE_CD = SRC.GATEWAY_RESPONSE_CD
,TGT.GATEWAY_STATE = SRC.GATEWAY_STATE
,TGT.MARKED_FOR_SUBMISSION_ON = SRC.MARKED_FOR_SUBMISSION_ON
,TGT.REFUND_AMT = SAFE_CAST(SRC.REFUND_AMT AS FLOAT64)
,TGT.SECOND_PAYMENT_REFERENCE_ID = SRC.SECOND_PAYMENT_REFERENCE_ID
,TGT.SETTLED_ON_DTTIME = SRC.SETTLED_ON
,TGT.SOFT_DESCRIPTOR = SRC.SOFT_DESCRIPTOR
,TGT.SOFT_DESCRIPTOR_PHONE_NUM = SRC.SOFT_DESCRIPTOR_PHONE_NUM
,TGT.SOURCE = SRC.SOURCE
,TGT.SOURCE_NM = SRC.SOURCE_NM
,TGT.STATUS = SRC.STATUS
,TGT.SUBMITTED_ON_DTTIME = SRC.SUBMITTED_ON
,TGT.TRANSFERRED_TO_ACCOUNTING = SRC.TRANSFERRED_TO_ACCOUNTING
,TGT.UNAPPLIED_AMT = SAFE_CAST(SRC.UNAPPLIED_AMT AS FLOAT64)
,TGT.PAYMENT_TYPE = SRC.PAYMENT_TYPE
,TGT.CREDIT_CARD_TYPE = SRC.CREDIT_CARD_TYPE
,TGT.PAYMENT_METHOD_TYPE = SRC.PAYMENT_METHOD_TYPE
,TGT.CREATED_BY_ID = SRC.CREATED_BY_ID
,TGT.SOURCE_CREATED_DTTIME = SRC.CREATED_DT
,TGT.PAYMENT_UPDATED_BY_ID = SRC.PAYMENT_UPDATED_BY_ID
,TGT.SOURCE_PAYMENT_UPDATED_DTTIME = SRC.PAYMENT_UPDATED_DT
,TGT.UPDATE_DTTIME =  CURRENT_DATETIME()
WHEN NOT MATCHED THEN INSERT 
(PAYMENT_ID
,PAYMENT_NUM
,REFERENCED_PAYMENT_ID
,REFERENCE_ID
,ACCOUNTING_CD
,PAYMENT_AMT
,APPLIED_AMT
,APPLIED_CREDIT_BALANCE_AMT
,AUTHORIZED_TRANSACTION_ID
,BANK_IDENTIFICATION_NUM
,CANCELLED_DT
,COMMENT
,CURRENCY
,EFFECTIVE_DT
,GATEWAY
,GATEWAY_ORDER_ID
,GATEWAY_RESPONSE
,GATEWAY_RESPONSE_CD
,GATEWAY_STATE
,MARKED_FOR_SUBMISSION_ON
,REFUND_AMT
,SECOND_PAYMENT_REFERENCE_ID
,SETTLED_ON_DTTIME
,SOFT_DESCRIPTOR
,SOFT_DESCRIPTOR_PHONE_NUM
,SOURCE
,SOURCE_NM
,STATUS
,SUBMITTED_ON_DTTIME
,TRANSFERRED_TO_ACCOUNTING
,UNAPPLIED_AMT
,PAYMENT_TYPE
,CREDIT_CARD_TYPE
,PAYMENT_METHOD_TYPE
,CREATED_BY_ID
,SOURCE_CREATED_DTTIME
,PAYMENT_UPDATED_BY_ID
,SOURCE_PAYMENT_UPDATED_DTTIME
,SOURCE_SYSTEM_CD
,CREATE_DTTIME
,UPDATE_DTTIME)
VALUES(
ID 
,PAYMENT_NUM
,REFERENCED_PAYMENT_ID
,REFERENCE_ID
,ACCOUNTING_CD
,SAFE_CAST(AMOUNT AS FLOAT64) 
,SAFE_CAST(APPLIED_AMT AS FLOAT64)
,SAFE_CAST(APPLIED_CREDIT_BALANCE_AMT AS FLOAT64)
,AUTHORIZED_TRANSACTION_ID
,BANK_IDENTIFICATION_NUM
,DATE(CANCELLED_DT)
,COMMENT
,CURRENCY
,DATE(EFFECTIVE_DT)
,GATEWAY
,GATEWAY_ORDER_ID
,GATEWAY_RESPONSE
,GATEWAY_RESPONSE_CD
,GATEWAY_STATE
,MARKED_FOR_SUBMISSION_ON
,SAFE_CAST(REFUND_AMT AS FLOAT64)
,SECOND_PAYMENT_REFERENCE_ID
,SETTLED_ON
,SOFT_DESCRIPTOR
,SOFT_DESCRIPTOR_PHONE_NUM
,SOURCE
,SOURCE_NM
,STATUS
,SUBMITTED_ON
,TRANSFERRED_TO_ACCOUNTING
,SAFE_CAST(UNAPPLIED_AMT AS FLOAT64)
,PAYMENT_TYPE
,CREDIT_CARD_TYPE
,PAYMENT_METHOD_TYPE
,CREATED_BY_ID
,CREATED_DT
,PAYMENT_UPDATED_BY_ID
,PAYMENT_UPDATED_DT
,'HOUSE_MANAGER'
,CURRENT_DATETIME()
,CURRENT_DATETIME())