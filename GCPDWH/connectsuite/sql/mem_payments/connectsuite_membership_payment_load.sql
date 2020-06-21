MERGE INTO OPERATIONAL.CONNECTSUITE_MEMBERSHIP_PAYMENT M
USING LANDING.WORK_CS_MEMBERSHIP_PAYMENT W
ON M.MEMBERSHIP_PAYMENT_KEY=SAFE_CAST(W.MEMBERSHIP_PAYMENT_KEY AS INT64)
WHEN NOT MATCHED THEN
INSERT
(
MEMBERSHIP_PAYMENT_KEY,
MEMBERSHIP_PAYMENT_AMT,
MEMBERSHIP_PAYMENT_BATCH_NM,
MEMBERSHIP_PAYMENT_METHOD_CD,
MEMBERSHIP_PAYMENT_CC_TYPE_CD,
MEMBERSHIP_PAYMENT_CC_EXPIRE_DTTIME,
MEMBERSHIP_PAYMENT_CC_NUM,
MEMBERSHIP_PAYMENT_DTTIME,
MEMBERSHIP_PAYMENT_TRAN_TYPE_CD,
MEMBERSHIP_PAYMENT_ADJ_DESC_CD,
MEMBERSHIP_PAYMENT_CC_AUTH_NUM,
MEMBERSHIP_PAYMENT_SRC_CD,
MEMBERSHIP_PAYMENT_CREATE_DT,
MEMBERSHIP_PAYMENT_WO_AMT,
MEMBERSHIP_PAYMENT_CC_OWNER_IND,
MEMBERSHIP_PAYMENT_BATCH_KEY,
MEMBERSHIP_PAYMENT_FROM_CD,
MEMBERSHIP_PAYMENT_BILL_KEY,
MEMBERSHIP_PAYMENT_TRAN_APPLY_AMT,
MEMBERSHIP_BALANCE_KEY,
MEMBERSHIP_KEY,
MEMBERSHIP_PAYMENT_REV_PMT_AMT,
MEMBERSHIP_PAYMENT_SAFE_AMT,
MEMBERSHIP_PAYMENT_CHK_NUM,
MEMBERSHIP_PAYMENT_TYPE_KEY,
CUSTOMER_RECIPT_NUM,
MEMBERSHIP_PAYMENT_REFUND_METH_CD,
MEMBERSHIP_PAYMENT_DONOR_KEY,
OFFICE_CD,
POSTED_BY_USERID,
MEMBERSHIP_PAYMENT_CC_NR_KEY_ID,
MEMBERSHIP_PAYMENT_CC_SETTLED_IND,
MEMBERSHIP_PAYMENT_CANCEL_IN_PROG_IND,
MEMBERSHIP_PAYMENT_EXTERN_ID,
MEMBERSHIP_PAYMENT_REV_KEY,
BILL_PAYMENT_KEY,
SERVICE_CHARGE,
ETL_JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DTTIME,
CREATED_BY,
UPDATE_DTTIME
)
VALUES(
CAST(MEMBERSHIP_PAYMENT_KEY AS INT64),
CAST(MEMBERSHIP_PAYMENT_AMT AS FLOAT64),
MEMBERSHIP_PAYMENT_BATCH_NM,
MEMBERSHIP_PAYMENT_METHOD_CD,
MEMBERSHIP_PAYMENT_CC_TYPE_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',MEMBERSHIP_PAYMENT_CC_EXPIRE_DT),
MEMBERSHIP_PAYMENT_CC_NUM,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',MEMBERSHIP_PAYMENT_DT),
MEMBERSHIP_PAYMENT_TRAN_TYPE_CD,
MEMBERSHIP_PAYMENT_ADJ_DESC_CD,
MEMBERSHIP_PAYMENT_CC_AUTH_NR,
MEMBERSHIP_PAYMENT_SRC_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',MEMBERSHIP_PAYMENT_CREATE_DT),
CAST(MEMBERSHIP_PAYMENT_WO_AMT AS FLOAT64),
MEMBERSHIP_PAYMENT_CC_OWNER_IND,
CAST(MEMBERSHIP_PAYMENT_BATCH_KEY AS INT64),
MEMBERSHIP_PAYMENT_FROM_CD,
CAST(MEMBERSHIP_PAYMENT_BILL_KEY AS INT64),
CAST(MEMBERSHIP_PAYMENT_TRAN_APPLY_AMT AS FLOAT64),
CAST(MEMBERSHIP_BALANCE_KEY AS INT64),
CAST(MEMBERSHIP_KEY AS INT64),
CAST(MEMBERSHIP_PAYMENT_REV_PMT_AMT AS FLOAT64),
CAST(MEMBERSHIP_PAYMENT_SAFE_AMT AS FLOAT64),
MEMBERSHIP_PAYMENT_CHK_NR,
CAST(MEMBERSHIP_PAYMENT_TYPE_KEY AS INT64),
CAST(CUSTOMER_RECIPT_NR AS INT64),
MEMBERSHIP_PAYMENT_REFUND_METH_CD,
CAST(MEMBERSHIP_PAYMENT_DONOR_KEY AS INT64),
OFFICE_CD,
POSTED_BY_USERID,
CAST(MEMBERSHIP_PAYMENT_CC_NR_KEY_ID AS INT64),
MEMBERSHIP_PAYMENT_CC_SETTLED_IND,
MEMBERSHIP_PAYMENT_CANCEL_IN_PROG_IND,
CAST(MEMBERSHIP_PAYMENT_EXTERN_ID AS INT64),
CAST(MEMBERSHIP_PAYMENT_REV_KEY AS INT64),
CAST(BILL_PAYMENT_KEY AS INT64),
CAST(SERVICE_CHARGE AS FLOAT64),
CAST('jobrunid' AS INT64),
'CONNECT SUITE',
CURRENT_DATETIME(),
'jobname',
CURRENT_DATETIME()
)
WHEN MATCHED THEN 
UPDATE
SET
MEMBERSHIP_PAYMENT_AMT = CAST(W.MEMBERSHIP_PAYMENT_AMT AS FLOAT64),
MEMBERSHIP_PAYMENT_BATCH_NM = W.MEMBERSHIP_PAYMENT_BATCH_NM,
MEMBERSHIP_PAYMENT_METHOD_CD = W.MEMBERSHIP_PAYMENT_METHOD_CD,
MEMBERSHIP_PAYMENT_CC_TYPE_CD = W.MEMBERSHIP_PAYMENT_CC_TYPE_CD,
MEMBERSHIP_PAYMENT_CC_EXPIRE_DTTIME = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',W.MEMBERSHIP_PAYMENT_CC_EXPIRE_DT),
MEMBERSHIP_PAYMENT_CC_NUM = W.MEMBERSHIP_PAYMENT_CC_NUM,
MEMBERSHIP_PAYMENT_DTTIME = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',W.MEMBERSHIP_PAYMENT_DT),
MEMBERSHIP_PAYMENT_TRAN_TYPE_CD = W.MEMBERSHIP_PAYMENT_TRAN_TYPE_CD,
MEMBERSHIP_PAYMENT_ADJ_DESC_CD = W.MEMBERSHIP_PAYMENT_ADJ_DESC_CD,
MEMBERSHIP_PAYMENT_CC_AUTH_NUM = W.MEMBERSHIP_PAYMENT_CC_AUTH_NR,
MEMBERSHIP_PAYMENT_SRC_CD = W.MEMBERSHIP_PAYMENT_SRC_CD,
MEMBERSHIP_PAYMENT_CREATE_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',W.MEMBERSHIP_PAYMENT_CREATE_DT),
MEMBERSHIP_PAYMENT_WO_AMT = CAST(W.MEMBERSHIP_PAYMENT_WO_AMT AS FLOAT64),
MEMBERSHIP_PAYMENT_CC_OWNER_IND =W.MEMBERSHIP_PAYMENT_CC_OWNER_IND,
MEMBERSHIP_PAYMENT_BATCH_KEY = CAST(W.MEMBERSHIP_PAYMENT_BATCH_KEY AS INT64),
MEMBERSHIP_PAYMENT_FROM_CD = W.MEMBERSHIP_PAYMENT_FROM_CD,
MEMBERSHIP_PAYMENT_BILL_KEY = CAST(W.MEMBERSHIP_PAYMENT_BILL_KEY AS INT64),
MEMBERSHIP_PAYMENT_TRAN_APPLY_AMT = CAST(W.MEMBERSHIP_PAYMENT_TRAN_APPLY_AMT AS FLOAT64),
MEMBERSHIP_BALANCE_KEY = CAST(W.MEMBERSHIP_BALANCE_KEY AS INT64),
MEMBERSHIP_KEY = CAST(W.MEMBERSHIP_KEY AS INT64),
MEMBERSHIP_PAYMENT_REV_PMT_AMT = CAST(W.MEMBERSHIP_PAYMENT_REV_PMT_AMT AS FLOAT64),
MEMBERSHIP_PAYMENT_SAFE_AMT = CAST(W.MEMBERSHIP_PAYMENT_SAFE_AMT AS FLOAT64),
MEMBERSHIP_PAYMENT_CHK_NUM = W.MEMBERSHIP_PAYMENT_CHK_NR,
MEMBERSHIP_PAYMENT_TYPE_KEY = CAST(W.MEMBERSHIP_PAYMENT_TYPE_KEY AS INT64),
CUSTOMER_RECIPT_NUM = CAST(W.CUSTOMER_RECIPT_NR AS INT64),
MEMBERSHIP_PAYMENT_REFUND_METH_CD = W.MEMBERSHIP_PAYMENT_REFUND_METH_CD,
MEMBERSHIP_PAYMENT_DONOR_KEY = CAST(W.MEMBERSHIP_PAYMENT_DONOR_KEY AS INT64),
OFFICE_CD = W.OFFICE_CD,
POSTED_BY_USERID = W.POSTED_BY_USERID,
MEMBERSHIP_PAYMENT_CC_NR_KEY_ID = CAST(W.MEMBERSHIP_PAYMENT_CC_NR_KEY_ID AS INT64),
MEMBERSHIP_PAYMENT_CC_SETTLED_IND = W.MEMBERSHIP_PAYMENT_CC_SETTLED_IND,
MEMBERSHIP_PAYMENT_CANCEL_IN_PROG_IND = W.MEMBERSHIP_PAYMENT_CANCEL_IN_PROG_IND,
MEMBERSHIP_PAYMENT_EXTERN_ID = CAST(W.MEMBERSHIP_PAYMENT_EXTERN_ID AS INT64),
MEMBERSHIP_PAYMENT_REV_KEY = CAST(W.MEMBERSHIP_PAYMENT_REV_KEY AS INT64),
BILL_PAYMENT_KEY = CAST(W.BILL_PAYMENT_KEY AS INT64),
SERVICE_CHARGE = CAST(W.SERVICE_CHARGE AS FLOAT64),
ETL_JOB_RUN_ID = CAST('jobrunid' AS INT64),
SOURCE_SYSTEM_CD = 'CONNECT SUITE',
CREATED_BY = 'jobname',
UPDATE_DTTIME = CURRENT_DATETIME()
