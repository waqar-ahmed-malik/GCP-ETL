MERGE
  OPERATIONAL.GLOBALWARE_PAYMENTS A
USING
  LANDING.WORK_GW_PAYMENTS B
ON
  A.ID = SAFE_CAST(B.ID AS INT64)
  WHEN MATCHED  THEN  UPDATE   SET  A.PAYMENT_SOURCE = B.PAYMENT_SOURCE, 
  A.ACCOUNT_ID = B.ACCOUNT_ID,  A.PROVIDER = B.PROVIDER,  
  A.CUST_DUE_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.CUST_DUE_DT ,0,10) ) ,  
  A.CUST_RCVD_AMT = SAFE_CAST(B.CUST_RCVD_AMT AS FLOAT64),  
  A.CUST_DUE_AMT = SAFE_CAST(B.CUST_DUE_AMT AS FLOAT64),  
  A.CUST_RCVD_FLG = B.CUST_RCVD_FLG,  
  A.CUST_RCVD_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.CUST_RCVD_DT ,0,10) ) ,  
  A.PROV_DUE_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.PROV_DUE_DT ,0,10) ) ,  
  A.PROV_DUE_AMT = SAFE_CAST(B.PROV_DUE_AMT AS FLOAT64),  
  A.PROV_PAID_AMT = SAFE_CAST(B.PROV_PAID_AMT AS FLOAT64),  
  A.PROV_PAID_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.PROV_PAID_DT ,0,10) ) , 
  A.PROV_PAID_FLG = B.PROV_PAID_FLG, 
  A.COMM_DUE_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.COMM_DUE_DT ,0,10) ) , 
  A.COMM_DUE_AMT = SAFE_CAST(B.COMM_DUE_AMT AS FLOAT64), 
  A.COMM_RCVD_AMT = SAFE_CAST(B.COMM_RCVD_AMT AS FLOAT64), 
  A.COMM_RCVD_DT = PARSE_DATE("%Y-%m-%d",SUBSTR(B.COMM_RCVD_DT ,0,10) ) , 
  A.COMM_RCVD_FLG = B.COMM_RCVD_FLG, 
  A.PAYMENT_COMMENT = B.PAYMENT_COMMENT, 
 A.PROPERTY = B.PROPERTY,  
  A.TAG = B.TAG,  
  A.TEMP_AMT = SAFE_CAST(B.TEMP_AMT AS FLOAT64),  A.GL_BRANCH = B.GL_BRANCH,
  A.ORIG_RCPT_NUM = SAFE_CAST(B.ORIG_RCPT_NUM AS INT64),  
  A.PREV_BILLED = B.PREV_BILLED, 
  A.GROUP_ID = B.GROUP_ID,  
  A.CREATE_DTTIME = PARSE_DATETIME("%Y-%m-%d %H:%M:%S",B.CREATE_DTTIME)
  WHEN NOT MATCHED
  THEN
INSERT
  ( ID,
    PAYMENT_SOURCE,
    ACCOUNT_ID,
    PROVIDER,
    CUST_DUE_DT,
    CUST_RCVD_AMT,
    CUST_DUE_AMT,
    CUST_RCVD_FLG,
    CUST_RCVD_DT,
    PROV_DUE_DT,
    PROV_DUE_AMT,
    PROV_PAID_AMT,
    PROV_PAID_DT,
    PROV_PAID_FLG,
    COMM_DUE_DT,
    COMM_DUE_AMT,
    COMM_RCVD_AMT,
    COMM_RCVD_DT,
    COMM_RCVD_FLG,
    PAYMENT_COMMENT,
    PROPERTY,
    TAG,
    TEMP_AMT,
    GL_BRANCH,
    ORIG_RCPT_NUM,
    PREV_BILLED,
    GROUP_ID,
    CREATE_DTTIME,
    JOB_RUN_ID,
    SOURCE_SYSTEM_CD,
    UPDATE_DTTIME )
VALUES
  ( SAFE_CAST(ID AS INT64), PAYMENT_SOURCE, ACCOUNT_ID, 
  PROVIDER,PARSE_DATE("%Y-%m-%d",SUBSTR( CUST_DUE_DT ,0,10) ) , 
  SAFE_CAST( CUST_RCVD_AMT AS FLOAT64), 
  SAFE_CAST( CUST_DUE_AMT AS FLOAT64), 
  CUST_RCVD_FLG, PARSE_DATE("%Y-%m-%d",SUBSTR(CUST_RCVD_DT ,0,10) ) ,
 PARSE_DATE("%Y-%m-%d",SUBSTR(PROV_DUE_DT,0,10) ) ,
 SAFE_CAST( PROV_DUE_AMT AS FLOAT64), 
  SAFE_CAST( PROV_PAID_AMT AS FLOAT64),
 PARSE_DATE("%Y-%m-%d",SUBSTR( PROV_PAID_DT ,0,10) ) , 
  PROV_PAID_FLG, 
  PARSE_DATE("%Y-%m-%d",SUBSTR( COMM_DUE_DT ,0,10) ) , 
  SAFE_CAST( COMM_DUE_AMT AS FLOAT64), SAFE_CAST( COMM_RCVD_AMT AS FLOAT64), 
  PARSE_DATE("%Y-%m-%d",SUBSTR( COMM_RCVD_DT ,0,10) ) ,
  COMM_RCVD_FLG, 
  PAYMENT_COMMENT, 
  PROPERTY, TAG, SAFE_CAST( TEMP_AMT AS FLOAT64), 
  GL_BRANCH, SAFE_CAST( ORIG_RCPT_NUM AS INT64), 
  PREV_BILLED, GROUP_ID, 
  PARSE_DATETIME("%Y-%m-%d %H:%M:%S",CREATE_DTTIME),
  'jobrunid', 'GLOBALWARE', CURRENT_DATETIME() );