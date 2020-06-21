MERGE
  CUSTOMER_PRODUCT.CTS_COMPLAINT_HISTORY_DIM A
USING
  (
  SELECT
    B.BRANCH_CD AS BRANCH_CD,
    PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
      B.RECEIVED_DT) AS RECEIVED_DTTIME,
    SAFE_CAST(B.CMPLNT_ID AS INT64) AS COMPLAINT_ID,
    B.STATUS_CD AS STATUS_CD,
    PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
      B.STATUS_DTIME_DT) AS STATUS_DTTIME,
    PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
      B.STATUS_DTIME_DT) AS CLOSE_DTTIME,
    B.STATUS_EMPLE_ID AS STATUS_EMPLOYEE_ID,
    B.STATUS_TX AS STATUS_DESC,
    SAFE_CAST(B.STATUS_SEQ_NBR AS INT64) AS STATUS_SEQUENCE_NUM,
    "v_job_run_id" AS ETL_JOB_RUN_ID,
    'CTS' AS SOURCE_SYSTEM_CD,
    CURRENT_DATETIME() AS CREATE_DTTIME,
    "v_job_name" AS CREATED_BY
  FROM
    `LANDING.WORK_CTS_COMPLAINT_HISTORY_DIM` B ) B
ON
  A.COMPLAINT_ID = B.COMPLAINT_ID
  AND A.RECEIVED_DTTIME = B.RECEIVED_DTTIME
  AND A.BRANCH_CD = B.BRANCH_CD
  AND A.STATUS_SEQUENCE_NUM=B.STATUS_SEQUENCE_NUM
  WHEN MATCHED  THEN  UPDATE   SET  A.STATUS_DTTIME=B.STATUS_DTTIME,  A.STATUS_CD=B.STATUS_CD,  A.STATUS_EMPLOYEE_ID=B.STATUS_EMPLOYEE_ID,  A.STATUS_DESC=B.STATUS_DESC,  A.STATUS_SEQUENCE_NUM=B.STATUS_SEQUENCE_NUM,  A.ETL_JOB_RUN_ID=B.ETL_JOB_RUN_ID,  A.SOURCE_SYSTEM_CD=B.SOURCE_SYSTEM_CD,  A.CREATE_DTTIME=B.CREATE_DTTIME,  A.CREATED_BY=B.CREATED_BY,  A.CLOSE_DTTIME=B.CLOSE_DTTIME
  WHEN NOT MATCHED
  THEN
INSERT
  (BRANCH_CD,
    RECEIVED_DTTIME,
    COMPLAINT_ID,
    STATUS_CD,
    STATUS_DTTIME,
    STATUS_EMPLOYEE_ID,
    STATUS_DESC,
    STATUS_SEQUENCE_NUM,
    ETL_JOB_RUN_ID,
    SOURCE_SYSTEM_CD,
    CREATE_DTTIME,
    CREATED_BY,
    CLOSE_DTTIME)
VALUES
  (BRANCH_CD, 
  RECEIVED_DTTIME, 
  COMPLAINT_ID, 
  STATUS_CD, 
  STATUS_DTTIME, 
  STATUS_EMPLOYEE_ID, 
  STATUS_DESC, 
  STATUS_SEQUENCE_NUM, 
  ETL_JOB_RUN_ID, 
  SOURCE_SYSTEM_CD, 
  CREATE_DTTIME, 
  CREATED_BY, 
  CLOSE_DTTIME);