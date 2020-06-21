INSERT INTO
  LANDING.PCOMP_INSURANCE_TRANSACTION (INSURANCE_TRANSACTION_ID,
    BATCH_NM,
    PRODUCT_CD,
    POLICY_NUM,
    POLICY_EFFECTIVE_DT,
    POLICY_ENDORSEMENT_DT,
    POLICY_HOLDER_LAST_NM,
    RESIDENTIAL_ZIP_CD,
    SOURCE_SYSTEM,
    TRANSACTION_TYPE,
    TRANSACTION_DT,
    TRANSACTION_AMT,
    SALES_REP_ID,
    SALES_REP_DO,
    PROCESS_TYPE,
    INCEPTION_DT,
    ERROR_FLAG,
    ETL_JOB_RUN_ID,
    CREATED_DT,
    CREATED_BY)
SELECT
  CASE
    WHEN PCOMP.MAX_ID IS NULL THEN 0+(ROW_NUMBER() OVER ())
    ELSE PCOMP.MAX_ID+(ROW_NUMBER() OVER ())
  END AS INSURANCE_TRANSACTION_ID,
  CONCAT('BATCH',CAST(CURRENT_DATE() AS STRING)) AS BATCH_NAME,
  PRODUCT_CD,
  POLICY_NUM,
  PARSE_DATE('%m/%d/%Y',
    SUBSTR(TRIM(POLICY_EFFECTIVE_DT),1,10)) AS POLICY_EFFECTIVE_DT,
  PARSE_DATE('%m/%d/%Y',
    SUBSTR(TRIM(POLICY_ENDORSEMENT_DT),1,10)) AS POLICY_ENDORSEMENT_DT,
  POLICY_HOLDER_LAST_NAME,
  RESIDENTIAL_ZIP_CD,
  SOURCE_SYSTEM,
  TRANSACTION_TYPE,
  PARSE_DATE('%m/%d/%Y',
    SUBSTR(TRIM(TRANSACTION_DT),1,10)) AS TRANSACTION_DT,
  TRANSACTION_AMOUNT,
  SALESREP_ID,
  SALESREP_DO,
  PROCESS_TYPE,
  PARSE_DATE('%m/%d/%Y',
    SUBSTR(TRIM(INCEPTION_DT),1,10)) AS INCEPTION_DT,
  ERROR_FLAG,
  "v_job_run_id" AS ETL_JOB_RUN_ID,
  CURRENT_DATETIME() AS CREATED_DT,
  "v_job_name" AS CREATED_BY
FROM
  LANDING.WORK_PCOMP_COMPUP,
  (
  SELECT
    MAX(INSURANCE_TRANSACTION_ID) MAX_ID
  FROM
    LANDING.PCOMP_INSURANCE_TRANSACTION ) PCOMP