INSERT INTO
  LANDING.PCOMP_INSURANCE_TRANSACTION ( INSURANCE_TRANSACTION_ID,
    BATCH_NM,
	PRODUCT_CD,
    POLICY_NUM,
    POLICY_EFFECTIVE_DT,
    POLICY_ENDORSEMENT_DT,
    POLICY_HOLDER_LAST_NM,
    CEA_POLICY_NUM,
    TRANSACTION_EFFECTIVE_DT,
    TRANSACTION_CD,
    SOURCE_SYSTEM,
    TRANSACTION_TYPE,
    TRANSACTION_DT,
    TRANSACTION_AMT,
    SALES_REP_ID,
    SALES_REP_DO,
    PROCESS_TYPE,
    ERROR_FLAG,
    ETL_JOB_RUN_ID,
    CREATED_DT,
    CREATED_BY)
SELECT
  INSURANCE_TRANSACTION_ID,
  BATCH_NAME,
  PRODUCT_CD,
  POLICY_NUM,
  POLICY_EFFECTIVE_DT,
  POLICY_ENDORSEMENT_DT,
  POLICY_HOLDER_LAST_NAME,
  CEA_POLICY_NUM,
  TRAN_EFF_DT,
  TRANSACTION_CD,
  SOURCE_SYSTEM,
  TRANSACTION_TYPE,
  TRANSACTION_DT,
  TRANSACTION_AMOUNT,
  SALESREP_ID,
  SALESREP_DO,
  PROCESS_TYPE,
  CASE
    WHEN TRANS_CD_FLAG = '1' AND (TRANSACTION_AMOUNT_FLAG = '0' OR (CASE
        WHEN PREMIUM IS NULL
      OR POLICY_NUM IS NULL
      OR TRANSACTION_AMOUNT IS NULL
      OR SALESREP_ID IS NULL
      OR SALESREP_DO IS NULL
      OR TRANSACTION_DT IS NULL
      OR TRANSACTION_TYPE IS NULL
      OR PROCESS_TYPE IS NULL THEN '0'
        ELSE '1' END) = '0' ) THEN 'Load Validation Error'
    WHEN TRANS_CD_FLAG = '0' THEN 'Invalid Transactioncode'
  END AS ERROR_FLAG,
  ETL_JOB_RUN_ID,
  CREATED_DT,
  CREATED_BY
FROM (
  SELECT
    CASE
      WHEN PCOMP.MAX_ID IS NULL THEN 0+(ROW_NUMBER() OVER ())
      ELSE PCOMP.MAX_ID+(ROW_NUMBER() OVER ())
    END AS INSURANCE_TRANSACTION_ID,
    CONCAT('BATCH',CAST(CURRENT_DATE() AS STRING)) AS BATCH_NAME,
    'FK' AS PRODUCT_CD,
    POLICY_NUM AS CEA_POLICY_NUM,
    CASE
      WHEN LPAD(LTRIM(RTRIM(TRAN_EFF_DT)),  8,  '0') = '00000000' THEN NULL
      ELSE PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(TRAN_EFF_DT)),
        8,
        '0'))
    END AS TRAN_EFF_DT,
    CASE
      WHEN SUBSTR(TRIM(POL_NBR),1,3) = 'DP3' THEN SUBSTR(TRIM(POL_NBR),5,9)
      WHEN LENGTH(TRIM(POL_NBR))=13
    AND SUBSTR(TRIM(POL_NBR),1,2) = 'CA' THEN SUBSTR(TRIM(POL_NBR), 5, 13)
      ELSE POL_NBR
    END AS POLICY_NUM,
    CASE
      WHEN LPAD(LTRIM(RTRIM(EFF_DT)),  8,  '0') = '00000000' THEN NULL
      ELSE PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(EFF_DT)),
        8,
        '0'))
    END AS POLICY_EFFECTIVE_DT,
    CASE
      WHEN LPAD(LTRIM(RTRIM(TERM_DT)),  8,  '0') = '00000000' THEN NULL
      ELSE PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(TERM_DT)),
        8,
        '0'))
    END AS POLICY_ENDORSEMENT_DT,
    LTRIM(RTRIM(INS_NM)) AS POLICY_HOLDER_LAST_NAME,
    TRANS_CD TRANSACTION_CD,
    'CEA' AS SOURCE_SYSTEM,
    CASE
      WHEN TRANS_CD='NB' OR TRANS_CD='NC' OR TRANS_CD='NE' OR TRANS_CD='NR' OR TRANS_CD='WB' OR TRANS_CD='WC' OR TRANS_CD='WE' OR TRANS_CD='WR' THEN 'New'
      WHEN TRANS_CD='RN'	
    OR TRANS_CD='RC'
    OR TRANS_CD='RE'
    OR TRANS_CD='RR'
    OR TRANS_CD='LB'
    OR TRANS_CD='LC'
    OR TRANS_CD='LE'
    OR TRANS_CD='LR' THEN 'Renewal'
      ELSE NULL
    END AS TRANSACTION_TYPE,
    PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(TRANS_DT)),8,'0')) AS TRANSACTION_DT,
    CASE
      WHEN CAST(PREM AS FLOAT64) IS NULL THEN 0
      ELSE CAST(PREM AS FLOAT64)
    END AS TRANSACTION_AMOUNT,
    PREM AS PREMIUM,
    CASE
      WHEN CAST(PREM AS FLOAT64) <= CP1.PAR_VALUE_NUM_1 THEN '1'
      ELSE '0'
    END AS TRANSACTION_AMOUNT_FLAG,
    CASE
      WHEN RRDN.REP_NUM IS NULL THEN '000'
      WHEN LENGTH(RRDN.REP_NUM)<3 THEN LPAD(RRDN.REP_NUM,
      3,
      '0')
      ELSE RRDN.REP_NUM
    END AS SALESREP_ID,
    CASE
      WHEN RRDN.REP_DO IS NULL THEN '00'
      ELSE LPAD(LTRIM(RTRIM(RRDN.REP_DO)),
      2,
      '0')
    END AS SALESREP_DO,
    CASE
      WHEN TRANS_CD='NB' OR TRANS_CD='RN' OR TRANS_CD='WB' OR TRANS_CD='LB' THEN 'Issue'
      WHEN TRANS_CD='NC'
    OR TRANS_CD='RC'
    OR TRANS_CD='WC'
    OR TRANS_CD='LC' THEN 'Cancel'
      WHEN TRANS_CD='NE' OR TRANS_CD='RE' OR TRANS_CD='WE' OR TRANS_CD='LE' THEN 'Endorse'
      WHEN TRANS_CD='NR'
    OR TRANS_CD='RR'
    OR TRANS_CD='WR'
    OR TRANS_CD='LR' THEN 'Reinstate'
      ELSE NULL
    END AS PROCESS_TYPE,
    CASE
      WHEN CP.PAR_VALUE_CHAR_1 IS NULL THEN '0'
      ELSE '1'
    END AS TRANS_CD_FLAG,
    "v_job_run_id" AS ETL_JOB_RUN_ID,
    CURRENT_DATETIME() AS CREATED_DT,
    "v_job_name" AS CREATED_BY	
  FROM
    LANDING.WORK_PCOMP_CEABLUEC PCS
  LEFT OUTER JOIN
      (
  SELECT
    *
  FROM (
    SELECT
      REP_NUM,
      REP_DO,
      PRIMARY_PAS_AGENT_NUM,
      ROW_NUMBER() OVER(PARTITION BY PRIMARY_PAS_AGENT_NUM ORDER BY START_DT DESC) AS RNK
    FROM
      `REFERENCE.REF_REP_DO_NUM` )
  WHERE
    RNK=1) RRDN
  ON
    PCS.PAS_AGENT_NUM = RRDN.PRIMARY_PAS_AGENT_NUM
  LEFT OUTER JOIN
    `REFERENCE.COMMON_PARAMETERS` CP
  ON
    CP.FILE_TYPE = 'CEABLUEC'
    AND CP.PARAMETER_TYPE='VALID_TRANS_CODES'
    AND TRANS_CD = CP.PAR_VALUE_CHAR_1
  LEFT OUTER JOIN
    `REFERENCE.COMMON_PARAMETERS` CP1
  ON
    CP1.FILE_TYPE = 'CEABLUEC'
    AND CP1.PARAMETER_TYPE='VALID_TRANS_AMT',
    (
    SELECT
      MAX(INSURANCE_TRANSACTION_ID) MAX_ID
    FROM
	      LANDING.PCOMP_INSURANCE_TRANSACTION) PCOMP)
        