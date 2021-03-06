INSERT INTO
  LANDING.PCOMP_INSURANCE_TRANSACTION
  (INSURANCE_TRANSACTION_ID,
  BATCH_NM,
  PRODUCT_CD,
  POLICY_NUM,
  POLICY_EFFECTIVE_DT,
  POLICY_ENDORSEMENT_DT,
  POLICY_STATE,
  POLICY_HOLDER_LAST_NM,
  POLICY_PREFIX,
  AGENT_NM,
  EXPIRATION_DT,
  PAS_AGENT_ID,
  TRANSACTION_EFFECTIVE_DT,
  MAILING_ZIP_CD,
  RESIDENTIAL_ZIP_CD,
  SOURCE_SYSTEM,
  PRODUCT_SUB_CATEGORY,
  TRANSACTION_TYPE,
  TRANSACTION_DT,
  TRANSACTION_AMT,
  SALES_REP_ID,
  SALES_REP_DO,
  AGENCY_NM,
  SELLING_AGENCY_NUM,
  AGENCY_OF_RECORD,
  AGENT_OF_TRANSACTION,
  UNDER_WRITING_COMPANY,
  PROCESS_TYPE,
  INCEPTION_DT,
  PRIOR_CARRIER,
  ERROR_FLAG,
  GA_CD,
  CAMPAIGN_SOURCE_CD,
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
  POLICY_STATE,
  POLICY_HOLDER_LAST_NAME,
  POLICY_PREFIX,
  AGENT_NAME,
  EXPIRATION_DT,
  PAS_AGENT_ID,
  TRANSACTION_EFFECTIVE_DT,
  MAILING_ZIP_CD,
  RESIDENTIAL_ZIP_CD,
  SOURCE_SYSTEM,
  PRODUCT_SUB_CATEGORY,
  TRANSACTION_TYPE,
  TRANSACTION_DT,
  TRANSACTION_AMOUNT,
  SALESREP_ID,
  SALESREP_DO,
  AGENCY_NAME,
  SELLING_AGENCY_NUM,
  AGENCY_OF_RECORD,
  AGENT_OF_TRANSACTION,
  UNDER_WRITING_COMPANY,
  PROCESS_TYPE,
  INCEPTION_DT,
  PRIOR_CARRIER,
  CASE
    WHEN TRANS_CD_FLAG = '1' AND (TRANSACTION_AMOUNT_FLAG = '0' OR (CASE
        WHEN STAT_TRANSCODE IS NULL
      OR POLICY_NUM IS NULL
      OR TRANSACTION_AMOUNT IS NULL
      OR SALESREP_ID IS NULL
      OR SALESREP_DO IS NULL
      OR TRANSACTION_DT IS NULL
      OR TRANSACTION_TYPE IS NULL
      OR PROCESS_TYPE IS NULL THEN '0'
        ELSE '1' END) = '0') THEN 'Load Validation Error'
  END AS ERROR_FLAG,
  GA_CODE,
  CAMPAIGN_SOURCE_CD,
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
    PP.CLUB_PRODUCT_CODE AS PRODUCT_CD,
    LTRIM(RTRIM(STAT_VIN_1_12)) AS POLICY_NUM,
    CASE
      WHEN LPAD(LTRIM(RTRIM(STAT_EFF_DT)),  8,  '0') = '00000000' THEN NULL
      ELSE PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(STAT_EFF_DT)),
        8,
        '0'))
    END AS POLICY_EFFECTIVE_DT,
    CASE
      WHEN LPAD(LTRIM(RTRIM(STAT_END_EFF_DT)),  8,  '0') = '00000000' THEN NULL
      ELSE PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(STAT_END_EFF_DT)),
        8,
        '0'))
    END AS POLICY_ENDORSEMENT_DT,
    LTRIM(RTRIM(STAT_STATE_CD)) AS POLICY_STATE,
    LTRIM(RTRIM(STAT_NM)) AS POLICY_HOLDER_LAST_NAME,
    LTRIM(RTRIM(STAT_POLICY_PFX)) AS POLICY_PREFIX,
    LTRIM(RTRIM(STAT_AGENT_NM)) AS AGENT_NAME,
    PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(STAT_EXP_DT)),
        8,
        '0')) EXPIRATION_DT,
    STAT_PAS_AGENT_ID AS PAS_AGENT_ID,
    CASE
      WHEN LENGTH(LTRIM(RTRIM(STAT_END_EFF_DT)))=0 THEN NULL
      ELSE PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(STAT_END_EFF_DT)),
        8,
        '0'))
    END AS TRANSACTION_EFFECTIVE_DT,
    CASE
      WHEN LTRIM(RTRIM(STAT_ZIP))='00000' OR LTRIM(RTRIM(STAT_ZIP))='999990000' THEN NULL
      WHEN LENGTH(LTRIM(RTRIM(STAT_ZIP)))=9
    OR LENGTH(LTRIM(RTRIM(STAT_ZIP)))=5 THEN LTRIM(RTRIM(STAT_ZIP))
      ELSE NULL
    END AS MAILING_ZIP_CD,
    CASE
      WHEN LTRIM(RTRIM(STAT_RESIDENTIAL_ZIP))='00000' OR LTRIM(RTRIM(STAT_RESIDENTIAL_ZIP))='999990000' THEN NULL
      WHEN LENGTH(LTRIM(RTRIM(STAT_RESIDENTIAL_ZIP)))=9
    OR LENGTH(LTRIM(RTRIM(STAT_RESIDENTIAL_ZIP)))=5 THEN LTRIM(RTRIM(STAT_RESIDENTIAL_ZIP))
      ELSE NULL
    END AS RESIDENTIAL_ZIP_CD,
    PP.DATA_SOURCE AS SOURCE_SYSTEM,
    PP.PRODUCT_SUB_CATEGORY AS PRODUCT_SUB_CATEGORY,
    CASE
      WHEN STAT_NR='0' THEN 'New'
      WHEN STAT_NR='1' THEN 'Renewal'
      WHEN STAT_NR='2' THEN 'Rewrite'
      WHEN STAT_NR='3' THEN 'Split'
      WHEN STAT_NR='4' THEN 'Spin Off'
      ELSE NULL
    END AS TRANSACTION_TYPE,
    CASE
      WHEN LENGTH(LTRIM(RTRIM(STAT_ENTRY_DT)))=0 THEN NULL
      ELSE PARSE_DATE('%m%d%Y',
      LPAD(LTRIM(RTRIM(STAT_ENTRY_DT)),
        8,
        '0'))
    END AS TRANSACTION_DT,
    CAST(STAT_ACT_AMT AS FLOAT64) AS TRANSACTION_AMOUNT,
    CASE
      WHEN CAST(STAT_ACT_AMT AS FLOAT64) <= CP1.PAR_VALUE_NUM_1 THEN '1'
      ELSE '0'
    END AS TRANSACTION_AMOUNT_FLAG,
    CASE
      WHEN TRIM(RTRIM(STAT_STATE_CD))='AZ' OR LTRIM(RTRIM(STAT_STATE_CD))='MT' OR LTRIM(RTRIM(STAT_STATE_CD))='WY' THEN CASE
      WHEN STAT_PAS_AGENT_ID IS NULL THEN '000'
      ELSE STAT_PAS_AGENT_ID
    END WHEN LENGTH(TRIM(STAT_REP_NUM))<3 THEN LPAD(TRIM(STAT_REP_NUM),  3,  '0')
      WHEN STAT_REP_NUM IS NULL THEN '000'
      ELSE STAT_REP_NUM
    END AS SALESREP_ID,
    CASE
      WHEN STAT_REP_DO IS NULL THEN '00 '
      WHEN LENGTH(TRIM(STAT_REP_DO))<2 THEN LPAD(TRIM(STAT_REP_DO),
      2,
      '0')
      ELSE STAT_REP_DO
    END AS SALESREP_DO,
    LTRIM(RTRIM(STAT_AGENCY_NM)) AS AGENCY_NAME,
    LTRIM(RTRIM(STAT_SELLING_AGENCY_NUM)) AS SELLING_AGENCY_NUM,
    LTRIM(RTRIM(STAT_AGENCY_OF_REC)) AS AGENCY_OF_RECORD,
    LTRIM(RTRIM(STAT_AGENCY_OF_TRANSACTION)) AS AGENT_OF_TRANSACTION,
    LTRIM(RTRIM(STAT_UNDERWRITING_COMPANY)) AS UNDER_WRITING_COMPANY,
    CASE
      WHEN (STAT_TRANSCODE='441' OR STAT_TRANSCODE='41') AND CAST(STAT_ACT_AMT AS FLOAT64)>0 THEN 'Issue'
      WHEN (STAT_TRANSCODE='441'
      OR STAT_TRANSCODE='41')
    AND CAST(STAT_ACT_AMT AS FLOAT64)<0 THEN 'Correct'
      WHEN STAT_TRANSCODE='461' OR STAT_TRANSCODE='61' THEN 'Cancel'
      WHEN STAT_TRANSCODE='771'
    OR STAT_TRANSCODE='71' THEN 'Reinstate'
      WHEN (STAT_TRANSCODE='092' OR STAT_TRANSCODE='92') THEN 'Drop'
      WHEN (STAT_TRANSCODE='442'
      OR STAT_TRANSCODE='462'
      OR STAT_TRANSCODE='772'
      OR STAT_TRANSCODE='776'
      OR STAT_TRANSCODE='489'
      OR STAT_TRANSCODE='89'
      OR STAT_TRANSCODE='779'
      OR STAT_TRANSCODE='79'
      OR STAT_TRANSCODE='58R'
      OR STAT_TRANSCODE='8R'
      OR STAT_TRANSCODE='67R'
      OR STAT_TRANSCODE='7R'
      OR STAT_TRANSCODE='486'
      OR STAT_TRANSCODE='86') THEN 'Endorse'
      ELSE NULL
    END AS PROCESS_TYPE,
    PARSE_DATE('%m%d%Y',
      CONCAT(SUBSTR(TRIM(STAT_BASE_YR),5,2),SUBSTR(TRIM(STAT_BASE_YR),7,2),SUBSTR(TRIM(STAT_BASE_YR),1,4))) AS INCEPTION_DT,
    LTRIM(RTRIM(STAT_PRIOR_CARRIER)) AS PRIOR_CARRIER,
    LTRIM(RTRIM(STAT_GA_CD)) AS GA_CODE,
    LTRIM(RTRIM(STAT_CAMPAIGN_SOURCE_CD)) AS CAMPAIGN_SOURCE_CD,
    "v_job_run_id" AS ETL_JOB_RUN_ID,
    CURRENT_DATETIME() AS CREATED_DT,
    "v_job_name" AS CREATED_BY,
    CASE
      WHEN CP.PAR_VALUE_CHAR_1 IS NULL THEN '0'
      ELSE '1'
    END AS TRANS_CD_FLAG,
    STAT_TRANSCODE
  FROM
    LANDING.WORK_PCOMP_EXIGEN_AUTO PEAS
  LEFT OUTER JOIN
    REFERENCE.PAS_PRODUCT PP
  ON
    UPPER(LTRIM(RTRIM(PEAS.STAT_POLICY_PFX))) = PP.POLICY_STATE_PREFIX
  LEFT OUTER JOIN
    `REFERENCE.COMMON_PARAMETERS` CP
  ON
    CP.FILE_TYPE = 'EXIGENA'
    AND CP.PARAMETER_TYPE='VALID_TRANS_CODES'
    AND STAT_TRANSCODE = CP.PAR_VALUE_CHAR_1
  LEFT OUTER JOIN
    `REFERENCE.COMMON_PARAMETERS` CP1
  ON
    CP1.FILE_TYPE = 'EXIGENA'
    AND CP1.PARAMETER_TYPE='VALID_TRANS_AMT',
    (
    SELECT
      MAX(INSURANCE_TRANSACTION_ID) MAX_ID
    FROM
      LANDING.PCOMP_INSURANCE_TRANSACTION ) PCOMP
  WHERE
    CASE
      WHEN CP.PAR_VALUE_CHAR_1 IS NULL THEN '0'
      ELSE '1'
    END = '1' )