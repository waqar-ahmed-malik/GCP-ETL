INSERT INTO LANDING.PCOMP_HO_HISTORY
(
STAGEKEY,
BATCH_NM,
SOURCE_SYSTEM,
LOAD_DT,
POL_PREFIX,
POL_NUM,
STREET_ADDR_NUM,
MEMBERSHIP_NUM,
MEMB_STREET_NUM,
EFF_DT,
FIRST_EFF_DT,
CANCEL_DT,
RENEWED,
POL_STATUS_CD,
POL_TYPE,
FILLER,
ETL_JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DT,
CREATED_BY
)
SELECT 
CASE
     WHEN PCOMP.MAX_ID IS NULL THEN 0+(ROW_NUMBER() OVER ())
     ELSE PCOMP.MAX_ID+(ROW_NUMBER() OVER ())
   END AS STAGEKEY,
CONCAT('BATCH',CAST(CURRENT_DATE() AS STRING)),
'AS400',
CURRENT_DATE(),
STG.POLICY_PREFIX,
CASE WHEN LENGTH(STG.POLICY_NUM) > 8
  THEN SUBSTR( TRIM(STG.POLICY_NUM), 3, 7 )
 ELSE STG.POLICY_NUM
END AS POL_NUM,
STG.STREET_NUM,
STG.MEMBERSHIP_NUM,
CONCAT( STG.MEMBERSHIP_NUM,'_', STG.STREET_NUM),
PARSE_DATE('%m%d%Y',LPAD( STG.TERM_EFFECTIVE_DT , 8, '0')),
CAST(NULL AS DATE) AS FIRST_EFF_DT,
CASE WHEN STG.CANCELLATION_DT = "10101"
	THEN PARSE_DATE('%Y%m%d','29991231')
ELSE
	PARSE_DATE('%Y%m%d',STG.CANCELLATION_DT)
END AS CANCEL_DT,
STG.RENEWED,
CAST(NULL AS STRING) AS POL_STATUS_CD,
STG.TRANSACTION_TYPE,
STG.FILLER,
"v_job_run_id" AS ETL_JOB_RUN_ID,
 "PCOMP" AS SOURCE_SYSTEM_CD,
    CURRENT_DATE() AS CREATE_DT,
    "v_job_name" AS CREATED_BY
FROM LANDING.WORK_PCOMP_HO_HISTORY STG
LEFT OUTER JOIN
LANDING.PCOMP_HO_HISTORY HO
ON CONCAT( STG.MEMBERSHIP_NUM,'_', STG.STREET_NUM) = CONCAT( HO.MEMBERSHIP_NUM,'_',HO. STREET_ADDR_NUM)
 AND (CASE WHEN LENGTH(STG.POLICY_NUM) > 8
  THEN SUBSTR( TRIM(STG.POLICY_NUM), 3, 7 )
 ELSE STG.POLICY_NUM
END) = HO.POL_NUM,
(
   SELECT
     MAX(STAGEKEY) MAX_ID
   FROM
     LANDING.PCOMP_HO_HISTORY ) PCOMP
WHERE STG.Transaction_Type = 'NC'