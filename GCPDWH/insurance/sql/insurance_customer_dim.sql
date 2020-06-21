CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.INSURANCE_CUSTOMER_DIM AS
SELECT 
MCB.CUSTOMER_MDM_KEY CUSTOMER_MDM_KEY,
IPD.SOURCE_ID,
IPD.DATA_SOURCE,
TRIM(IPD.SOURCE_PRODUCT_CD) AS SOURCE_PRODUCT_CD,
TRIM(IPD.PRODUCT_CD) AS PRODUCT_CD,
IPD.PRODUCT_TYPE,
IPD.SUB_PRODUCT_TYPE,
IPD.POLICY_STATE,
IPD.POLICY_PREFIX,
IPD.POLICY_NUM,
IPD.AGREEMENT_NUM,
IPD.IG_MDM_AGREEMENT_NUM,
TRIM(IPD.STATUS_CD) AS STATUS_CD,
M.MEMBER_NUM AS MEMBER_NUM ,
TRIM(M.STATUS_CD) AS  MEMBER_STATUS_CD,
CLAIM.COUNT_OF_CLAIMS AS INSURANCE_CLAIMS_CNT,
TRIM(IPD.DRIVER_TYP_CD) AS DRIVER_TYPE_CD,
CLDT.CODE_DESC AS DRIVER_TYPE_DESC,
CAST(NULL AS STRING) AS DRIVER_YAF,
IPD.LICENSE_STATE AS DRIVER_LICENSE_STATE,
IPD.LICENSE_DT AS DRIVER_LICENSE_DT,
CAST(NULL AS STRING) AS DRIVER_LICENSE_VALID_IND,
CASE WHEN TRIM(IPD.STUDENT_DISCOUNT_IND)  IS NULL THEN 'N' ELSE TRIM(IPD.STUDENT_DISCOUNT_IND)  END AS DRIVER_STUDENT_DISCOUNT_IND,
TRIM(IPD.DRIVING_TRAINING_IND) AS DRIVING_TRAINING_IND,
IPD.DEFENSIVE_DRIVER_DT AS DEFENSIVE_DRIVER_DT,
IPD.DRIVER_DISCOUNT_CD AS DRIVER_DISCOUNT_CD,
12345 AS JOB_RUN_ID,
'INSURANCE' AS SOURCE_SYSTEM_CD,
DATETIME(CURRENT_TIMESTAMP()) AS CREATE_DTTIME
FROM (SELECT IPD.*, E1.* FROM  (SELECT * FROM  CUSTOMER_PRODUCT.INSURANCE_POLICY_DIM WHERE ACTIVE_FLG = 'Y') IPD 
LEFT OUTER JOIN (SELECT  CONCAT(UPPER(E1.DRVR_LST_NM) ,  '~'  ,UPPER(E1.DRVR_FRST_NM), '~' ,CAST(E1.BRTH_DT AS STRING) , '~' ,CAST(E1.DRVR_NUM AS STRING)) AS DRIVER_SOURCE_ID,
						E1.DRVR_TYP_CD AS DRIVER_TYP_CD, E1.LIC_ST_CD AS LICENSE_STATE , E1.LIC_DT AS LICENSE_DT, E1.STU_DISC_CD AS STUDENT_DISCOUNT_IND, 
                        E1.DRVR_TRN_IND DRIVING_TRAINING_IND, E1.DFNS_DRVR_DT DEFENSIVE_DRIVER_DT, E1.FILE_ID, E1.TRANS_NUM, E1. DRVR_DISC_CD  AS  DRIVER_DISCOUNT_CD
                        FROM  LANDING.IE_E1_LDG E1) E1 ON IPD.SOURCE_ID = CONCAT("IE~",CAST(E1.FILE_ID AS STRING),"~",CAST(E1.TRANS_NUM AS STRING)) ) IPD		
--LEFT OUTER JOIN CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE MCB ON  IPD.AGREEMENT_NUM = MCB.SOURCE_KEY1 AND MCB.SOURCE_SYSTEM LIKE 'IVANS_PAS%' AND MCB.SOURCE_KEY2 = IPD.DRIVER_SOURCE_ID
LEFT OUTER JOIN CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE MCB ON IPD.AGREEMENT_NUM = MCB.SOURCE_KEY1 AND
MCB.SOURCE_SYSTEM_CD LIKE 'IVANS_PAS%' AND coalesce(MCB.SOURCE_KEY2,'X') = coalesce(IPD.DRIVER_SOURCE_ID,'X')

LEFT OUTER JOIN ( SELECT * FROM (SELECT CUSTOMER_MDM_KEY, STATUS_CD,  MEMBER_NUM, ROW_START_DT, ROW_END_DT, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_MDM_KEY ORDER BY STATUS_CD ASC, TERM_EXPIRY_DT DESC) AS ROW_NUM FROM `aaa-mwg-dwprod.CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_DIM` WHERE ACTIVE_FLG = 'Y') WHERE ROW_NUM = 1 ) M ON  MCB.CUSTOMER_MDM_KEY = M.CUSTOMER_MDM_KEY

LEFT OUTER JOIN (SELECT AGREEMENT_NUM, COUNT(CLAIM_NUM) AS COUNT_OF_CLAIMS FROM  (SELECT AGREEMENT_NUM, CLAIM_NUM, CLAIM_OPEN_DT ,ROW_NUMBER() OVER(PARTITION BY CLAIM_NUM, CLAIM_OPEN_DT) RN 
                 FROM `CUSTOMER_PRODUCT.INSURANCE_CLAIMS_DIM` ) CLAIM WHERE CLAIM.RN= 1 GROUP BY AGREEMENT_NUM) CLAIM  ON IPD.AGREEMENT_NUM= CLAIM.AGREEMENT_NUM
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLDT ON CLDT.CODE_VALUE = IPD.DRIVER_TYP_CD AND CLDT.COLUMN_NAME = 'DRIVER_TYPE_CD'