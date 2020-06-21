CREATE OR REPLACE TABLE LANDING.CUR_WD_EMPLOYEE AS 
SELECT WD_ID,
EMP_ID EMPLOYEE_ID,
WORKER_ENT_ID ENT_ID,
WORKER_LIFE_ID LIFE_ID,
WORKER_P_C_10_DIGIT PC10_ID,
WORKER_P_C_3_DIGIT PC03_ID,
WORKER_TRAVEL_ID TRAVEL_ID,
LOCATION BRANCH_NUM,
COMPANY,
COST_CENTER,
SAFE_CAST(HIRE_DT AS DATE) HIRE_DT,
SAFE_CAST(TERMINATION_DT AS DATE) TERMINATION_DT,
LEGAL_FIRST_NM LEGAL_FIRST_NM,
LEGAL_MIDDLE_NM LEGAL_MIDDLE_NM,
LEGAL_LAST_NM LEGAL_LAST_NM,
LEGAL_SOCIAL_SUFFIX ,
PREFERRED_FIRST_NM PREFERRED_FIRST_NM,
PREFERRED_LAST_NM PREFERRED_LAST_NM,
PREFERRED_SUFFIX PREFERRED_SUFFIX,
LICENSE_TYPE,
RESIDENT_LICENSE_IND,
LICENSE_NUM,
LICENSE_ST,
SAFE_CAST(LICENSE_EFFECTIVE_DT AS DATE) LICENSE_EFFECTIVE_DT,
SAFE_CAST(LICENSE_EXPIRATION_DT AS DATE) LICENSE_EXPIRATION_DT,
EMAIL_ADDRESS,
JOB_CD,
LANGUAGE_TYPE_CD,
SAFE_CAST(CHANGE_EFFECTIVE_DT AS DATE) CHANGE_EFFECTIVE_DT,
PHONE_NUM,
PHONE_EXTSN,
PRFS_DESIG_TYPE_CD,
SAFE_CAST(PRFS_DESIG_EFFECTIVE_DT AS DATE) PRFS_DESIG_EFFECTIVE_DT,
SAFE_CAST(PRFS_DESIG_EXPIRATION_DT AS DATE) PRFS_DESIG_EXPIRATION_DT,
SUPERVISOR_ID,
ORGANIZATION_ID,
CAST( EMPLOYEE_STATUS AS  STRING) EMPLOYEE_STATUS,
CAST(WORKER_TYPE AS  STRING)  WORKER_TYPE,
CAST(SECONDARY_EMAIL_ADDRESS  AS  STRING) SECONDARY_EMAIL,
CAST(WORKER_AVAYA_ID  AS  STRING) WORKER_AVAYA_ID
FROM 
(SELECT *,ROW_NUMBER() OVER(PARTITION BY EMP_ID ORDER BY  SAFE_CAST(HIRE_DT AS DATE) DESC ) RN
FROM LANDING.WORKDAY_WORKER )
WHERE RN=1