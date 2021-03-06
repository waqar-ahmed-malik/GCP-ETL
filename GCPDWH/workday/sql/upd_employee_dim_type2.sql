UPDATE  LANDING.EMPLOYEE_DIM_STAGE EMP
SET EMP.EFFECTIVE_END_DT = TEMP.END_DT,
EMP.ACTIVE_FLG=TEMP.ACTV_FL
FROM (
SELECT EMPLOYEE_ID,EFFECTIVE_START_DT,EFFECTIVE_END_DT ,ACTIVE_FLG, MD5_VALUE,
CASE WHEN 
LEAD(DATE_SUB(EFFECTIVE_START_DT, INTERVAL 1 DAY)) OVER (PARTITION BY EMPLOYEE_ID ORDER BY EFFECTIVE_START_DT)  IS NULL
THEN 
'9999-12-31'
ELSE
LEAD(DATE_SUB(EFFECTIVE_START_DT, INTERVAL 1 DAY)) OVER (PARTITION BY EMPLOYEE_ID ORDER BY EFFECTIVE_START_DT) END END_DT,
CASE WHEN 
LEAD(DATE_SUB(EFFECTIVE_START_DT, INTERVAL 1 DAY)) OVER (PARTITION BY EMPLOYEE_ID ORDER BY EFFECTIVE_START_DT)  IS NOT NULL
THEN 
'N'
ELSE
ACTIVE_FLG END ACTV_FL
FROM 
LANDING.EMPLOYEE_DIM_STAGE
ORDER BY 1 ) TEMP
WHERE EMP.EMPLOYEE_ID=TEMP.EMPLOYEE_ID AND EMP.EFFECTIVE_START_DT=TEMP.EFFECTIVE_START_DT