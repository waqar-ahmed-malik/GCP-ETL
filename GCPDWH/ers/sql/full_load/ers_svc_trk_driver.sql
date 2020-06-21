CREATE OR REPLACE TABLE REFERENCE.ERS_SVC_TRK_DRIVER
AS (SELECT
SVC_FACL_ID, 
CAST(TRK_DRIVR_ID AS INT64) as TRK_DRIVR_ID, 
CASE WHEN (LENGTH(TRIM(TRK_DRIVR_ID)) < 6 AND UNLOCK_LICENSE_ID IS NOT NULL) THEN CAST(UNLOCK_LICENSE_ID AS INT64)
    ELSE CAST(TRK_DRIVR_ID AS INT64) END AS DERIVED_TRUCK_DRIVR_ID,
TRK_DRIVR_FST_NM, 
TRK_DRIVR_LST_NM, 
TRK_DRIVR_TEL_NR, 
TRK_DRIVR_TEL_X_NR, 
TRK_DRIVR_TEL_T_CD, 
PAGER_PROVIDER_ID, 
PAGER_TYPE, 
PAGER_PIN, 
TRK_DRIVR_BSC_AD, 
TRK_DRIVR_CTY_NM, 
TRK_DRIVR_ST_CD, 
TRK_DRIVR_ZIP_CD, 
UNLOCK_LICENSE_ID, 
CAST(substr(UNLOCK_LICENSE_DT,1,10) AS DATE) as UNLOCK_LICENSE_DT, 
TRK_DRIVR_COMMENTS, 
TECH_LEVEL, 
TRK_DRIVR_RPST_TRAINED, 
CAST(substr(TRK_DRIVR_RPST_TRAINED_DT,1,10) AS DATE) as TRK_DRIVR_RPST_TRAINED_DT, 
CAST(DRIVER_PREFERENCE AS INT64) as DRIVER_PREFERENCE, 
EMAILADDRESS, 
CAST(LAST_UPDT_TS AS TIMESTAMP) as LAST_UPDT_TS, 
LOGIN, 
DRIVER_ROLE, 
SPMG_GROUP, 
DRIVER_PHOTO_URL, 
PASSWORDHASH, 
CAST(PASSWORDGRACEUSERTIME AS INT64) as PASSWORDGRACEUSERTIME, 
CAST(PASSWORDEXPIRATIONTIME AS TIMESTAMP) as PASSWORDEXPIRATIONTIME,
'jobrunid' as JOB_RUN_ID,
'ERS' as SOURCE_SYSTEM_CD,
CURRENT_DATETIME() as CREATE_DT
from LANDING.WORK_ERS_SVC_TRK_DRIVER);
