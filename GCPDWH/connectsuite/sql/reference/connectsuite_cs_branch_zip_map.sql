CREATE OR REPLACE TABLE REFERENCE.CONNECTSUITE_CS_BRANCH_ZIP_MAP AS
(SELECT 
CAST(TRIM(START_DATE) AS DATETIME) AS START_DATE,
CAST(TRIM(END_DATE) AS DATETIME) AS END_DATE,
BRANCH,
ZIP,
STATE,
"jobrunid" as JOB_RUN_ID,
'CONNECT SUITE' as SOURCE_SYSTEM_CD,
CURRENT_DATETIME() as CREATE_DT
from
LANDING.WORK_CS_BRANCH_ZIP_MAP)