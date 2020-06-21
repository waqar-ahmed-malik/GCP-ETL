CREATE OR REPLACE TABLE REFERENCE.CONNECTSUITE_CS_ROLES AS
(select
ROLE_ID,
NAME,
"jobrunid" as JOB_RUN_ID,
'CONNECT SUITE' as SOURCE_SYSTEM_CD,
CURRENT_DATETIME() as CREATE_DT
from
LANDING.WORK_CS_ROLES)
