CREATE OR REPLACE TABLE REFERENCE.CONNECTSUITE_CS_EMPLOYEEBRANCH AS
(select
EMPLOYEE_ID,
CLUB_CD,
BRANCH,
ROLE_ID,
"jobrunid" as JOB_RUN_ID,
'CONNECT SUITE' as SOURCE_SYSTEM_CD,
CURRENT_DATETIME() as CREATE_DT
from
LANDING.WORK_CS_EMPLOYEEBRANCH)