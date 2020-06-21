CREATE OR REPLACE TABLE REFERENCE.CONNECTSUITE_M_BRANCH AS
(select
CAST(BRN_KY AS INT64) AS BRN_KY,
BRN_CD,
BRN_NM,
BRN_BSC_AD,
BRN_SUPL_AD,
BRN_CTY_NM,
BRN_ST_PROV_CD,
BRN_ZIP_CD,
CAST(DIV_KY AS INT64) AS DIV_KY,
BRN_TEL_NR,
BRN_CO_CD,
BRN_BUDGT_CTR_CD,
BRN_SUBCO_CD,
BRN_AVATAR_DSN_NM_,
BRN_RT_MRCHT_NB,
BRN_MRCHT_NB,
"jobrunid" as JOB_RUN_ID,
'CONNECT SUITE' as SOURCE_SYSTEM_CD,
CURRENT_DATETIME() as CREATE_DT
from
LANDING.WORK_M_BRANCH)