CREATE OR REPLACE TABLE OPERATIONAL.POS_INV_CAT AS (
SELECT ASSOC_CD,
CLB_CD,
INV_CAT_CD,
INV_CAT_DESC,
INV_CAT_GL_PREFIX,
STATUS,
PROMO_OVERRIDE_CAT,
'jobrunid' AS JOB_RUN_ID,
'POS' AS SOURCE_SYSTEM_CD,
CURRENT_DATETIME() AS CREATE_DTTIME
FROM LANDING.WORK_POS_INV_CAT
)