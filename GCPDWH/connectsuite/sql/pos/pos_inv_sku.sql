INSERT INTO OPERATIONAL.POS_INV_SKU(
INV_SKU,
STATUS,
INV_SKU_DESC,
MEMBER_PRC,
NONMEMBER_PRC,
LIST_PRC,
LAST_UPDATE,
ORG_INV_SKU,
INV_ITM_NR,
INV_ITM_TAXABLE,
LAST_UPDATE_DTTIME,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DT
)
SELECT 
INV_SKU,
STATUS,
INV_SKU_DESC,
SAFE_CAST(MEMBER_PRC AS FLOAT64),
SAFE_CAST(NONMEMBER_PRC AS FLOAT64),
SAFE_CAST(LIST_PRC AS FLOAT64),
LAST_UPDATE,
ORG_INV_SKU,
INV_ITM_NR,
INV_ITM_TAXABLE,
CURRENT_DATETIME(),
'jobrunid',
'POS' ,
CURRENT_DATE()
FROM LANDING.WORK_POS_INV_SKU
