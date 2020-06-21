CREATE OR REPLACE TABLE REFERENCE.CONNECTSUITE_POS_INV_SKU AS
(select
ASSOC_CD,
CLB_CD,
CAST(INV_SKU_ID AS INT64) AS INV_SKU_ID,
CAST(INV_ITM_ID AS INT64) AS INV_ITM_ID,
INV_SKU,
STATUS,
CAST(TRIM(INV_SKU_EFF_DT) AS DATETIME) AS INV_SKU_EFF_DT,
CAST(TRIM(INV_SKU_EXP_DT) AS DATETIME) AS INV_SKU_EXP_DT,
INV_SKU_DESC,
INV_SKU_UOM,
CAST(INV_SKU_WEIGHT AS FLOAT64) AS INV_SKU_WEIGHT,
CAST(INV_SKU_SHP_CST AS FLOAT64) AS INV_SKU_SHP_CST,
CAST(INV_SKU_HNDL_CST AS FLOAT64) AS INV_SKU_HNDL_CST,
CAST(INV_SKU_SHP_CT AS INT64) AS INV_SKU_SHP_CT,
CAST(MEMBER_PRC AS FLOAT64) AS MEMBER_PRC,
CAST(NONMEMBER_PRC AS FLOAT64) AS NONMEMBER_PRC,
CAST(EMPLOYEE_PRC AS FLOAT64) AS EMPLOYEE_PRC,
CAST(LIST_PRC AS FLOAT64) AS LIST_PRC,
CAST(MEMBER_SAVINGS_AMT AS FLOAT64) AS MEMBER_SAVINGS_AMT,
CAST(UNIT_COST AS FLOAT64) AS UNIT_COST,
CAST(PRE_PUB_PRC AS FLOAT64) AS PRE_PUB_PRC,
CAST(AFTER_PUB_PRC AS FLOAT64) AS AFTER_PUB_PRC,
CAST(EDITION_YEAR AS INT64) AS EDITION_YEAR,
CAST(REVISION_MONTH AS INT64) AS REVISION_MONTH,
ZERO_PRICE_IND,
CAST(TRIM(LAST_UPDATE) AS DATETIME) AS LAST_UPDATE,
USER_DATA_FLAG,
USER_DATA_DESC,
DEFAULT_TO_CASH_ADV,
FOREIGN_CURRENCY,
PROMO_OVERRIDE_SKU,
TRAVEL_MONEY,
WARRANTY,
OFAC_COMP_REQ,
LTV_CALC_FUNCT,
CAST(LTV_CALC_AMOUNT AS INT64) AS LTV_CALC_AMOUNT,
"jobrunid" as JOB_RUN_ID,
'CONNECT SUITE' as SOURCE_SYSTEM_CD,
CURRENT_DATETIME() as CREATE_DT
from
LANDING.WORK_POS_INV_SKU)