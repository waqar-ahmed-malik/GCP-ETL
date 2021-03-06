CREATE OR REPLACE  TABLE OPERATIONAL.GLOBALWARE_PROVIDER AS (
SELECT

ACCOUNT_ID,
OUR_ID,
SAFE_CAST(MULT_ITEMS AS INT64)  AS MULT_ITEMS,
SAFE_CAST(CENTRAL_PAY AS INT64) AS CENTRAL_PAY ,
PREFERRED,
GL_CHART,
SAFE_CAST(DUE_DAY AS INT64)  AS DUE_DAY,
SAFE_CAST(ON_HOLD AS INT64)  AS ON_HOLD,
ADDL_CMT1,
ADDL_CMT2,
ADDL_CMT3,
ADDL_CMT4,
MARK_1099,
TAX_ID,
SALUTATION,
PROPERTY_1V,
PROPERTY_1P,
"jobrunid" AS JOB_RUN_ID,
"GLOBALWARE" AS SOURCE_SYSTEM_CD,
CURRENT_DATE() AS CREATE_DT 
FROM LANDING.WORK_GW_PROVIDER)


