SELECT
CLB_CD,
CL_NM,
CL_PLS_MBR_IN,
MBR_VERF_TEL_NR,
MBR_VERF_TEL_X_NR,
DIVISION_FLG,
CAST(PARSE_TYPE AS INT64) AS PARSE_TYPE,
CDX_SUPPORT,
CAST(ENTITLEMENT_TYPE AS INT64) AS ENTITLEMENT_TYPE,
CLUB_TYPE,
LOCALE,
CAST(DEFAULT_PTA AS INT64) AS DEFAULT_PTA,
CAST(LAST_DEF_PTA_OVERRIDE AS DATE) AS LAST_DEF_PTA_OVERRIDE,
CALLMOVER_FLAG,
CAST(SRC.JOB_RUN_ID AS INT64) AS ETL_JOB_RUN_ID,
SRC.SOURCE_SYSTEM_CD,
SRC.CREATED_BY,
current_datetime() AS CREATE_DTTIME
FROM LANDING.WORK_ERS_STG_CAD_CLUB_INFO SRC