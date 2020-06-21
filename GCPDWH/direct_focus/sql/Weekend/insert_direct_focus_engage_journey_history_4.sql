INSERT INTO `DIGITAL_MARKETING.DIRECT_FOCUS_ENGAGE_JOURNEY_HISTORY` (
AC_CUSTOMER_ID ,		
HUB_JOURNEY_HISTORY_ID,	
HUB_CIDG_UID,	
JOURNEY_CD,	
JOURNEY_DESC,	
JOURNEY_END_DTTIME,	
JOURNEY_ID ,	
JOURNEY_ITERATION,	
JOURNEY_NM,	
JOURNEY_START_DTTIME,	
NODE_DTTIME	,	
NODE_NM	,	
NODE_TYPE ,	
NODE_CATEGORY ,	
ARCHIVE_DTTIME ,	
JOB_RUN_ID ,	
SOURCE_SYSTEM_CD ,	
CREATE_DT
)
SELECT
AC_CUSTOMER_ID,	
HUB_JOURNEY_HISTORY_ID,	
HUB_CIDG_UID,	
JOURNEY_CD,	
JOURNEY_DESC,	
PARSE_DATETIME('%Y-%m-%d %H:%M:%S',JOURNEY_END_DTTIME),	
JOURNEY_ID,	
SAFE_CAST(JOURNEY_ITERATION AS INT64),	
JOURNEY_NM,	
PARSE_DATETIME('%Y-%m-%d %H:%M:%S',JOURNEY_START_DTTIME),	
PARSE_DATETIME('%Y-%m-%d %H:%M:%S',NODE_DTTIME),	
NODE_NM,	
NODE_TYPE,	
NODE_CATEGORY,	
PARSE_DATETIME('%Y-%m-%d %H:%M:%S',ARCHIVE_DTTIME),
SAFE_CAST('jobrunid' AS INT64),
'DIRECT_FOCUS',
CURRENT_DATE()
FROM `LANDING.WORK_DF_ENGAGE_JOURNEY_HISTORY_4`