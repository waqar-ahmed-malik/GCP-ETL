INSERT INTO  OPERATIONAL.AV_HVECTOR(ROW_DATE,
STARTTIME,
STARTTIME_UTC,
INTRVL,
ACD,
VECTOR,
INCALLS,
INTIME,
ANSTIME,
ACDCALLS,
BACKUPCALLS,
ABNCALLS,
ABNTIME,
ABNQUECALLS,
BUSYCALLS,
BUSYTIME,
DISCCALLS,
DISCTIME,
OTHERCALLS,
OTHERTIME,
OUTFLOWCALLS,
OUTFLOWTIME,
INTERFLOWCALLS,
GOTOCALLS,
GOTOTIME,
LOOKATTEMPTS,
LOOKFLOWCALLS,
ADJATTEMPTS,
ADJROUTED,
INFLOWCALLS,
ABNRINGCALLS,
RINGTIME,
RINGCALLS,
INCOMPLETE,
PHANTOMABNS,
VDISCCALLS,
DEFLECTCALLS,
NETDISCCALLS,
NETPOLLS,
ACDCALLS_R1,
ACDCALLS_R2,
ICRPULLCALLS,
ICRPULLTIME,
ICRPULLQUECALLS,
ICRPULLRINGCALLS,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DTTIME,
LAST_UPDATE_DTTIME)
SELECT 
SAFE_CAST(ROW_DATE AS DATE),
SAFE_CAST(STARTTIME  AS INT64),
SAFE_CAST(STARTTIME_UTC  AS INT64),
SAFE_CAST(INTRVL  AS INT64),
SAFE_CAST(ACD  AS INT64),
SAFE_CAST(VECTOR  AS INT64),
SAFE_CAST(INCALLS  AS INT64),
SAFE_CAST(INTIME  AS INT64),
SAFE_CAST(ANSTIME  AS INT64),
SAFE_CAST(ACDCALLS  AS INT64),
SAFE_CAST(BACKUPCALLS  AS INT64),
SAFE_CAST(ABNCALLS  AS INT64),
SAFE_CAST(ABNTIME  AS INT64),
SAFE_CAST(ABNQUECALLS  AS INT64),
SAFE_CAST(BUSYCALLS  AS INT64),
SAFE_CAST(BUSYTIME  AS INT64),
SAFE_CAST(DISCCALLS  AS INT64),
SAFE_CAST(DISCTIME  AS INT64),
SAFE_CAST(OTHERCALLS  AS INT64),
SAFE_CAST(OTHERTIME  AS INT64),
SAFE_CAST(OUTFLOWCALLS  AS INT64),
SAFE_CAST(OUTFLOWTIME  AS INT64),
SAFE_CAST(INTERFLOWCALLS  AS INT64),
SAFE_CAST(GOTOCALLS  AS INT64),
SAFE_CAST(GOTOTIME  AS INT64),
SAFE_CAST(LOOKATTEMPTS  AS INT64),
SAFE_CAST(LOOKFLOWCALLS  AS INT64),
SAFE_CAST(ADJATTEMPTS  AS INT64),
SAFE_CAST(ADJROUTED  AS INT64),
SAFE_CAST(INFLOWCALLS  AS INT64),
SAFE_CAST(ABNRINGCALLS  AS INT64),
SAFE_CAST(RINGTIME  AS INT64),
SAFE_CAST(RINGCALLS  AS INT64),
SAFE_CAST(INCOMPLETE  AS INT64),
SAFE_CAST(PHANTOMABNS  AS INT64),
SAFE_CAST(VDISCCALLS  AS INT64),
SAFE_CAST(DEFLECTCALLS  AS INT64),
SAFE_CAST(NETDISCCALLS  AS INT64),
SAFE_CAST(NETPOLLS  AS INT64),
SAFE_CAST(ACDCALLS_R1  AS INT64),
SAFE_CAST(ACDCALLS_R2  AS INT64),
SAFE_CAST(ICRPULLCALLS  AS INT64),
SAFE_CAST(ICRPULLTIME  AS INT64),
SAFE_CAST(ICRPULLQUECALLS  AS INT64),
SAFE_CAST(ICRPULLRINGCALLS  AS INT64),
'jobrunid',
"AVAYA",
current_datetime(),
current_datetime()
FROM LANDING.WORK_AV_HVECTOR