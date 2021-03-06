SELECT
  AUTOID AS AUTO_ID,
  CAST(SERVICENO AS int64) SERVICE_NUMBER,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    LASTDATE) AS LAST_DATE,
  CAST(LASTMILEAGE AS int64)LAST_MILEAGE,
  ORDERNUMBER AS ORDER_NUMBER,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    DUEDATE) AS DUE_DATE,
  CAST(DUEMILEEAGE AS int64) DUE_MILEAGE,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    REMINDERDATE) AS REMINDER_DATE,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME,
  COLUMN1SOURCE_SYSTEM_CD AS SOURCE_SYSTEM_CD
FROM
  `aaadata-181822.COR_WINWORKS.STG_HISTORY`