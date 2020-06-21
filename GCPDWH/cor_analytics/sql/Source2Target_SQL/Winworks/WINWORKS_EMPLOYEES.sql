SELECT
  NAME,
  ADDRESS1,
  ADDRESS2,
  CITY,
  STATE,
  ZIPCODE,
  HOMEPHONE,
  OTHERPHONE,
  NICKNAME,
  IDNUMBER,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    STARTDATE) AS STARTDATE,
  WORKSCHEDULE,
  JOBCLASS,
  JOBGRADE,
  PAYTYPE,
  CAST(PAYRATE AS FLOAT64)PAYRATE,
  PASSWORD,
  SECURITY,
  CAST(LABORCOM AS INT64)LABORCOM,
  CAST(PARTSCOM AS INT64)PARTSCOM,
  CAST(STATUS AS INT64)STATUS,
  NOTES,
  CAST(ID AS INT64)ID,
  CAST(TLTIER AS INT64)TLTIER,
  CAST(SCHORDER AS INT64)SCHORDER,
  SECURITY2,
  EMAIL,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME,
  SOURCE_SYSTEM_CD
FROM
  `aaadata-181822.COR_WINWORKS.STG_EMPLOYEES`