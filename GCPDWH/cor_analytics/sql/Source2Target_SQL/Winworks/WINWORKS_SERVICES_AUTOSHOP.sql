SELECT
  SERVICENO AS SERVICE_NUMBER,
  SERVICEDESC AS SERVICE_DESCRIPTION,
  STATUS,
  SOURCE_SYSTEM_CD,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME
FROM
  `aaadata-181822.COR_WINWORKS.STG_SERVICES_AUTOSHOP`