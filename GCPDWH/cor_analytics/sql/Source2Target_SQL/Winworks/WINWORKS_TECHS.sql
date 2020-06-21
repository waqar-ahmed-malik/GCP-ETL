SELECT
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    W_DATE) AS W_DATE,
  ORDER_NUMBER,
  TECH_ID,
  CAST (TASK AS INT64) AS TASK,
  CAST (HOURS AS FLOAT64) AS HOURS,
  DESCRIPTION,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME,
  CREATE_BY,
  SOURCE_SYSTEM_CD
FROM
  `aaadata-181822.COR_WINWORKS.STG_TECHS`