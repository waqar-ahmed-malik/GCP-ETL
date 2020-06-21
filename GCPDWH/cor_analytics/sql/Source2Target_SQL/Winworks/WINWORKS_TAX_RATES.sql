SELECT
  CAST (ID AS int64 ) AS ID,
  CAST (TAX AS int64 ) AS TAX,
  CAST (PRATE AS float64 ) AS PRATE,
  CAST (LRATE AS float64 ) AS LRATE,
  NAME,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME,
  SOURCE_SYSTEM_CD
FROM
  `aaadata-181822.COR_WINWORKS.STG_TAX_RATES`