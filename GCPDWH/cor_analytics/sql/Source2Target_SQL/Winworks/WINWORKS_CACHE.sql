SELECT
  CHR_7,
  NOTES,
  CAST(SVAL_1 AS FLOAT64)SVAL_1,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME,
  SOURCE_SYSTEM_CD
FROM
  `aaadata-181822.COR_WINWORKS.STG_CACHE`