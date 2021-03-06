SELECT
  MATRIX_ID,
  C_TYPE,
  CAST (P_LEVEL AS INT64)AS P_LEVEL,
  CAST (THRESHOLD AS FLOAT64)AS THRESHOLD,
  CAST (MARGIN AS INT64)AS MARGIN,
  SOURCE_SYSTEM_CD,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME
FROM
  `aaadata-181822.COR_WINWORKS.STG_PRICING`