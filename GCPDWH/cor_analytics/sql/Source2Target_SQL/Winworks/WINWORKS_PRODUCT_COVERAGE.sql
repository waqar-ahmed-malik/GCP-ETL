SELECT
  MFG_NAME,
  CAST (CAT_CODE AS INT64) CAT_CODE,
  DESCRIPTION,
  MFG_PRC,
  MFG_CODE,
  SOURCE_SYSTEM_CD,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME
FROM
  `aaadata-181822.COR_WINWORKS.STG_PRODUCT_COVERAGE`