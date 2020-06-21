SELECT
  NAME AS SERVICE_CATEGORY_NAME,
  CAT AS SERVICE_CATEGORY_CODE,
  SOURCE_SYSTEM_CD CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME
FROM
  `aaadata-181822.COR_WINWORKS.STG_CATEGORY`