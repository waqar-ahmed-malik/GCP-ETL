SELECT
  DEPT_ID,
  DEPT_NAME,
  ACCT_NAME,
  STATUS,
  DEFAULT_DEPT,
  SOURCE_SYSTEM_CD,
  CREATE_BY,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME
FROM
  `aaadata-181822.COR_WINWORKS.STG_DEPARTMENTS`