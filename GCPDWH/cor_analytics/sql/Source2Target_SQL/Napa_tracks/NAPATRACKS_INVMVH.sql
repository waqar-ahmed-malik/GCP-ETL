SELECT
  INVMVID AS INVMV_ID,
  REFID AS REF_ID,
  INVMVTYPE AS INVMV_TYPE,
  PARSE_DATETIME('%m/%d/%Y %H:%M %p',
    TRXTIME) TRX_TIME
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_INVMVH`