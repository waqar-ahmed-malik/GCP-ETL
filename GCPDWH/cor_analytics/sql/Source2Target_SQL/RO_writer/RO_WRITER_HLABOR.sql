SELECT
  HLABORID,
  LOACTIONNO,
  RO_NO,
  PARSE_DATE('%m/%d/%Y',
    SUBSTR(RO_DATE,1,10)) AS RO_DATE,
  TECH_NO,
  DESC_LINES,
  CATAGORY,
  CAST (TIME AS FLOAT64)TIME,
  CAST (RATE AS FLOAT64)RATE,
  CAST (CHARGE AS FLOAT64)CHARGE,
  CAST (T_COST AS FLOAT64)T_COST,
  DECLINED,
  UNDECLINED,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME,
  CREATE_BY
FROM
  `aaadata-181822.COR_RO_WRITER.STG_HLABOR`