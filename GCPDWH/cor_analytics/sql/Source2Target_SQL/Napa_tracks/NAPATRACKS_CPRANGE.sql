SELECT
  CAST (CPMATRIXID AS INT64) CP_MATRIX_ID,
  CAST (MAXCOST AS FLOAT64) MAX_COST,
  GPPCT AS GP_PCT
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_CPRANGE`