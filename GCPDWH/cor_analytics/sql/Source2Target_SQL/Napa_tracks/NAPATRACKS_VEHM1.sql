SELECT
  CAST(VEHID AS INT64) VEH_ID,
  CAST(M1YEAR AS INT64) M1_YEAR,
  CAST(M1MODELID AS INT64) M1_MODEL_ID,
  M1MAKE AS M1_MAKE,
  M1MODEL AS M1_MODEL,
  M1SUBMODEL AS M1_SUB_MODEL,
  CAST(M1MAKEID AS INT64) M1_MAKE_ID
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_VEHM1`