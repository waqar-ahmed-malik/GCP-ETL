SELECT
CAST (VEHID AS INT64) VEH_ID,
CAST(PPYEAR AS INT64) PP_YEAR,
CAST(PPMAKEID AS INT64) PP_MAKE_ID,
PPMAKENAME AS PP_MAKE_NAME,
CAST (PPMODELID AS INT64)PP_MODEL_ID,
PPMODELNAME AS PP_MODEL_NAME,
CAST (PPENGINEID AS INT64) PP_ENGINE_ID,
PPENGINENAME AS PP_ENGINE_NAME
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_VEHPP`