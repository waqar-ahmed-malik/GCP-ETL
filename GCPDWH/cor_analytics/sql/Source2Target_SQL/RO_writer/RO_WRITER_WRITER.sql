SELECT 
CAST(LOCATIONNO AS INT64) AS LOCATION_NO,
WRITER,
WRITER_NAM AS WRITER_NAME,
PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p', CREATE_DTTIME) AS CREATE_DTTIME, 
CREATE_BY 
FROM `aaadata-181822.COR_RO_WRITER.STG_WRITER`