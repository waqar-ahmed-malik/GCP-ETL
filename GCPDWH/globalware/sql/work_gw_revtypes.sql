SELECT 
TRIM(CONVERT(CHAR,RevType)) AS REV_TYPE,
TRIM(CONVERT(CHAR,TravType)) AS TRAV_TYPE,
TRIM(CONVERT(CHAR,Description)) AS DESCRIPTION
FROM  DBA.RevenueTypes