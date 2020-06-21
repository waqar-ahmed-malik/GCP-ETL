SELECT 
  Branch_Number,
  CAST('{{ ds_nodash }}' as int64) as fileid, 
  Business_Site, 
  Market_Code,
  Market,
  District_Code,
  District
FROM LANDING.WD_MARKET_HIERARCHY_RAW_{{ ds_nodash }}
WHERE 1=1
and trim(lower(Market)) <> 'inactive location'
ORDER BY 2,1;