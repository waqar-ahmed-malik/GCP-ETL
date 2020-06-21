WITH part1 as (
SELECT
    Cost_Center_CD,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) as file_date,
  STRUCT(
    Cost_Center_Desc,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) as file_date,
    fileid
   ) as row_data,
  FARM_FINGERPRINT(
    COALESCE(CAST( Cost_Center_Desc AS STRING),'')
    ) as row_hash
FROM LANDING.WD_COSTCENTER_DAILY_HISTORY
), part2 as (
select
    Cost_Center_CD, 
    row_hash,
    array_agg(row_data order by file_date) as cc_hist
from part1
group by 1,2
)
select
  Cost_Center_CD,
  row_hash,
  (SELECT AS STRUCT 
    Cost_Center_Desc,
    file_date,
    fileid
   FROM unnest(cc_hist)
   ORDER BY file_date 
   LIMIT 1).*
from part2
order by 1,4;