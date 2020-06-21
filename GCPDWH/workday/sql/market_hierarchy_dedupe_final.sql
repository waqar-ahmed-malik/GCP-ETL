WITH part1 AS (
SELECT
  Branch_Number ,
  PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS file_date,
  STRUCT(
    Business_Site,
    Market_Code,
    Market,
    District_Code,
    District,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS file_date,
    fileid
   ) AS row_data,
  FARM_FINGERPRINT(
    CONCAT(
        COALESCE(CAST(Business_Site AS STRING),''),
        COALESCE(CAST(Market_Code AS STRING),''),
        COALESCE(CAST(Market AS STRING),''),
        COALESCE(CAST(District_Code AS STRING),''),
        COALESCE(CAST(District AS STRING),'')
        )
    ) AS row_hash
FROM LANDING.WD_MARKET_HIERARCHY_DAILY_HISTORY
), part2 AS (
SELECT
    Branch_Number, 
    row_hash,
    array_agg(row_data ORDER BY file_date) AS mh_hist
FROM part1
GROUP BY 1,2
)
SELECT
  Branch_Number,
  row_hash,
  (SELECT AS STRUCT 
    Business_Site,
    Market_Code,
    Market,
    District_Code,
    District,
    file_date,
    fileid
   FROM unnest(mh_hist)
   ORDER BY file_date 
   LIMIT 1).*
FROM part2
ORDER BY 1,8