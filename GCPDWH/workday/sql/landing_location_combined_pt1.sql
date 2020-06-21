WITH part1 AS (
SELECT
    WD_LOCATION_ID,
    LOCATION_NAME,
    STR_ADDRESS_1,
    STR_ADDRESS_2,
    STR_ADDRESS_3,
    CITY_NM,
    STATE_CD,
    ZIP_CD,
    EMAIL_ADDRESS,
    PHONE_NUMBER,
    PHONE_EXTNSN,
    LOCATION_STATUS,
    EFFECTIVE_DATE_FOR_LOCATION,
    LOCATION_TYPES,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS file_date,
    fileid 
FROM LANDING.WD_LOCATION_DAILY_HISTORY
ORDER BY WD_LOCATION_ID, file_date
), part2 AS (
SELECT
    Branch_Number ,
    Business_Site,
    Market_Code,
    Market,
    District_Code,
    District,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS file_date,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS start_date,
    COALESCE(DATE_SUB(PARSE_DATE('%Y%m%d',LEAD(REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) OVER (PARTITION BY Branch_Number ORDER BY fileid)), INTERVAL 1 DAY),'9999-12-31') AS end_date,
    fileid
FROM LANDING.WD_MARKET_HIERARCHY_DAILY_HISTORY
ORDER BY Branch_Number, file_date
), part3 AS (
select 
    aa.WD_LOCATION_ID,
    aa.file_date,
   STRUCT(
    aa.LOCATION_NAME,
    aa.STR_ADDRESS_1,
    aa.STR_ADDRESS_2,
    aa.STR_ADDRESS_3,
    aa.CITY_NM,
    aa.STATE_CD,
    aa.ZIP_CD,
    aa.EMAIL_ADDRESS,
    aa.PHONE_NUMBER,
    aa.PHONE_EXTNSN,
    aa.LOCATION_STATUS,
    aa.EFFECTIVE_DATE_FOR_LOCATION,
    aa.LOCATION_TYPES,
    bb.Branch_Number ,
    bb.Business_Site,
    bb.Market_Code,
    bb.Market,
    bb.District_Code,
    bb.District,
    aa.file_date,
    aa.fileid
    ) as row_data,
     FARM_FINGERPRINT(
    CONCAT(
        COALESCE(CAST(LOCATION_NAME AS STRING),''),
        COALESCE(CAST( STR_ADDRESS_1 AS STRING),''),
        COALESCE(CAST( STR_ADDRESS_2 AS STRING),''),
        COALESCE(CAST( STR_ADDRESS_3 AS STRING),''),
        COALESCE(CAST( CITY_NM AS STRING),''),
        COALESCE(CAST( STATE_CD AS STRING),''),
        COALESCE(CAST( ZIP_CD AS STRING),''),
        COALESCE(CAST( EMAIL_ADDRESS AS STRING),''),
        COALESCE(CAST( PHONE_NUMBER AS STRING),''),
        COALESCE(CAST( PHONE_EXTNSN AS STRING),''),
        COALESCE(CAST( LOCATION_STATUS AS STRING),''),
        COALESCE(CAST( EFFECTIVE_DATE_FOR_LOCATION AS STRING),''),
        COALESCE(CAST( ARRAY_LENGTH(LOCATION_TYPES) AS STRING),''),
        COALESCE(CAST(Business_Site AS STRING),''),
        COALESCE(CAST(Market_Code AS STRING),''),
        COALESCE(CAST(Market AS STRING),''),
        COALESCE(CAST(District_Code AS STRING),''),
        COALESCE(CAST(District AS STRING),'')
        )
    ) AS row_hash
from part1 aa
left join part2 bb on (aa.WD_LOCATION_ID = bb.Branch_Number 
and aa.file_date >= bb.start_date
and aa.file_date <= bb.end_date)
), part4 AS (
SELECT
    WD_LOCATION_ID, 
    row_hash,
    array_agg(row_data ORDER BY file_date) AS loc_hist
FROM part3
GROUP BY 1,2
)
SELECT
  WD_LOCATION_ID,
  row_hash,
  (SELECT AS STRUCT 
    LOCATION_NAME,
    STR_ADDRESS_1,
    STR_ADDRESS_2,
    STR_ADDRESS_3,
    CITY_NM,
    STATE_CD,
    ZIP_CD,
    EMAIL_ADDRESS,
    PHONE_NUMBER,
    PHONE_EXTNSN,
    LOCATION_STATUS,
    EFFECTIVE_DATE_FOR_LOCATION,
    LOCATION_TYPES,
    Branch_Number ,
    Business_Site,
    Market_Code,
    Market,
    District_Code,
    District,
    file_date,
    fileid
   FROM unnest(loc_hist)
   ORDER BY file_date 
   LIMIT 1).*
FROM part4
ORDER BY 1,22