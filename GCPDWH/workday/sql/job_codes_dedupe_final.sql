WITH part1 AS (
SELECT
    JOB_CD,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS file_date,
  STRUCT(
    JOB_CD_DESC,
    JOB_CD_EFFECTIVE_DT,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS file_date,
    fileid
   ) AS row_data,
  FARM_FINGERPRINT(
    CONCAT(
        COALESCE(CAST(JOB_CD_DESC AS STRING),''),
        COALESCE(CAST(JOB_CD_EFFECTIVE_DT AS STRING),'')
        )
    ) AS row_hash
FROM LANDING.WD_JOB_CODES_DAILY_HISTORY
), part2 AS (
SELECT
    JOB_CD, 
    row_hash,
    array_agg(row_data ORDER BY file_date) AS jc_hist
from part1
group by 1,2
)
SELECT
  JOB_CD,
  row_hash,
  (SELECT AS STRUCT 
    JOB_CD_DESC,
    JOB_CD_EFFECTIVE_DT,
    file_date,
    fileid
   FROM unnest(jc_hist)
   ORDER BY file_date 
   LIMIT 1).*
FROM part2
ORDER BY 1,5;