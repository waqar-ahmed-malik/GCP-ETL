WITH part1 AS (
  SELECT 
    fileid,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(fileid AS String),r'(\d{2}\d{2}\d{2,4})')) AS file_date,
    COMPANY_NAME,
    Primary_Location_Code,
    ORG_TYP,
    ORG_CODE,
    MANAGER,
    REGEXP_EXTRACT(MANAGER,r"^.*\((\d+)\)") AS manager_emp_id,
    AVAIL_DATE,
    [
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_02,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_03,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_04,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_05,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_06,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_07,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_08,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_09,r"^.*\((\w+)\)")),'active'),
    COALESCE(LOWER(REGEXP_EXTRACT(LEVEL_10,r"^.*\((\w+)\)")),'active')
    ] as statuses,
    LEVEL_01,
    LEVEL_02,
    LEVEL_03,
    LEVEL_04,
    LEVEL_05,
    LEVEL_06,
    LEVEL_07,
    LEVEL_08,
    LEVEL_09,
    LEVEL_10
  FROM LANDING.WD_SUPERVISORY_HIERARCHY_DAILY_HISTORY
  WHERE org_code IS NOT NULL
), part2 AS (
  SELECT
    fileid,
    file_date,
    COMPANY_NAME,
    Primary_Location_Code,
    ORG_TYP,
    ORG_CODE,
    MANAGER,
    manager_emp_id,
    AVAIL_DATE,
    LEVEL_01,
    LEVEL_02,
    LEVEL_03,
    LEVEL_04,
    LEVEL_05,
    LEVEL_06,
    LEVEL_07,
    LEVEL_08,
    LEVEL_09,
    LEVEL_10
  FROM part1
  WHERE NOT EXISTS (
    SELECT *
    FROM UNNEST(statuses) AS x
    WHERE x = 'inactive')
), part3 AS (
  SELECT 
    ORG_CODE, 
    manager_emp_id,
    file_date,
    STRUCT(
      fileid,
      file_date,
      COMPANY_NAME,
      Primary_Location_Code,
      ORG_TYP,
      ORG_CODE,
      MANAGER,
      manager_emp_id,
      AVAIL_DATE,
      LEVEL_01,
      LEVEL_02,
      LEVEL_03,
      LEVEL_04,
      LEVEL_05,
      LEVEL_06,
      LEVEL_07,
      LEVEL_08,
      LEVEL_09,
      LEVEL_10
    ) AS row_data,
    FARM_FINGERPRINT(
      CONCAT(
      COALESCE(CAST(COMPANY_NAME AS STRING),''),
      COALESCE(CAST(Primary_Location_Code AS STRING),''),
      COALESCE(CAST(ORG_TYP AS STRING),''),
      COALESCE(CAST(ORG_CODE AS STRING),''),
      COALESCE(CAST(MANAGER AS STRING),''),
      COALESCE(CAST(AVAIL_DATE AS STRING),''),
      COALESCE(CAST(LEVEL_01 AS STRING),''),
      COALESCE(CAST(LEVEL_02 AS STRING),''),
      COALESCE(CAST(LEVEL_03 AS STRING),''),
      COALESCE(CAST(LEVEL_04 AS STRING),''),
      COALESCE(CAST(LEVEL_05 AS STRING),''),
      COALESCE(CAST(LEVEL_06 AS STRING),''),
      COALESCE(CAST(LEVEL_07 AS STRING),''),
      COALESCE(CAST(LEVEL_08 AS STRING),''),
      COALESCE(CAST(LEVEL_09 AS STRING),''),
      COALESCE(CAST(LEVEL_10 AS STRING),'')
    )) AS row_hash
  FROM part2
), part4 AS (
SELECT
    ORG_CODE, 
    manager_emp_id,
    row_hash,
    array_agg(row_data ORDER BY file_date) AS sh_hist
FROM part3
GROUP BY 1,2,3
)
SELECT
    ORG_CODE, 
    manager_emp_id,
    row_hash,
  (SELECT AS STRUCT 
      fileid,
      file_date,
      COMPANY_NAME,
      Primary_Location_Code,
      ORG_TYP,
      MANAGER,
      AVAIL_DATE,
      LEVEL_01,
      LEVEL_02,
      LEVEL_03,
      LEVEL_04,
      LEVEL_05,
      LEVEL_06,
      LEVEL_07,
      LEVEL_08,
      LEVEL_09,
      LEVEL_10
   FROM UNNEST(sh_hist)
   ORDER BY file_date 
   LIMIT 1).*
FROM part4
ORDER BY 1,5;
