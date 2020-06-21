WITH base AS (
  SELECT DISTINCT
    Empid,
    PARSE_DATE('%Y-%m-%d',REGEXP_EXTRACT(CAST(HIRE_DT AS String),r'(\d{4}-\d{2}-\d{2})')) AS HIRE_DT
  FROM LANDING.WD_WORKER_DAILY_HISTORY
  ORDER BY 1,2
  ), part1 AS ( 
SELECT
  Empid,
  HIRE_DT AS hire_dt_start,
    COALESCE(
      DATE_SUB(
        LEAD(HIRE_DT) OVER (PARTITION BY Empid ORDER BY HIRE_DT)
        , INTERVAL 1 DAY)
        , '9999-12-31'
    ) as hire_dt_end
FROM base
ORDER BY 1,2,3
)
SELECT * FROM part1