WITH base AS (
  SELECT DISTINCT
    Empid,
    PARSE_DATE('%Y-%m-%d',REGEXP_EXTRACT(CAST(HIRE_DT AS String),r'(\d{4}-\d{2}-\d{2})')) AS HIRE_DT,
    MAX(CASE 
      WHEN PARSE_DATE('%Y-%m-%d',REGEXP_EXTRACT(CAST(HIRE_DT AS String),r'(\d{4}-\d{2}-\d{2})')) > PARSE_DATE('%Y-%m-%d',REGEXP_EXTRACT(CAST(TERMINATION_DT AS String),r'(\d{4}-\d{2}-\d{2})')) THEN NULL 
      ELSE PARSE_DATE('%Y-%m-%d',REGEXP_EXTRACT(CAST(TERMINATION_DT AS String),r'(\d{4}-\d{2}-\d{2})')) END) AS TERMINATION_DT
  from LANDING.WD_WORKER_DAILY_HISTORY
  GROUP BY 1,2
  ORDER BY 2,3
  ), part1 AS ( 
select
  Empid,
  'Terminated' AS EMPLOYEE_STATUS,
  HIRE_DT,
  TERMINATION_DT AS termination_start,
  CASE WHEN TERMINATION_DT IS NOT NULL THEN 
    COALESCE(
      DATE_SUB(
        LEAD(HIRE_DT) OVER (PARTITION BY Empid ORDER BY HIRE_DT, TERMINATION_DT)
        , INTERVAL 1 DAY)
        , '9999-12-31'
    ) ELSE NULL END AS termination_end
FROM base
ORDER BY 1,3
)
SELECT * FROM part1
WHERE termination_start IS NOT NULL