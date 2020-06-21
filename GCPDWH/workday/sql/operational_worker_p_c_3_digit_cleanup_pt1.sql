    SELECT 
      fileid as c, 
    (SELECT AS STRUCT 
      Worker_P_C_3_Digit as a,
      HIRE_DT as b
     FROM UNNEST(INSURANCE_REPS) ORDER BY HIRE_DT DESC LIMIT 1).*
    FROM (
    WITH base AS (
  select distinct case when TERMINATION_DT IS NULL then Worker_P_C_3_Digit else null end as Worker_P_C_3_Digit, HIRE_DT, fileid
  from LANDING.WD_WORKER_DAILY_HISTORY
  where (case when TERMINATION_DT IS NULL then Worker_P_C_3_Digit else null end) IS NOT NULL
  order by 1
  ), part1 as (
  select 
    Worker_P_C_3_Digit as INSURANCE_REP_ID,
    struct(Worker_P_C_3_Digit, HIRE_DT) as INSURANCE_REP,
    fileid
  from base
  )
  select
    INSURANCE_REP_ID,
    fileid,
    array_agg(INSURANCE_REP) AS INSURANCE_REPS
  from part1
  group by 
    INSURANCE_REP_ID,
    fileid  )