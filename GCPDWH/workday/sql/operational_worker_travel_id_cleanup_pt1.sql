WITH base AS (
  select distinct case when TERMINATION_DT IS NULL then Worker_Travel_ID else null end as Worker_Travel_ID, HIRE_DT, fileid
  from LANDING.WD_WORKER_DAILY_HISTORY
  where (case when TERMINATION_DT IS NULL then Worker_Travel_ID else null end) IS NOT NULL
  order by 1
  ), part1 as (
  select 
    Worker_Travel_ID as TRAVEL_ID,
    struct(Worker_Travel_ID, HIRE_DT) as TRAVEL_IDS,
    fileid
  from base
  )
  select
    TRAVEL_ID,
    fileid,
    array_agg(TRAVEL_IDS) AS TRAVEL_IDS
  from part1
  group by 
    TRAVEL_ID,
    fileid 