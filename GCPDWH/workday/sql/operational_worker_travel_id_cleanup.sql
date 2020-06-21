SELECT
  aa.Empid,
  aa.WD_ID,
  aa.fileid,
  CASE WHEN aa.TERMINATION_DT IS NULL AND (aa.HIRE_DT = bb.b) THEN aa.Worker_Travel_ID 
  WHEN aa.TERMINATION_DT IS NOT NULL THEN NULL
  ELSE NULL END AS Worker_Travel_ID,
  aa.HIRE_DT,
  aa.TERMINATION_DT
FROM LANDING.WD_WORKER_DAILY_HISTORY aa
LEFT JOIN (
    SELECT 
      fileid as c, 
    (SELECT AS STRUCT 
      Worker_Travel_ID as a,
      HIRE_DT as b
     FROM UNNEST(TRAVEL_IDS) ORDER BY HIRE_DT DESC LIMIT 1).*
    FROM OPERATIONAL.TRAVEL_ID_part1
) bb ON (aa.Worker_Travel_ID = bb.a and aa.fileid = bb.c)
--order by 3,1