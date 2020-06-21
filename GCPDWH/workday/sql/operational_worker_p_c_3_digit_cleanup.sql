SELECT
  aa.Empid,
  aa.WD_ID,
  aa.fileid,
  CASE WHEN aa.TERMINATION_DT IS NULL AND (aa.HIRE_DT = bb.b) THEN aa.Worker_P_C_3_Digit 
  WHEN aa.TERMINATION_DT IS NOT NULL THEN NULL
  ELSE NULL END AS Worker_P_C_3_Digit,
  aa.HIRE_DT,
  aa.TERMINATION_DT
FROM LANDING.WD_WORKER_DAILY_HISTORY aa
LEFT JOIN OPERATIONAL.P_C_3_part1
 bb ON (aa.Worker_P_C_3_Digit = bb.a and aa.fileid = bb.c)
--order by 3,1