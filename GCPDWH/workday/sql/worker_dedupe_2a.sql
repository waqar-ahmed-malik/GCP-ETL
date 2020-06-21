SELECT
    WD_ID,
    empid, 
    row_hash,
    hire_dt,
    array_agg(row_data ORDER BY file_date) AS emp_hist
FROM LANDING.WD_WORKER_DEDUPE_1a
GROUP BY 1,2,3,4;