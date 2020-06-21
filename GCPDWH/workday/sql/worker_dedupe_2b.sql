SELECT
    WD_ID,
    empid, 
    CAST(1 AS int64) AS row_hash,
    hire_dt,
    array_agg(row_data ORDER BY file_date) AS emp_hist
FROM LANDING.WD_WORKER_DEDUPE_1b
GROUP BY WD_ID,empid,hire_dt;