SELECT
    WD_ID,
    empid, 
    row_hash,
    array_agg(row_data ORDER BY file_date) AS emp_hist
FROM LANDING.WD_WORKER_DEDUPE_1
GROUP BY 1,2,3;