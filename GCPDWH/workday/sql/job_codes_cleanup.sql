SELECT
  JOB_CD,
  cast('{{ ds_nodash }}' as int64) as fileid,
  JOB_CD_DESC,
  JOB_CD_EFFECTIVE_DT
FROM LANDING.WD_JOB_CODES_RAW_{{ ds_nodash }}
WHERE 1=1
ORDER BY 2, 1 DESC;