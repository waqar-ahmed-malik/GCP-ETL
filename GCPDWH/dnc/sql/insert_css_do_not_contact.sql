INSERT INTO `LANDING.CSS_DO_NOT_CONTACT` 
(
MEMBER_NUM,
PHONE_NUM,
CAMPAIGN_ID,
TREATMENT_CD,
STATUS,
ETL_JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DT,
CREATE_BY
)
SELECT case
when SAFE_CAST(MEMBER_NUM AS INT64) IS NOT NULL
THEN
MEMBER_NUM
else ' '
end AS MEMBER_NUM,
regexp_replace(PHONE_NUM,'[^0-9]','') AS PHONE_NUM,
CAMPAIGN_ID,
TREATMENT_CD,
STATUS,
"v_job_run_id" AS ETL_JOB_RUN_ID,
"CSS" AS SOURCE_SYSTEM_CD,
CURRENT_DATE() AS CREATE_DT,
"v_job_name" AS CREATE_BY
FROM LANDING.WORK_CSS_DO_NOT_CONTACT