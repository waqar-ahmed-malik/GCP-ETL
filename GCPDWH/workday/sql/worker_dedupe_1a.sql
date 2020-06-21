SELECT
    aa.WD_ID,
    aa.Empid,
    cc.hire_dt_start AS HIRE_DT,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(aa.fileid AS String),r'(\d{2}\d{2}\d{2,4})')) as file_date,
  STRUCT(
    aa.WD_ID,
    aa.Empid,
    PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(aa.fileid AS String),r'(\d{2}\d{2}\d{2,4})')) as file_date, 
    aa.licenses,
    aa.company,
    aa.cost_center,
    email_address,
    UPPER(aa.job_code) AS job_code,
    aa.language_type_cd,
    aa.location,
    aa.Legal_First_Name,
    aa.Legal_Middle_Name,
    aa.Legal_Last_Name,
    aa.Legal_Social_Suffix,
    aa.Preferred_First_Name,
    aa.Preferred_Middle_Name,
    aa.Preferred_Last_Name,
    aa.Preferred_Suffix,
    bb.Worker_P_C_3_Digit,
    aa.Worker_ENT_ID,
    aa.Worker_Life_ID,
    aa.Worker_P_C_10_Digit,
    ee.Worker_Travel_ID,
    aa.change_effective_dt,
    aa.phone_num,
    aa.phone_extsn,
    aa.PRFS_DESIGS,
    aa.SUPERVISOR_ID,
    dd.termination_start AS TERMINATION_DT,
    aa.ORGANIZATION_ID,
    aa.fileid,
    CASE WHEN aa.EMPLOYEE_STATUS = 'Terminated' THEN 'Active Employee' ELSE aa.EMPLOYEE_STATUS END AS EMPLOYEE_STATUS,
    --COALESCE(dd.EMPLOYEE_STATUS,aa.EMPLOYEE_STATUS) AS EMPLOYEE_STATUS,
    aa.WORKER_TYPE,
    aa.SECONDARY_EMAIL,
    aa.WORKER_AVAYA_ID
   ) as row_data,
  FARM_FINGERPRINT(
  CONCAT(
    COALESCE(CAST(aa.WD_ID AS STRING),''),
    COALESCE(CAST(ARRAY_LENGTH(aa.licenses) AS STRING),''),
    COALESCE(CAST(aa.company AS STRING),''),
    COALESCE(CAST(aa.cost_center AS STRING),''),
    COALESCE(CAST(aa.email_address AS STRING),''),
    COALESCE(CAST(UPPER(aa.job_code) AS STRING),''),
    COALESCE(CAST(aa.language_type_cd AS STRING),''),
    COALESCE(CAST(aa.location AS STRING),''),
    COALESCE(CAST(aa.Legal_First_Name AS STRING),''),
    COALESCE(CAST(aa.Legal_Middle_Name AS STRING),''),
    COALESCE(CAST(aa.Legal_Last_Name AS STRING),''),
    COALESCE(CAST(aa.Legal_Social_Suffix AS STRING),''),
    COALESCE(CAST(aa.Preferred_First_Name AS STRING),''),
    COALESCE(CAST(aa.Preferred_Middle_Name AS STRING),''),
    COALESCE(CAST(aa.Preferred_Last_Name AS STRING),''),
    COALESCE(CAST(aa.Preferred_Suffix AS STRING),''),
    COALESCE(CAST(bb.Worker_P_C_3_Digit AS STRING),''),
    COALESCE(CAST(aa.Worker_ENT_ID AS STRING),''),
    COALESCE(CAST(aa.Worker_Life_ID AS STRING),''),
    COALESCE(CAST(aa.Worker_P_C_10_Digit AS STRING),''),
    COALESCE(CAST(ee.Worker_Travel_ID AS STRING),''),
    COALESCE(CAST(aa.Empid AS STRING),''),
    COALESCE(CAST(aa.phone_num AS STRING),''),
    COALESCE(CAST(aa.phone_extsn AS STRING),''),
    COALESCE(CAST(ARRAY_LENGTH(aa.PRFS_DESIGS) AS STRING),''),
    COALESCE(CAST(aa.SUPERVISOR_ID AS STRING),''),
    COALESCE(CAST(cc.hire_dt_start AS STRING),''),
    COALESCE(CAST(dd.termination_start AS STRING),''),
    COALESCE(CAST(aa.ORGANIZATION_ID AS STRING),''),
    COALESCE(CAST(CASE WHEN aa.EMPLOYEE_STATUS = 'Terminated' THEN 'Active Employee' ELSE aa.EMPLOYEE_STATUS END AS STRING),''),
    --COALESCE(CAST(COALESCE(dd.EMPLOYEE_STATUS,aa.EMPLOYEE_STATUS) AS STRING),''),
    COALESCE(CAST(aa.WORKER_TYPE AS STRING),''),
    COALESCE(CAST(aa.SECONDARY_EMAIL AS STRING),''),
    COALESCE(CAST(aa.WORKER_AVAYA_ID AS STRING),'')
    )
   ) as row_hash
FROM LANDING.WD_WORKER_DAILY_HISTORY aa
LEFT JOIN OPERATIONAL.Worker_P_C_3_Digit_cleanup bb ON (
    aa.WD_ID = bb.WD_ID AND
    aa.Empid = bb.Empid AND
    aa.fileid = bb.fileid)
LEFT JOIN OPERATIONAL.Worker_effective_hire_date_banding cc ON (
	aa.Empid = cc.Empid 
AND PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(aa.fileid AS String),r'(\d{2}\d{2}\d{2,4})')) >= cc.hire_dt_start AND PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(aa.fileid AS String),r'(\d{2}\d{2}\d{2,4})')) <= cc.hire_dt_end
)
LEFT JOIN OPERATIONAL.Worker_effective_termination_date_banding dd ON 
    aa.Empid = dd.Empid 
AND PARSE_DATE('%Y-%m-%d',REGEXP_EXTRACT(CAST(aa.HIRE_DT AS String),r'(\d{4}-\d{2}-\d{2})')) = cc.hire_dt_start
AND PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(aa.fileid AS String),r'(\d{2}\d{2}\d{2,4})')) >= dd.termination_start 
AND PARSE_DATE('%Y%m%d',REGEXP_EXTRACT(CAST(aa.fileid AS String),r'(\d{2}\d{2}\d{2,4})')) <= dd.termination_end 
LEFT JOIN OPERATIONAL.Worker_Travel_ID_cleanup ee ON (
    aa.WD_ID = ee.WD_ID AND
    aa.Empid = ee.Empid AND
    aa.fileid = ee.fileid
)
WHERE dd.termination_start IS NULL