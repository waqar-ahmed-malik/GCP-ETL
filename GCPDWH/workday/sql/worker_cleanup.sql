with base as (
select
  WD_ID,	
  Empid,  
  STRUCT(
  license_type,	
  resident_license_Ind,	
  license_num,	
  license_st as licence_st,	
  license_effective_dt,	
  license_expiration_dt
  ) AS LICENSE,
  company,	
  cost_center,	
  email_address,	
  job_code,	
  language_type_cd,	
  location,	
  Legal_First_Name,	
  Legal_Middle_Name,	
  Legal_Last_Name,	
  Legal_Social_Suffix,	
  Preferred_First_Name,	
  Preferred_Middle_Name,	
  Preferred_Last_Name,	
  Preferred_Suffix,	
  Worker_P_C_3_Digit,	
  Worker_ENT_ID,	
  Worker_Life_ID,	
  Worker_P_C_10_Digit,	
  Worker_Travel_ID,	
  case when REGEXP_CONTAINS(change_effective_dt,r'(\d{2,4}\-\d{2}\-\d{2})') then PARSE_DATE('%Y-%m-%d', change_effective_dt) else null end as change_effective_dt,	
  phone_num,	
  phone_extsn,	
  STRUCT(
  PRFS_DESIG_TYPE_CD,	
  PRFS_DESIG_EFFECTIVE_DT,	
  PRFS_DESIG_EXPIRATION_DT
  ) AS PRFS_DESIG,
  SUPERVISOR_ID,	
  HIRE_DT,	
  TERMINATION_DT,	
  ORGANIZATION_ID,
  CAST('{{ ds_nodash }}' as INT64) as fileid,
  -- future fields
  EMPLOYEE_STATUS,	
  WORKER_TYPE,
  SECONDARY_EMAIL,	
  WORKER_AVAYA_ID
from LANDING.WD_WORKER_RAW_{{ ds_nodash }}
order by Empid , WD_ID 
), license_cleanup as (
  select 
    WD_ID,
    Empid,
    ARRAY_AGG(LICENSE) as licenses
   from base
   where LICENSE.LICENSE_TYPE IS NOT NULL
   group by
    WD_ID,
    Empid  
), PRFS_DESIG_cleanup as (
  select 
    WD_ID,
    Empid,
    ARRAY_AGG(PRFS_DESIG) as PRFS_DESIGS
   from base
   WHERE PRFS_DESIG.PRFS_DESIG_TYPE_CD IS NOT NULL
   group by
    WD_ID,
    Empid  
), contingent_conversion_cleanup_1 AS (
select DISTINCT
  WD_ID,	
  Empid,  
  company,	
  cost_center,	
  email_address,	
  job_code,	
  language_type_cd,	
  location,	
  Legal_First_Name,	
  Legal_Middle_Name,	
  Legal_Last_Name,	
  Legal_Social_Suffix,	
  Preferred_First_Name,	
  Preferred_Middle_Name,	
  Preferred_Last_Name,	
  Preferred_Suffix,	
  Worker_P_C_3_Digit,	
  Worker_ENT_ID,	
  Worker_Life_ID,	
  Worker_P_C_10_Digit,	
  Worker_Travel_ID,	
  change_effective_dt,	
  phone_num,	
  phone_extsn,	
  SUPERVISOR_ID,	
  HIRE_DT,	
  TERMINATION_DT,	
  ORGANIZATION_ID,
  fileid,
  EMPLOYEE_STATUS,	
  WORKER_TYPE,	
  SECONDARY_EMAIL,	
  WORKER_AVAYA_ID
from base
order by 1
), contingent_conversion_cleanup_2 as (
select
  Empid,
  array_agg(struct(
  WD_ID,	
  company,	
  cost_center,	
  email_address,	
  job_code,	
  language_type_cd,	
  location,	
  Legal_First_Name,	
  Legal_Middle_Name,	
  Legal_Last_Name,	
  Legal_Social_Suffix,	
  Preferred_First_Name,	
  Preferred_Middle_Name,	
  Preferred_Last_Name,	
  Preferred_Suffix,	
  Worker_P_C_3_Digit,	
  Worker_ENT_ID,	
  Worker_Life_ID,	
  Worker_P_C_10_Digit,	
  Worker_Travel_ID,	
  change_effective_dt,	
  phone_num,	
  phone_extsn,	
  SUPERVISOR_ID,	
  HIRE_DT,	
  TERMINATION_DT,	
  ORGANIZATION_ID,
  fileid,
  EMPLOYEE_STATUS,	
  WORKER_TYPE,	
  SECONDARY_EMAIL,	
  WORKER_AVAYA_ID
  ) ORDER BY change_effective_dt DESC) as employee_data
 from contingent_conversion_cleanup_1
 group by 1
 ), contingent_conversion_cleanup_final as (
  SELECT 
    Empid,
    (SELECT AS STRUCT
      WD_ID,	 
      company,	
      cost_center,	
      email_address,	
      job_code,	
      language_type_cd,	
      location,	
      Legal_First_Name,	
      Legal_Middle_Name,	
      Legal_Last_Name,	
      Legal_Social_Suffix,	
      Preferred_First_Name,	
      Preferred_Middle_Name,	
      Preferred_Last_Name,	
      Preferred_Suffix,	
      Worker_P_C_3_Digit,	
      Worker_ENT_ID,	
      Worker_Life_ID,	
      Worker_P_C_10_Digit,	
      Worker_Travel_ID,	
      change_effective_dt,	
      phone_num,	
      phone_extsn,	
      SUPERVISOR_ID,	
      HIRE_DT,	
      TERMINATION_DT,	
      ORGANIZATION_ID,
      fileid,
      EMPLOYEE_STATUS,	
      WORKER_TYPE,	
      SECONDARY_EMAIL,	
      WORKER_AVAYA_ID
     FROM UNNEST(employee_data) 
     ORDER BY change_effective_dt DESC
     LIMIT 1).*
  FROM contingent_conversion_cleanup_2
  )
  select 
    aa.*,
    bb.licenses,
    cc.PRFS_DESIGS
  from contingent_conversion_cleanup_final aa
  left join license_cleanup bb on (aa.Empid = bb.Empid AND aa.WD_ID = bb.WD_ID)
  left join PRFS_DESIG_cleanup cc on (aa.Empid = cc.Empid AND aa.WD_ID = cc.WD_ID);