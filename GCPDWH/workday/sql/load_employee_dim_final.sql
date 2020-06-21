CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.EMPLOYEE_DIM AS
(WITH TERMINATION_WINDOW
-- pull all employee records into a window function, sorted by effective start date. look for term date and copy that to each row if it exists.
AS
(
  SELECT EMPLOYEE_ID, ENT_ID, LEGAL_FIRST_NM, LEGAL_MIDDLE_NM, LEGAL_LAST_NM, LEGAL_SUFFIX, HIRE_DT, TERMINATION_DT, EMPLOYEE_STATUS, WORKER_TYPE, LOCATION_ID, EMAIL_ADDRESS, SECONDARY_EMAIL_ADDRESS, PHONE_NUM, PHONE_EXTENSION, LIFE_ID, PAS_ID, INSURANCE_REP_ID, TRAVEL_ID, COMPANY, CHANGE_EFFECTIVE_DT, LANGUAGE_TYPE_CD, PRFS_DESIG_EFFECTIVE_DT, PRFS_DESIG_EXPIRATION_DT, PRFS_DESIG_TYPE_CD, COST_CENTER_CD, RESIDENT_LICENSE_IND, LICENSE_NUM, LICENSE_STATE, LICENSE_EFFECTIVE_DT, LICENSE_EXPIRATION_DT, LICENSE_TYPE_CD, JOB_CD, JOB_CD_DESC, JOB_CD_EFFECTIVE_DT, SUPERVISOR_EMPLOYEE_ID, ORGANIZATION_CD, WORKER_AVAYA_ID, SUPERVISOR_HIERARCHY_LEVEL, SAM_ROLE_ID, SAM_ROLE_DESC, SAM_LOGIN_STATUS, SALES_AGENT_KEY, SALES_AGENT_STATUS, WD_ID, EFFECTIVE_START_DT, EFFECTIVE_END_DT, ACTIVE_FLG, ETL_JOB_RUN_ID, SOURCE_SYSTEM_CD, CREATE_DTTIME, CREATED_BY, MD5_VALUE,
  DATE_SUB(PARSE_DATE('%Y%m%d',FILE_ID), INTERVAL 1 DAY) FILE_ID_DATE, FILE_ID,
  FIRST_VALUE(TERMINATION_DT)
      OVER (PARTITION BY EMPLOYEE_ID ORDER BY EFFECTIVE_START_DT DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS TERMINATION_DT_WINDOW,
  ROW_NUMBER()
      OVER (PARTITION BY EMPLOYEE_ID ORDER BY EFFECTIVE_START_DT DESC) as ROWNUM
  FROM LANDING.EMPLOYEE_DIM_STAGE
),
  
-- only select records where the termination date is greater than or equal to the records file_id_date, which is a transaction date
TERMINATION_FILTERED
AS
(
  select * from TERMINATION_WINDOW
  where TERMINATION_DT_WINDOW >= FILE_ID_DATE
),
  
-- resort and renumber the employees after filtering out for records following term dates
TERMINATION_FILTERED_SORTED
AS
(
  SELECT *,
    ROW_NUMBER()
      OVER (PARTITION BY EMPLOYEE_ID ORDER BY EFFECTIVE_START_DT DESC) as ROWNUM_FILTERED
  FROM TERMINATION_FILTERED
),
  
-- join all sets together so it can be filtered and sorted
UNION_SETS
AS
(
  #if employee was NOT terminated, returns their entire rowset
  select EMPLOYEE_ID, ENT_ID, LEGAL_FIRST_NM, LEGAL_MIDDLE_NM, LEGAL_LAST_NM, LEGAL_SUFFIX, HIRE_DT, TERMINATION_DT, EMPLOYEE_STATUS, WORKER_TYPE, LOCATION_ID, EMAIL_ADDRESS, SECONDARY_EMAIL_ADDRESS, PHONE_NUM, PHONE_EXTENSION, LIFE_ID, PAS_ID, INSURANCE_REP_ID, TRAVEL_ID, COMPANY, CHANGE_EFFECTIVE_DT, LANGUAGE_TYPE_CD, PRFS_DESIG_EFFECTIVE_DT, PRFS_DESIG_EXPIRATION_DT, PRFS_DESIG_TYPE_CD, COST_CENTER_CD, RESIDENT_LICENSE_IND, LICENSE_NUM, LICENSE_STATE, LICENSE_EFFECTIVE_DT, LICENSE_EXPIRATION_DT, LICENSE_TYPE_CD, JOB_CD, JOB_CD_DESC, JOB_CD_EFFECTIVE_DT, SUPERVISOR_EMPLOYEE_ID, ORGANIZATION_CD, WORKER_AVAYA_ID, SUPERVISOR_HIERARCHY_LEVEL, SAM_ROLE_ID, SAM_ROLE_DESC, SAM_LOGIN_STATUS, SALES_AGENT_KEY, SALES_AGENT_STATUS, WD_ID, EFFECTIVE_START_DT, EFFECTIVE_END_DT, ACTIVE_FLG, ETL_JOB_RUN_ID, SOURCE_SYSTEM_CD, CREATE_DTTIME, CREATED_BY, MD5_VALUE, FILE_ID
  from TERMINATION_WINDOW
  where TERMINATION_DT_WINDOW is null
  union all
  
  # if employee was terminated, returns everything preceding their termination date record
  select EMPLOYEE_ID, ENT_ID, LEGAL_FIRST_NM, LEGAL_MIDDLE_NM, LEGAL_LAST_NM, LEGAL_SUFFIX, HIRE_DT, TERMINATION_DT, EMPLOYEE_STATUS, WORKER_TYPE, LOCATION_ID, EMAIL_ADDRESS, SECONDARY_EMAIL_ADDRESS, PHONE_NUM, PHONE_EXTENSION, LIFE_ID, PAS_ID, INSURANCE_REP_ID, TRAVEL_ID, COMPANY, CHANGE_EFFECTIVE_DT, LANGUAGE_TYPE_CD, PRFS_DESIG_EFFECTIVE_DT, PRFS_DESIG_EXPIRATION_DT, PRFS_DESIG_TYPE_CD, COST_CENTER_CD, RESIDENT_LICENSE_IND, LICENSE_NUM, LICENSE_STATE, LICENSE_EFFECTIVE_DT, LICENSE_EXPIRATION_DT, LICENSE_TYPE_CD, JOB_CD, JOB_CD_DESC, JOB_CD_EFFECTIVE_DT, SUPERVISOR_EMPLOYEE_ID, ORGANIZATION_CD, WORKER_AVAYA_ID, SUPERVISOR_HIERARCHY_LEVEL, SAM_ROLE_ID, SAM_ROLE_DESC, SAM_LOGIN_STATUS, SALES_AGENT_KEY, SALES_AGENT_STATUS, WD_ID, EFFECTIVE_START_DT, EFFECTIVE_END_DT, ACTIVE_FLG, ETL_JOB_RUN_ID, SOURCE_SYSTEM_CD, CREATE_DTTIME, CREATED_BY, MD5_VALUE, FILE_ID
  from TERMINATION_FILTERED_SORTED
  where ROWNUM_FILTERED > 1
  union all
  
  #if employee was terminated, returns their last record before termination day record
  select EMPLOYEE_ID, ENT_ID, LEGAL_FIRST_NM, LEGAL_MIDDLE_NM, LEGAL_LAST_NM, LEGAL_SUFFIX, HIRE_DT, TERMINATION_DT, EMPLOYEE_STATUS, WORKER_TYPE, LOCATION_ID, EMAIL_ADDRESS, SECONDARY_EMAIL_ADDRESS, PHONE_NUM, PHONE_EXTENSION, LIFE_ID, PAS_ID, INSURANCE_REP_ID, TRAVEL_ID, COMPANY, CHANGE_EFFECTIVE_DT, LANGUAGE_TYPE_CD, PRFS_DESIG_EFFECTIVE_DT, PRFS_DESIG_EXPIRATION_DT, PRFS_DESIG_TYPE_CD, COST_CENTER_CD, RESIDENT_LICENSE_IND, LICENSE_NUM, LICENSE_STATE, LICENSE_EFFECTIVE_DT, LICENSE_EXPIRATION_DT, LICENSE_TYPE_CD, JOB_CD, JOB_CD_DESC, JOB_CD_EFFECTIVE_DT, SUPERVISOR_EMPLOYEE_ID, ORGANIZATION_CD, WORKER_AVAYA_ID, SUPERVISOR_HIERARCHY_LEVEL, SAM_ROLE_ID, SAM_ROLE_DESC, SAM_LOGIN_STATUS, SALES_AGENT_KEY, SALES_AGENT_STATUS, WD_ID, EFFECTIVE_START_DT, DATE_SUB(TERMINATION_DT_WINDOW, INTERVAL 1 DAY) as EFFECTIVE_END_DT, ACTIVE_FLG, ETL_JOB_RUN_ID, SOURCE_SYSTEM_CD, CREATE_DTTIME, CREATED_BY, MD5_VALUE, FILE_ID
  from TERMINATION_FILTERED_SORTED
  where ROWNUM_FILTERED = 1 and FILE_ID_DATE <> TERMINATION_DT_WINDOW
  union all
  
  #if employee was terminated, hardcodes a termination day record that has the termination date as the termination_dt and effective start and end dates
  select EMPLOYEE_ID, ENT_ID, LEGAL_FIRST_NM, LEGAL_MIDDLE_NM, LEGAL_LAST_NM, LEGAL_SUFFIX, HIRE_DT, TERMINATION_DT_WINDOW as TERMINATION_DT, 'Terminated' as EMPLOYEE_STATUS, WORKER_TYPE, LOCATION_ID, EMAIL_ADDRESS, SECONDARY_EMAIL_ADDRESS, PHONE_NUM, PHONE_EXTENSION, LIFE_ID, PAS_ID, INSURANCE_REP_ID, TRAVEL_ID, COMPANY, CHANGE_EFFECTIVE_DT, LANGUAGE_TYPE_CD, PRFS_DESIG_EFFECTIVE_DT, PRFS_DESIG_EXPIRATION_DT, PRFS_DESIG_TYPE_CD, COST_CENTER_CD, RESIDENT_LICENSE_IND, LICENSE_NUM, LICENSE_STATE, LICENSE_EFFECTIVE_DT, LICENSE_EXPIRATION_DT, LICENSE_TYPE_CD, JOB_CD, JOB_CD_DESC, JOB_CD_EFFECTIVE_DT, SUPERVISOR_EMPLOYEE_ID, ORGANIZATION_CD, WORKER_AVAYA_ID, SUPERVISOR_HIERARCHY_LEVEL, SAM_ROLE_ID, SAM_ROLE_DESC, SAM_LOGIN_STATUS, SALES_AGENT_KEY, SALES_AGENT_STATUS, WD_ID, TERMINATION_DT_WINDOW as EFFECTIVE_START_DT, TERMINATION_DT_WINDOW as EFFECTIVE_END_DT, ACTIVE_FLG, ETL_JOB_RUN_ID, SOURCE_SYSTEM_CD, CREATE_DTTIME, CREATED_BY, MD5_VALUE, FILE_ID
  from TERMINATION_WINDOW where ROWNUM = 1 and TERMINATION_DT_WINDOW is not null
),
# Combine the results from the Union
UNION_RESULT
AS
(
select * from UNION_SETS
order by EMPLOYEE_ID, EFFECTIVE_START_DT)
# Strip the duplicate erroneous Termination date from the results leaving the proper, re-written termination date record
SELECT * FROM UNION_RESULT
WHERE EMPLOYEE_STATUS <> 'Terminated' OR EMPLOYEE_STATUS IS NULL OR (EMPLOYEE_STATUS = 'Terminated' AND EFFECTIVE_START_DT = EFFECTIVE_END_DT)
)
