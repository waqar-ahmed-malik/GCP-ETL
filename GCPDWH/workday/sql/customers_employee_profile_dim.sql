WITH base AS (
  SELECT 
    WD_ID,
    empid AS EMPLOYEE_ID,
    file_date,
    fileid AS FILE_ID,
    ROW_NUMBER() OVER (PARTITION BY empid ORDER BY file_date DESC) AS rn,
    ROW_NUMBER() OVER (PARTITION BY empid ORDER BY file_date ASC) AS VERSION,
    licenses AS LICENSES,
    company AS COMPANY,
    cost_center AS COST_CENTER_CD,
    email_address AS EMAIL_ADDRESS,
    job_code AS JOB_CD,
    language_type_cd AS LANGUAGE_TYPE_CD,
    location AS LOCATION_ID,
    Legal_First_Name AS LEGAL_FIRST_NM,
    Legal_Middle_Name AS LEGAL_MIDDLE_NM,
    Legal_Last_Name AS LEGAL_LAST_NM,
    Legal_Social_Suffix AS LEGAL_SUFFIX,
    Preferred_First_Name AS PREFERRED_FIRST_NM,
    Preferred_Middle_Name,
    Preferred_Last_Name AS PREFERRED_LAST_NM,
    Preferred_Suffix AS PREFERRED_SUFFIX,
    Worker_P_C_3_Digit AS INSURANCE_REP_ID,
    Worker_ENT_ID AS ENT_ID,
    Worker_Life_ID AS LIFE_ID,
    Worker_P_C_10_Digit AS PAS_ID,
    Worker_Travel_ID AS TRAVEL_ID,
    SAFE_CAST(CHANGE_EFFECTIVE_DT AS DATE) AS CHANGE_EFFECTIVE_DT,
    phone_num AS PHONE_NUM,
    phone_extsn AS PHONE_EXTENSION,
    PRFS_DESIGS, 
    SUPERVISOR_ID,
    SAFE_CAST(HIRE_DT AS DATE) AS HIRE_DT,
    SAFE_CAST(TERMINATION_DT AS DATE) AS TERMINATION_DT,
    ORGANIZATION_ID AS ORGANIZATION_CD,
    EMPLOYEE_STATUS,
    WORKER_TYPE,
    SECONDARY_EMAIL AS SECONDARY_EMAIL_ADDRESS,
    WORKER_AVAYA_ID,
    row_hash AS MD5_VALUE
  FROM LANDING.WD_WORKER
), orgs AS (
  SELECT 
    ORG_CODE,
    manager_emp_id,
    file_date AS start_date,
    COALESCE(DATE_SUB(LEAD(file_date) OVER (PARTITION BY ORG_CODE ORDER BY file_date), INTERVAL 1 DAY),'9999-12-31') AS end_date,
    MANAGER,
    CASE 
      WHEN LEVEL_10 IS NOT NULL THEN '10'
      WHEN LEVEL_09 IS NOT NULL THEN '9' 
      WHEN LEVEL_08 IS NOT NULL THEN '8'
      WHEN LEVEL_07 IS NOT NULL THEN '7'
      WHEN LEVEL_06 IS NOT NULL THEN '6'
      WHEN LEVEL_05 IS NOT NULL THEN '5'
      WHEN LEVEL_04 IS NOT NULL THEN '4'
      WHEN LEVEL_03 IS NOT NULL THEN '3'
      WHEN LEVEL_02 IS NOT NULL THEN '2'
      WHEN LEVEL_01 IS NOT NULL THEN '1'
      ELSE '0'
    END AS SUPERVISOR_HIERARCHY_LEVEL,
    LEVEL_01,
    LEVEL_02,
    LEVEL_03,
    LEVEL_04,
    LEVEL_05,
    LEVEL_06,
    LEVEL_07,
    LEVEL_08,
    LEVEL_09,
    LEVEL_10
  FROM LANDING.WD_SUPERVISORY_HIERARCHY
), manager_lookup AS (
  SELECT distinct 
    WD_ID, 
    empid AS SUPERVISOR_EMPLOYEE_ID 
  FROM LANDING.WD_WORKER
), costcenter AS (
  SELECT 
    Cost_Center_CD,
    Cost_Center_Desc,
    file_date as start_date,
    COALESCE(DATE_SUB(LEAD(file_date) OVER (PARTITION BY Cost_Center_CD ORDER BY file_date), INTERVAL 1 DAY),'9999-12-31') AS end_date
  FROM LANDING.WD_COSTCENTER
), job_code AS (
  SELECT
    JOB_CD,
    JOB_CD_DESC,
    SAFE_CAST(JOB_CD_EFFECTIVE_DT AS DATE) AS JOB_CD_EFFECTIVE_DT,
    file_date AS start_date,
    COALESCE(DATE_SUB(LEAD(file_date) OVER (PARTITION BY JOB_CD ORDER BY file_date), INTERVAL 1 DAY),'9999-12-31') AS end_date
  FROM LANDING.WD_JOB_CODES
), location AS (
  SELECT 
    WD_LOCATION_ID,
    LOCATION_NAME,
    file_date as start_date,
    COALESCE(DATE_SUB(LEAD(file_date) OVER (PARTITION BY WD_LOCATION_ID ORDER BY file_date), INTERVAL 1 DAY),'9999-12-31') AS end_date  
  FROM LANDING.WD_LOCATION
), market AS (
  SELECT 
    Branch_Number,
    Business_Site,
    Market_Code,
    Market,
    District_Code,
    District,
    file_date AS start_date,
    COALESCE(DATE_SUB(LEAD(file_date) OVER (PARTITION BY Branch_Number ORDER BY file_date), INTERVAL 1 DAY),'9999-12-31') AS end_date
  FROM LANDING.WD_MARKET_HIERARCHY
), sam AS (
  SELECT 
    aa.EMPLOYEE_ID, 
    UPPER(aa.ROLE_ID) AS SAM_ROLE_ID, 
    aa.LOGIN_ENABLED AS SAM_LOGIN_STATUS,
    bb.NAME AS SAM_ROLE_DESC
  FROM LANDING.SAM_EMPLOYEE_DIM aa
  LEFT JOIN LANDING.SAM_ROLES bb ON (UPPER(aa.ROLE_ID) = bb.ROLE_ID)
), agent AS (
  SELECT
    EMPLOYEE_ID,
    ARRAY_AGG(
    STRUCT(SAGT_KY AS SALES_AGENT_KEY,
    CASE WHEN INACTIVE_IND = 'Y' THEN 'INACTIVE' ELSE 'ACTIVE' END AS SALES_AGENT_STATUS)
    ) AS AGENT
  FROM REFERENCE.CONNECTSUITE_M_SALES_AGENT
  GROUP BY EMPLOYEE_ID
)
SELECT 
  aa.EMPLOYEE_ID, 
  aa.ENT_ID,
  aa.LEGAL_FIRST_NM,
  aa.LEGAL_MIDDLE_NM,
  aa.LEGAL_LAST_NM,
  aa.LEGAL_SUFFIX,
  aa.HIRE_DT,
  aa.TERMINATION_DT,
  aa.EMPLOYEE_STATUS,
  aa.WORKER_TYPE,  
  aa.LOCATION_ID,
  aa.EMAIL_ADDRESS,
  aa.SECONDARY_EMAIL_ADDRESS,
  aa.PHONE_NUM,
  aa.PHONE_EXTENSION,
  CASE WHEN aa.TERMINATION_DT IS NOT NULL THEN aa.TERMINATION_DT ELSE aa.file_date END AS ROW_START_DT,
  CASE WHEN aa.TERMINATION_DT IS NOT NULL THEN aa.TERMINATION_DT ELSE COALESCE(DATE_SUB(LEAD(aa.file_date) OVER (PARTITION BY aa.EMPLOYEE_ID ORDER BY aa.file_date), INTERVAL 1 DAY),'9999-12-31') END AS ROW_END_DT,
  CASE WHEN aa.rn = 1 THEN 'Y' ELSE 'N' end AS ACTIVE_FLG,  
  aa.FILE_ID AS ETL_JOB_RUN_ID,
  CAST('WORKDAY' AS String) AS SOURCE_SYSTEM_CD,
  CURRENT_TIMESTAMP() AS CREATE_DTTIME,
  CAST('create_employee_dim' AS String) AS CREATED_BY,
  aa.MD5_VALUE,
  aa.FILE_ID
FROM base aa
LEFT JOIN orgs bb ON (aa.ORGANIZATION_CD = bb.ORG_CODE AND (aa.file_date >= bb.start_date AND aa.file_date <= bb.end_date))
LEFT JOIN manager_lookup cc ON (aa.SUPERVISOR_ID = cc.WD_ID)
LEFT JOIN costcenter dd ON (aa.COST_CENTER_CD = dd.Cost_Center_CD AND (aa.file_date >= dd.start_date AND aa.file_date <= dd.end_date))
LEFT JOIN job_code ee ON (aa.JOB_CD = ee.JOB_CD AND (aa.file_date >= ee.start_date AND aa.file_date <= ee.end_date))
LEFT JOIN location ff ON (aa.LOCATION_ID = ff.WD_LOCATION_ID AND (aa.file_date >= ff.start_date AND aa.file_date <= ff.end_date))
LEFT JOIN market gg ON (aa.LOCATION_ID = gg.Branch_Number AND (aa.file_date >= gg.start_date AND aa.file_date <= gg.end_date))
LEFT JOIN sam hh ON (aa.EMPLOYEE_ID = hh.EMPLOYEE_ID)
LEFT JOIN agent ii ON (aa.EMPLOYEE_ID = ii.EMPLOYEE_ID)