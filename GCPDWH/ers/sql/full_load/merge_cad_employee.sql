SELECT
EMPLE_ID,
CAD_EMPLE_TYP_CD,
EMPLE_FST_NM,
EMPLE_FUNC_CD,
EMPLE_LST_NM,
EMPLE_MODE,
EMPLE_PSSWD,
EMPLE_STATUS,
CAST(CR_NUM_CALLS AS INT64) AS CR_NUM_CALLS,
CAST(CR_NUM_SECONDS AS INT64) AS CR_NUM_SECONDS,
CAST(CR_MAX_SECONDS AS INT64) AS CR_MAX_SECONDS,
CAST(CR_MIN_SECONDS AS INT64) AS CR_MIN_SECONDS,
CAST(LAST_CHG_DATE AS DATE) AS LAST_CHG_DATE,
MOTD_READ,
RESET_DI_PROF,
CAST(EMP_PSWD_CHG_DT AS DATE) AS EMP_PSWD_CHG_DT,
CAST(EMP_PSWD_TRIES AS INT64) AS EMP_PSWD_TRIES,
EMPLE_GROUP,
EMAIL,
FACILITY,
SPMG,
AAR,
LDAP_EMPLE_ID,
LST_USED_LOC_CD,
LST_USED_ROLE_CD,
CAST(LAST_UPDT_TS AS DATETIME) AS LAST_UPDT_TS,
DI_QUEUE_PROMPT,
SMS_NUMBER,
CAST(SRC.JOB_RUN_ID AS INT64) AS ETL_JOB_RUN_ID,
SRC.SOURCE_SYSTEM_CD,
SRC.CREATED_BY,
current_datetime() AS CREATE_DTTIME
FROM LANDING.WORK_ERS_STG_CAD_EMPLOYEE SRC
