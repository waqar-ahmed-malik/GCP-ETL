SELECT TO_CHAR(SERVICE_FACILITY.FACIL_INT_ID) FACIL_INT_ID,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_ID) SVC_FACL_ID, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_BSC_AD) SVC_FACL_BSC_AD, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_CTY_NM) SVC_FACL_CTY_NM, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_D_S_E_TM) SVC_FACL_D_S_E_TM, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_NM) SVC_FACL_NM,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_ST_CD) SVC_FACL_ST_CD, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_STS_CD) SVC_FACL_STS_CD, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_SUPL_AD) SVC_FACL_SUPL_AD, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_ZIP_CD) SVC_FACL_ZIP_CD, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_FUNC_CD) SVC_FACL_FUNC_CD,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_REP_ID) SVC_FACL_REP_ID, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_FED_ID) SVC_FACL_FED_ID, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_ID_FLAG) SVC_FACL_ID_FLAG,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_DIR_DEP) SVC_FACL_DIR_DEP, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_START_DT,'YYYY-MM-DD HH24:MI:SS') AS SVC_FACL_START_DT,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_ROUTE_NR) SVC_FACL_ROUTE_NR,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_ACCT_NR) SVC_FACL_ACCT_NR, 
TO_CHAR (SERVICE_FACILITY.SVC_FACL_INACT_DT,'YYYY-MM-DD HH24:MI:SS') AS SVC_FACL_INACT_DT,
TO_CHAR (SERVICE_FACILITY.SVC_FACL_INS_EXP,'YYYY-MM-DD HH24:MI:SS') AS SVC_FACL_INS_EXP,
TO_CHAR(SERVICE_FACILITY.DONT_PRICE) DONT_PRICE, 
TO_CHAR(SERVICE_FACILITY.COMM_CTR_ID) COMM_CTR_ID,
TO_CHAR(SERVICE_FACILITY.TAX_FLAG) TAX_FLAG,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_BONUS_AMT) SVC_FACL_BONUS_AMT, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_REC_BON) SVC_FACL_REC_BON, 
TO_CHAR (SERVICE_FACILITY.SVC_FACL_FED_START,'YYYY-MM-DD HH24:MI:SS') AS SVC_FACL_FED_START,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_CONTACT) SVC_FACL_CONTACT, 
TO_CHAR(SERVICE_FACILITY.SUSPEND_STMT_FLAG) SUSPEND_STMT_FLAG,
TO_CHAR(SERVICE_FACILITY.AUTO_SUSPEND_FLAG) AUTO_SUSPEND_FLAG, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_PST_ID) SVC_FACL_PST_ID,
TO_CHAR (SERVICE_FACILITY.SVC_FACL_PST_START,'YYYY-MM-DD HH24:MI:SS') AS SVC_FACL_PST_START,
TO_CHAR(SERVICE_FACILITY.DAILY_BALANCING) DAILY_BALANCING, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_TYPE) SVC_FACL_TYPE, 
TO_CHAR(SERVICE_FACILITY.SVC_FACL_TYPE2) SVC_FACL_TYPE2,
TO_CHAR(SERVICE_FACILITY.SVC_FACL_EMAIL) SVC_FACL_EMAIL, 
TO_CHAR(SERVICE_FACILITY.ELECTRONIC_STMT_FLAG) ELECTRONIC_STMT_FLAG, 
TO_CHAR(SERVICE_FACILITY.CLUB_CONTRACT_FLAG) CLUB_CONTRACT_FLAG, 
TO_CHAR(SERVICE_FACILITY.CALC_TOW_MILE_FLAG) CALC_TOW_MILE_FLAG, 
TO_CHAR(SERVICE_FACILITY.CALC_ENROUTE_MILE_FLAG) CALC_ENROUTE_MILE_FLAG,
TO_CHAR(SERVICE_FACILITY.TRUCK_ER_FLAG) TRUCK_ER_FLAG
,'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM
 NCA_ERS_HIST.SERVICE_FACILITY SERVICE_FACILITY