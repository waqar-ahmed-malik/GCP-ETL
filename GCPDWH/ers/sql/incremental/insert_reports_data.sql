SELECT 
TO_CHAR(REPORTS_DATA.COMM_CTR_ID)  COMM_CTR_ID, 
TO_CHAR (REPORTS_DATA.SC_DT, 'YYYY-MM-DD HH24:MI:SS')  SC_DT,
TO_CHAR(REPORTS_DATA.SC_ID) SC_ID, 
TO_CHAR (REPORTS_DATA.ARCH_DATE,'YYYY-MM-DD HH24:MI:SS') ARCH_DATE,
TO_CHAR(REPORTS_DATA.CALL_MBR_ID) CALL_MBR_ID, 
TO_CHAR(REPORTS_DATA.CALL_SOURCE) CALL_SOURCE, 
TO_CHAR(REPORTS_DATA.RE_TM,'YYYY-MM-DD HH24:MI:SS') RE_TM,
TO_CHAR(REPORTS_DATA.RE_EMPLE_ID)  RE_EMPLE_ID, 
TO_CHAR (REPORTS_DATA.FST_SP_TM,'YYYY-MM-DD HH24:MI:SS')  FST_SP_TM,
TO_CHAR(REPORTS_DATA.FST_SP_EMPLE_ID) FST_SP_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.FST_SP_STS_RSN_CD) FST_SP_STS_RSN_CD, 
TO_CHAR (REPORTS_DATA.FST_AS_TM,'YYYY-MM-DD HH24:MI:SS')  FST_AS_TM,
TO_CHAR(REPORTS_DATA.FST_AS_EMPLE_ID) FST_AS_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.FST_AS_STS_RSN_CD) FST_AS_STS_RSN_CD, 
TO_CHAR (REPORTS_DATA.FST_DI_TM,'YYYY-MM-DD HH24:MI:SS')  FST_DI_TM,
TO_CHAR(REPORTS_DATA.FST_DI_EMPLE_ID) FST_DI_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.FST_DI_STS_RSN_CD) FST_DI_STS_RSN_CD,
TO_CHAR (REPORTS_DATA.FST_ER_TM,'YYYY-MM-DD HH24:MI:SS')  FST_ER_TM,
TO_CHAR(REPORTS_DATA.FST_ER_EMPLE_ID) FST_ER_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.FST_ER_STS_RSN_CD) FST_ER_STS_RSN_CD, 
TO_CHAR (REPORTS_DATA.FST_OL_TM,'YYYY-MM-DD HH24:MI:SS') FST_OL_TM,
TO_CHAR(REPORTS_DATA.FST_OL_EMPLE_ID) FST_OL_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.FST_OL_STS_RSN_CD) FST_OL_STS_RSN_CD,
TO_CHAR (REPORTS_DATA.FST_TW_TM,'YYYY-MM-DD HH24:MI:SS') FST_TW_TM,
TO_CHAR(REPORTS_DATA.FST_TW_EMPLE_ID) FST_TW_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.FST_TW_STS_RSN_CD) FST_TW_STS_RSN_CD,
TO_CHAR (REPORTS_DATA.FST_CL_TM,'YYYY-MM-DD HH24:MI:SS') FST_CL_TM,
REPORTS_DATA.FST_CL_EMPLE_ID FST_CL_EMPLE_ID, 
REPORTS_DATA.FST_CL_STS_RSN_CD FST_CL_STS_RSN_CD,
TO_CHAR (REPORTS_DATA.LST_SP_TM,'YYYY-MM-DD HH24:MI:SS') LST_SP_TM,
TO_CHAR(REPORTS_DATA.LST_SP_EMPLE_ID) LST_SP_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.LST_SP_STS_RSN_CD) LST_SP_STS_RSN_CD,
TO_CHAR (REPORTS_DATA.LST_AS_TM,'YYYY-MM-DD HH24:MI:SS') LST_AS_TM,
TO_CHAR(REPORTS_DATA.LST_AS_EMPLE_ID) LST_AS_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.LST_AS_STS_RSN_CD) LST_AS_STS_RSN_CD,
TO_CHAR (REPORTS_DATA.LST_DI_TM,'YYYY-MM-DD HH24:MI:SS') LST_DI_TM,
TO_CHAR(REPORTS_DATA.LST_DI_EMPLE_ID) LST_DI_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.LST_DI_STS_RSN_CD)  LST_DI_STS_RSN_CD, 
TO_CHAR (REPORTS_DATA.LST_OL_TM, 'YYYY-MM-DD HH24:MI:SS') LST_OL_TM,
TO_CHAR(REPORTS_DATA.LST_OL_EMPLE_ID) LST_OL_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.LST_OL_STS_RSN_CD) LST_OL_STS_RSN_CD, 
TO_CHAR (REPORTS_DATA.UP1_TM,'YYYY-MM-DD HH24:MI:SS') UP1_TM,
TO_CHAR(REPORTS_DATA.UP1_STS_CD) UP1_STS_CD, 
TO_CHAR(REPORTS_DATA.UP1_STS_RSN_CD) UP1_STS_RSN_CD,
TO_CHAR(REPORTS_DATA.UP1_EMPLE_ID) UP1_EMPLE_ID, 
TO_CHAR (REPORTS_DATA.UP2_TM,'YYYY-MM-DD HH24:MI:SS') UP2_TM,
TO_CHAR(REPORTS_DATA.UP2_STS_CD) UP2_STS_CD, 
TO_CHAR(REPORTS_DATA.UP2_STS_RSN_CD) UP2_STS_RSN_CD, 
TO_CHAR(REPORTS_DATA.UP2_EMPLE_ID) UP2_EMPLE_ID, 
TO_CHAR (REPORTS_DATA.UP3_TM,'YYYY-MM-DD HH24:MI:SS') UP3_TM,
REPORTS_DATA.UP3_STS_CD UP3_STS_CD, 
REPORTS_DATA.UP3_STS_RSN_CD UP3_STS_RSN_CD, 
REPORTS_DATA.UP3_EMPLE_ID UP3_EMPLE_ID, 
TO_CHAR (REPORTS_DATA.UP4_TM,'YYYY-MM-DD HH24:MI:SS') UP4_TM,
TO_CHAR(REPORTS_DATA.UP4_STS_CD) UP4_STS_CD,
TO_CHAR(REPORTS_DATA.UP4_STS_RSN_CD) UP4_STS_RSN_CD, 
TO_CHAR(REPORTS_DATA.UP4_EMPLE_ID) UP4_EMPLE_ID, 
TO_CHAR (REPORTS_DATA.UP5_TM,'YYYY-MM-DD HH24:MI:SS') UP5_TM,
TO_CHAR(REPORTS_DATA.UP5_STS_CD) UP5_STS_CD, 
TO_CHAR(REPORTS_DATA.UP5_STS_RSN_CD) UP5_STS_RSN_CD, 
TO_CHAR(REPORTS_DATA.UP5_EMPLE_ID) UP5_EMPLE_ID, 
TO_CHAR(REPORTS_DATA.FIRST_FACL_ID) FIRST_FACL_ID, 
TO_CHAR(REPORTS_DATA.FIRST_FACIL_INT_ID) FIRST_FACIL_INT_ID, 
TO_CHAR(REPORTS_DATA.LAST_FACL_ID) LAST_FACL_ID, 
TO_CHAR(REPORTS_DATA.LAST_FACIL_INT_ID) LAST_FACIL_INT_ID, 
TO_CHAR(REPORTS_DATA.SC_A_SVC_TRK_ID) SC_A_SVC_TRK_ID, 
TO_CHAR(REPORTS_DATA.SC_A_SVC_TRK_D_ID) SC_A_SVC_TRK_D_ID, 
TO_CHAR (REPORTS_DATA.SC_PRMS_ARR_TM,'YYYY-MM-DD HH24:MI:SS') SC_PRMS_ARR_TM,
TO_CHAR(REPORTS_DATA.CALL_COST) CALL_COST, 
TO_CHAR(REPORTS_DATA.PROB1_CD) PROB1_CD, 
TO_CHAR(REPORTS_DATA.PROB2_CD) PROB2_CD, 
TO_CHAR(REPORTS_DATA.PLUS_IND) PLUS_IND, 
TO_CHAR(REPORTS_DATA.RAP_INDIC) RAP_INDIC, 
TO_CHAR(REPORTS_DATA.RAP_PROGRAM_ID) RAP_PROGRAM_ID, 
TO_CHAR(REPORTS_DATA.PTA_FST_OL_VAR) PTA_FST_OL_VAR,
TO_CHAR( REPORTS_DATA.REASGN_RSN_CD) REASGN_RSN_CD, 
TO_CHAR(REPORTS_DATA.LOST_FACL_ID) LOST_FACL_ID, 
TO_CHAR(REPORTS_DATA.CALLOUT_CNT_DISPATCHER) CALLOUT_CNT_DISPATCHER, 
TO_CHAR(REPORTS_DATA.CALLOUT_CNT_CALLOUTBOX) CALLOUT_CNT_CALLOUTBOX, 
TO_CHAR(REPORTS_DATA.CALLOUT_INITIAL_ETA_RESULT) CALLOUT_INITIAL_ETA_RESULT, 
TO_CHAR(REPORTS_DATA.CALLOUT_CONFIRM_RESULT) CALLOUT_CONFIRM_RESULT, 
TO_CHAR(REPORTS_DATA.CALLOUTBOX1_RESULT) CALLOUTBOX1_RESULT, 
TO_CHAR(REPORTS_DATA.CALLOUTBOX2_RESULT) CALLOUTBOX2_RESULT, 
TO_CHAR(REPORTS_DATA.CALLOUTBOX3_RESULT) CALLOUTBOX3_RESULT, 
TO_CHAR(REPORTS_DATA.LST_CL_STS_RSN_CD) LST_CL_STS_RSN_CD, 
TO_CHAR(REPORTS_DATA.LST_CL_EMPLE_ID) LST_CL_EMPLE_ID, 
TO_CHAR (REPORTS_DATA.LST_CL_TM,'YYYY-MM-DD HH24:MI:SS') LST_CL_TM,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM
NCA_ERS_HIST.REPORTS_DATA REPORTS_DATA
WHERE  ARCH_DATE >=  TO_DATE('incr_date_report','yyyy-mm-dd hh24:mi:ss')