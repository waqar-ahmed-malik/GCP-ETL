SELECT 
TO_CHAR(translate(ARCH_CALL.COMM_CTR_ID, chr(10)||chr(11)||chr(13),' ')) AS COMM_CTR_ID,
TO_CHAR(ARCH_CALL.SC_DT,'YYYY-MM-DD HH24:MI:SS') AS SC_DT,
TO_CHAR(translate(ARCH_CALL.SC_ID, chr(10)||chr(11)||chr(13),' ')) SC_ID, 
TO_CHAR(ARCH_CALL.ARCH_DATE,'YYYY-MM-DD HH24:MI:SS') AS ARCH_DATE,
TO_CHAR(translate(ARCH_CALL.SC_CALL_CLB_CD, chr(10)||chr(11)||chr(13),' ')) SC_CALL_CLB_CD, 
TO_CHAR(translate(ARCH_CALL.SC_CALL_MBR_ID, chr(10)||chr(11)||chr(13),' ')) SC_CALL_MBR_ID, 
TO_CHAR(translate(ARCH_CALL.SC_CALL_ASC_ID, chr(10)||chr(11)||chr(13),' ')) SC_CALL_ASC_ID,
TO_CHAR(translate(ARCH_CALL.SC_CSH_RQR_IN, chr(10)||chr(11)||chr(13),' ')) SC_CSH_RQR_IN, 
TO_CHAR(translate(ARCH_CALL.SC_CNTC_FST_NM, chr(10)||chr(11)||chr(13),' ')) SC_CNTC_FST_NM, 
TO_CHAR(translate(ARCH_CALL.SC_CNTC_LST_NM, chr(10)||chr(11)||chr(13),' ')) SC_CNTC_LST_NM, 
TO_CHAR(translate(ARCH_CALL.SC_CNTC_SUFFIX, chr(10)||chr(11)||chr(13),' ')) SC_CNTC_SUFFIX, 
TO_CHAR(translate(ARCH_CALL.SC_PRTY_CD, chr(10)||chr(11)||chr(13),' ')) SC_PRTY_CD, 
TO_CHAR(translate(ARCH_CALL.SC_WAIT_TM, chr(10)||chr(11)||chr(13),' ')) SC_WAIT_TM,
TO_CHAR(ARCH_CALL.SC_PRMS_ARR_TM,'YYYY-MM-DD HH24:MI:SS') AS SC_PRMS_ARR_TM,
TO_CHAR(translate(ARCH_CALL.SC_RED_FLAG_IN, chr(10)||chr(11)||chr(13),' ')) SC_RED_FLAG_IN, 
TO_CHAR(translate(ARCH_CALL.STATUS_CD, chr(10)||chr(11)||chr(13),' ')) STATUS_CD, 
TO_CHAR(translate(ARCH_CALL.SC_STS_RSN_CD, chr(10)||chr(11)||chr(13),' ')) SC_STS_RSN_CD, 
TO_CHAR(translate(ARCH_CALL.CB_FLAG, chr(10)||chr(11)||chr(13),' ')) CB_FLAG,
TO_CHAR(ARCH_CALL.SC_RCVD_TM, 'YYYY-MM-DD HH24:MI:SS') AS SC_RCVD_TM, 
TO_CHAR(translate(ARCH_CALL.HIST_FLAG , chr(10)||chr(11)||chr(13),' ')) HIST_FLAG, 
TO_CHAR(translate(ARCH_CALL.SC_POLICY, chr(10)||chr(11)||chr(13),' ')) SC_POLICY, 
TO_CHAR(translate(ARCH_CALL.SC_SRC_CD, chr(10)||chr(11)||chr(13),' ')) SC_SRC_CD, 
TO_CHAR(translate(ARCH_CALL.HOLD_FOR_CALLBK, chr(10)||chr(11)||chr(13),' '))HOLD_FOR_CALLBK, 
TO_CHAR(translate(ARCH_CALL.CB_MEMBER, chr(10)||chr(11)||chr(13),' ')) CB_MEMBER, 
TO_CHAR(translate(ARCH_CALL.CB_REQ_MIN, chr(10)||chr(11)||chr(13),' ')) CB_REQ_MIN, 
TO_CHAR(translate(ARCH_CALL.SC_VEH_LIC_ST_CD, chr(10)||chr(11)||chr(13),' ')) SC_VEH_LIC_ST_CD, 
TO_CHAR(translate(ARCH_CALL.SC_VEH_LIC_TAG_NR, chr(10)||chr(11)||chr(13),' ')) SC_VEH_LIC_TAG_NR, 
TO_CHAR(translate(ARCH_CALL.SC_VEH_MANF_YR_DT, chr(10)||chr(11)||chr(13),' ')) SC_VEH_MANF_YR_DT, 
TO_CHAR(translate(ARCH_CALL.SC_VEH_MANFR_NM, chr(10)||chr(11)||chr(13),' ')) SC_VEH_MANFR_NM, 
TO_CHAR(translate(ARCH_CALL.SC_VEH_MDL_NM, chr(10)||chr(11)||chr(13),' ')) SC_VEH_MDL_NM, 
TO_CHAR(translate(ARCH_CALL.SC_VEH_COLR_NM, chr(10)||chr(11)||chr(13),' ')) SC_VEH_COLR_NM, 
TO_CHAR(translate(ARCH_CALL.BL_NEAR_CTY_NM, chr(10)||chr(11)||chr(13),' ')) BL_NEAR_CTY_NM, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_N_LNDMRK_TX, chr(10)||chr(11)||chr(13),' ')) BL_N_LNDMRK_TX, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_OOT_IN, chr(10)||chr(11)||chr(13),' ')) BL_OOT_IN, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_TEL_NR_EXT, chr(10)||chr(11)||chr(13),' ')) BL_TEL_NR_EXT, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_TYP_CD, chr(10)||chr(11)||chr(13),' ')) BL_TYP_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_AREA, chr(10)||chr(11)||chr(13),' ')) BL_AREA, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_UPARS_TX, chr(10)||chr(11)||chr(13),' ')) BL_UPARS_TX, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_STATE_CD, chr(10)||chr(11)||chr(13),' ')) BL_STATE_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.SVC_REGN_GRID_ID, chr(10)||chr(11)||chr(13),' ')) SVC_REGN_GRID_ID, 
TO_CHAR(TRANSLATE(ARCH_CALL.TOW_DST_UPRS_TX, chr(10)||chr(11)||chr(13),' ')) TOW_DST_UPRS_TX, 
TO_CHAR(TRANSLATE(ARCH_CALL.FACIL_INT_ID, chr(10)||chr(11)||chr(13),' ')) FACIL_INT_ID, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_A_SVC_TRK_ID, chr(10)||chr(11)||chr(13),' ')) SC_A_SVC_TRK_ID, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_A_SVC_TRK_D_ID, chr(10)||chr(11)||chr(13),' ')) SC_A_SVC_TRK_D_ID, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_REASGN_RSN_CD, chr(10)||chr(11)||chr(13),' ')) SC_REASGN_RSN_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.PROB1_CD, chr(10)||chr(11)||chr(13),' ')) PROB1_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.PROB2_CD, chr(10)||chr(11)||chr(13),' ')) PROB2_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.CSH_COLL, chr(10)||chr(11)||chr(13),' ')) CSH_COLL, 
TO_CHAR(TRANSLATE(ARCH_CALL.STATUS_CHGS, chr(10)||chr(11)||chr(13),' ')) STATUS_CHGS, 
TO_CHAR(TRANSLATE(ARCH_CALL.EMPLE_ID, chr(10)||chr(11)||chr(13),' ')) EMPLE_ID,
TO_CHAR(ARCH_CALL.SC_CNTC_EXP_DT_YR,'YYYY-MM-DD HH24:MI:SS') AS SC_CNTC_EXP_DT_YR,
TO_CHAR(TRANSLATE(ARCH_CALL.PLUS_IND, chr(10)||chr(11)||chr(13),' ')) PLUS_IND, 
TO_CHAR(TRANSLATE(ARCH_CALL.CHG_ENTITLEMENT, chr(10)||chr(11)||chr(13),' ')) CHG_ENTITLEMENT, 
TO_CHAR(TRANSLATE(ARCH_CALL.BATCH_CD, chr(10)||chr(11)||chr(13),' ')) BATCH_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.MMP_CD, chr(10)||chr(11)||chr(13),' ')) MMP_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_SVC_TIK_NO, chr(10)||chr(11)||chr(13),' ')) SC_SVC_TIK_NO, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_TOD_IND, chr(10)||chr(11)||chr(13),' ')) SC_TOD_IND, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_PROB_IND, chr(10)||chr(11)||chr(13),' ')) SC_PROB_IND, 
TO_CHAR(TRANSLATE(ARCH_CALL.RATE_CATEGORY, chr(10)||chr(11)||chr(13),' ')) RATE_CATEGORY, 
TO_CHAR(TRANSLATE(ARCH_CALL.CALL_SOURCE, chr(10)||chr(11)||chr(13),' ')) CALL_SOURCE, 
TO_CHAR(TRANSLATE(ARCH_CALL.CALL_COST, chr(10)||chr(11)||chr(13),' ')) CALL_COST, 
TO_CHAR(TRANSLATE(ARCH_CALL.MILES_START, chr(10)||chr(11)||chr(13),' ')) MILES_START, 
TO_CHAR(TRANSLATE(ARCH_CALL.MILES_ONLC, chr(10)||chr(11)||chr(13),' ')) MILES_ONLC, 
TO_CHAR(TRANSLATE(ARCH_CALL.MILES_DEST, chr(10)||chr(11)||chr(13),' ')) MILES_DEST, 
TO_CHAR(TRANSLATE(ARCH_CALL.PAY_ID, chr(10)||chr(11)||chr(13),' ')) PAY_ID, 
TO_CHAR(TRANSLATE(ARCH_CALL.SUSPENDED, chr(10)||chr(11)||chr(13),' ')) SUSPENDED, 
TO_CHAR(TRANSLATE(ARCH_CALL.RECIPROCALIZED, chr(10)||chr(11)||chr(13),' ')) RECIPROCALIZED, 
TO_CHAR(TRANSLATE(ARCH_CALL.DTL_PROB1_CD, chr(10)||chr(11)||chr(13),' ')) DTL_PROB1_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.DTL_PROB2_CD, chr(10)||chr(11)||chr(13),' ')) DTL_PROB2_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.DTL_STS_RSN_CD, chr(10)||chr(11)||chr(13),' ')) DTL_STS_RSN_CD, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_VEH_TYPE, chr(10)||chr(11)||chr(13),' ')) SC_VEH_TYPE, 
TO_CHAR(TRANSLATE(ARCH_CALL.SPP_TKT, chr(10)||chr(11)||chr(13),' ')) SPP_TKT, 
TO_CHAR(TRANSLATE(ARCH_CALL.SPP_CADJ, chr(10)||chr(11)||chr(13),' ')) SPP_CADJ, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_TRC_FLAG, chr(10)||chr(11)||chr(13),' ')) SC_TRC_FLAG, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_TD_TYPE, chr(10)||chr(11)||chr(13),' ')) SC_TD_TYPE, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_TIMEZONE, chr(10)||chr(11)||chr(13),' ')) BL_TIMEZONE, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_ZIP, chr(10)||chr(11)||chr(13),' ')) BL_ZIP, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_ZIP4, chr(10)||chr(11)||chr(13),' ')) BL_ZIP4, 
TO_CHAR(TRANSLATE(ARCH_CALL.HIGHWAY_CALL, chr(10)||chr(11)||chr(13),' ')) HIGHWAY_CALL, 
TO_CHAR(TRANSLATE(ARCH_CALL.MILE_MARKER, chr(10)||chr(11)||chr(13),' ')) MILE_MARKER, 
TO_CHAR(TRANSLATE(ARCH_CALL.CALLBOX, chr(10)||chr(11)||chr(13),' ')) CALLBOX, 
TO_CHAR(TRANSLATE(ARCH_CALL.BL_VERIFIED_LOC, chr(10)||chr(11)||chr(13),' ')) BL_VERIFIED_LOC, 
TO_CHAR(TRANSLATE(ARCH_CALL.AA_VERIFIED_LOC, chr(10)||chr(11)||chr(13),' ')) AA_VERIFIED_LOC, 
TO_CHAR(TRANSLATE(ARCH_CALL.ACCIDENT_ASSIST_FLAG, chr(10)||chr(11)||chr(13),' ')) ACCIDENT_ASSIST_FLAG, 
TO_CHAR(TRANSLATE(ARCH_CALL.PUBLIC_FLAG, chr(10)||chr(11)||chr(13),' ')) PUBLIC_FLAG, 
TO_CHAR(TRANSLATE(ARCH_CALL.TD_VERIFIED_LOC, chr(10)||chr(11)||chr(13),' ')) TD_VERIFIED_LOC,
TO_CHAR(TRANSLATE(ARCH_CALL.ER_DIRECTIONS, chr(10)||chr(11)||chr(13),' ')) ER_DIRECTIONS, 
TO_CHAR(TRANSLATE(ARCH_CALL.TOW_DIRECTIONS, chr(10)||chr(11)||chr(13),' ')) TOW_DIRECTIONS, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_VEH_ODOMETER, chr(10)||chr(11)||chr(13),' ')) SC_VEH_ODOMETER, 
TO_CHAR(TRANSLATE(ARCH_CALL.SC_VEH_VIN, chr(10)||chr(11)||chr(13),' ')) SC_VEH_VIN,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM
NCA_ERS_HIST.ARCH_CALL ARCH_CALL
INNER JOIN
(
SELECT SC_ID, SC_DT , COMM_CTR_ID FROM NCA_ERS_HIST.ARCH_CALL  
WHERE  ARCH_DATE >= TO_DATE('2020-01-13 00:00:00','yyyy-mm-dd hh24:mi:ss')
UNION
SELECT  SC_ID, SC_DT , COMM_CTR_ID FROM NCA_ERS_HIST.SC_CALL_COMMENT SC 
WHERE  SC_COMM_ADD_TM >=  TO_DATE('2020-01-13 00:00:00','yyyy-mm-dd hh24:mi:ss')
UNION 
SELECT  cast(KEY3_DA AS number)   AS SC_ID,TO_DATE(TRANSLATE(TRIM(KEY2_DA),chr(47),'-'),'mm-dd-yyyy hh24:mi:ss' ) AS SC_DT , KEY1_DA AS COMM_CTR_ID 
FROM NCA_APD.D2K_AUDIT 
WHERE   KEY1_DA =  '005' AND  TIMESTAMP_DT >=   TO_DATE('2020-01-13 00:00:00','yyyy-mm-dd hh24:mi:ss') 
AND KEY3_DA IS NOT NULL AND trim(KEY3_DA) NOT IN ('0','1','2','3','4','5','6','7','8','9')

AND trim(KEY3_DA) NOT IN (0,1,2,3,4,5,6,7,8,9)
) CDC
ON ARCH_CALL.SC_ID=CDC.SC_ID AND ARCH_CALL.COMM_CTR_ID=CDC.COMM_CTR_ID AND ARCH_CALL.SC_DT=CDC.SC_DT