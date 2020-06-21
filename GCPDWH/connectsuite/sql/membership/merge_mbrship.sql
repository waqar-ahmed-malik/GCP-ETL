MERGE OPERATIONAL.MBRSHIP AS TGT
USING LANDING.WORK_MBRSHIP AS SRC
ON TGT.MBRS_KY = CAST(TRIM(SRC.MBRS_KY) AS INT64)
WHEN MATCHED
THEN UPDATE SET
TGT.MBRS_ID = SRC.MBRS_ID,
TGT.MBRS_CH_DIGIT_NR = CAST(TRIM(SRC.MBRS_CH_DIGIT_NR) AS INT64),
TGT.MBRS_DNR_REN_CD = SRC.MBRS_DNR_REN_CD,
TGT.MBRS_REN_METH_CD = SRC.MBRS_REN_METH_CD,
TGT.MBRS_GFT_TYP_CD = SRC.MBRS_GFT_TYP_CD,
TGT.MBRS_EXPIR_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_EXPIR_DT),
TGT.MBRS_CRD_EXPIR_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CRD_EXPIR_DT),
TGT.MBRS_INC_CNTR_CT = SRC.MBRS_INC_CNTR_CT,
TGT.MBRS_INCM_CAT_CD = SRC.MBRS_INCM_CAT_CD,
TGT.MBRS_XFND_DISP_CD = SRC.MBRS_XFND_DISP_CD,
TGT.MBRS_CRD_SHP_CD = SRC.MBRS_CRD_SHP_CD,
TGT.MBRS_DUP_CRD_CT = CAST(TRIM(SRC.MBRS_DUP_CRD_CT) AS INT64),
TGT.MBRS_DUPCD_REQ_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_DUPCD_REQ_DT),
TGT.MBRS_DUP_STICK_CT = CAST(TRIM(SRC.MBRS_DUP_STICK_CT) AS INT64),
TGT.MBRS_DUPSTK_REQ_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_DUPSTK_REQ_DT),
TGT.MBRS_BIL_PRT_CD = SRC.MBRS_BIL_PRT_CD,
TGT.MBRS_STS_CD = SRC.MBRS_STS_CD,
TGT.MBRS_STS_DSC_CD = SRC.MBRS_STS_DSC_CD,
TGT.MBRS_BIL_CAT_CD = SRC.MBRS_BIL_CAT_CD,
TGT.MBRS_RTN_CHK_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_RTN_CHK_DT),
TGT.MBRS_RTN_CHK_CT = CAST(TRIM(SRC.MBRS_RTN_CHK_CT) AS INT64),
TGT.MBRS_DUES_COST_AT = CAST(TRIM(SRC.MBRS_DUES_COST_AT) AS FLOAT64),
TGT.MBRS_PMT_APPLY_AT = CAST(TRIM(SRC.MBRS_PMT_APPLY_AT) AS FLOAT64),
TGT.MBRS_PMT_PEND_AT = CAST(TRIM(SRC.MBRS_PMT_PEND_AT) AS FLOAT64),
TGT.MBRS_PMT_RFND_AT = CAST(TRIM(SRC.MBRS_PMT_RFND_AT) AS FLOAT64),
TGT.MBRS_DUES_ADJ_AT = CAST(TRIM(SRC.MBRS_DUES_ADJ_AT) AS FLOAT64),
TGT.MBRS_CRED_APPLY_AT = CAST(TRIM(SRC.MBRS_CRED_APPLY_AT) AS FLOAT64),
TGT.MBRS_CRED_PEND_AT = CAST(TRIM(SRC.MBRS_CRED_PEND_AT) AS FLOAT64),
TGT.MBRS_RTNCK_FEE_AT = CAST(TRIM(SRC.MBRS_RTNCK_FEE_AT) AS FLOAT64),
TGT.MBRS_OTH_FEE_AT = CAST(TRIM(SRC.MBRS_OTH_FEE_AT) AS FLOAT64),
TGT.MBRS_PLS_IN = SRC.MBRS_PLS_IN,
TGT.MBRS_OCP_CD = SRC.MBRS_OCP_CD,
TGT.MBRS_INC_LVL_CD = SRC.MBRS_INC_LVL_CD,
TGT.MBRS_RTN_CHK_NR = SRC.MBRS_RTN_CHK_NR,
TGT.MBRS_MKT_TRK_CD = SRC.MBRS_MKT_TRK_CD,
TGT.MBRS_GFT_MSG_TX = SRC.MBRS_GFT_MSG_TX,
TGT.MBRS_DNR_EFF_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_DNR_EFF_DT),
TGT.MBRS_KIT_ISSD_IN = SRC.MBRS_KIT_ISSD_IN,
TGT.MBRS_MICRVSN_CD = SRC.MBRS_MICRVSN_CD,
TGT.BRN_KY = CAST(TRIM(SRC.BRN_KY) AS INT64),
TGT.MBRS_REFR_KY = CAST(TRIM(SRC.MBRS_REFR_KY) AS INT64),
TGT.MBRS_DNR_KY = CAST(TRIM(SRC.MBRS_DNR_KY) AS INT64),
TGT.MBRS_SPSR_KY = CAST(TRIM(SRC.MBRS_SPSR_KY) AS INT64),
TGT.MBRS_INCENT_TYP_CD = SRC.MBRS_INCENT_TYP_CD,
TGT.MBRS_CANC_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CANC_DT),
TGT.MBRS_REN_METH_IN = SRC.MBRS_REN_METH_IN,
TGT.MBRS_REN_METH_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_REN_METH_DT),
TGT.MBRS_ACTV_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_ACTV_DT),
TGT.MBRS_ENT_AT = CAST(TRIM(SRC.MBRS_ENT_AT) AS FLOAT64),
TGT.MBRS_D2K_UPDT_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_D2K_UPDT_DT),
TGT.MBRS_SAFE_APPL_IN = SRC.MBRS_SAFE_APPL_IN,
TGT.MBRS_SAFE_AT = CAST(TRIM(SRC.MBRS_SAFE_AT) AS FLOAT64) ,
TGT.MBRS_PREV_STS_DSC_CD = SRC.MBRS_PREV_STS_DSC_CD,
TGT.MBRS_ENT_COL_AT = CAST(TRIM(SRC.MBRS_ENT_COL_AT) AS FLOAT64),
TGT.MBRS_KIT = SRC.MBRS_KIT,
TGT.MBRS_RTD2K_UPDT_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_RTD2K_UPDT_DT),
TGT.MBRS_CRD_PRT_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CRD_PRT_DT),
TGT.MBRS_DO_NOT_REN_RSN = SRC.MBRS_DO_NOT_REN_RSN,
TGT.MBRS_FUTURE_EFFECT_IN = SRC.MBRS_FUTURE_EFFECT_IN,
TGT.SAFE_FUND_DNT_AMOUNT = CAST(TRIM(SRC.SAFE_FUND_DNT_AMOUNT) AS FLOAT64),
TGT.SAFE_FUND_DNT_ANNUAL = SRC.SAFE_FUND_DNT_ANNUAL,
TGT.BEMI_CD = SRC.BEMI_CD,
TGT.EXTRACT_HANDLE_CD = SRC.EXTRACT_HANDLE_CD,
TGT.AUTOREN_DISC_APPLIED_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.AUTOREN_DISC_APPLIED_DT),
TGT.MBRS_CURR_EFFECT_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CURR_EFFECT_DT),
TGT.MBRS_REINST_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_REINST_DT),
TGT.MBRS_OLJ_TEMP_IN = SRC.MBRS_OLJ_TEMP_IN,
TGT.PAY_PLAN_TYPE_CD = SRC.PAY_PLAN_TYPE_CD,
TGT.LAST_UPDATE_DTTIME = CURRENT_DATETIME()
WHEN NOT MATCHED THEN INSERT
(
MBRS_KY,
MBRS_ID,
MBRS_CH_DIGIT_NR,
MBRS_DNR_REN_CD,
MBRS_REN_METH_CD,
MBRS_GFT_TYP_CD,
MBRS_EXPIR_DT,
MBRS_CRD_EXPIR_DT,
MBRS_INC_CNTR_CT,
MBRS_INCM_CAT_CD,
MBRS_XFND_DISP_CD,
MBRS_CRD_SHP_CD,
MBRS_DUP_CRD_CT,
MBRS_DUPCD_REQ_DT,
MBRS_DUP_STICK_CT,
MBRS_DUPSTK_REQ_DT,
MBRS_BIL_PRT_CD,
MBRS_STS_CD,
MBRS_STS_DSC_CD,
MBRS_BIL_CAT_CD,
MBRS_RTN_CHK_DT,
MBRS_RTN_CHK_CT,
MBRS_DUES_COST_AT,
MBRS_PMT_APPLY_AT,
MBRS_PMT_PEND_AT,
MBRS_PMT_RFND_AT,
MBRS_DUES_ADJ_AT,
MBRS_CRED_APPLY_AT,
MBRS_CRED_PEND_AT,
MBRS_RTNCK_FEE_AT,
MBRS_OTH_FEE_AT,
MBRS_PLS_IN,
MBRS_OCP_CD,
MBRS_INC_LVL_CD,
MBRS_RTN_CHK_NR,
MBRS_MKT_TRK_CD,
MBRS_GFT_MSG_TX,
MBRS_DNR_EFF_DT,
MBRS_KIT_ISSD_IN,
MBRS_MICRVSN_CD,
BRN_KY,
MBRS_REFR_KY,
MBRS_DNR_KY,
MBRS_SPSR_KY,
MBRS_INCENT_TYP_CD,
MBRS_CANC_DT,
MBRS_REN_METH_IN,
MBRS_REN_METH_DT,
MBRS_ACTV_DT,
MBRS_ENT_AT,
MBRS_D2K_UPDT_DT,
MBRS_SAFE_APPL_IN,
MBRS_SAFE_AT,
MBRS_PREV_STS_DSC_CD,
MBRS_ENT_COL_AT,
MBRS_KIT,
MBRS_RTD2K_UPDT_DT,
MBRS_CRD_PRT_DT,
MBRS_DO_NOT_REN_RSN,
MBRS_FUTURE_EFFECT_IN,
SAFE_FUND_DNT_AMOUNT,
SAFE_FUND_DNT_ANNUAL,
BEMI_CD,
EXTRACT_HANDLE_CD,
AUTOREN_DISC_APPLIED_DT,
MBRS_CURR_EFFECT_DT,
MBRS_REINST_DT,
MBRS_OLJ_TEMP_IN,
PAY_PLAN_TYPE_CD,
LAST_UPDATE_DTTIME,
CREATED_DT,
JOB_RUN_ID,
SOURCE_SYSTEM_CD
)
VALUES(
CAST(TRIM(SRC.MBRS_KY) AS INT64),
SRC.MBRS_ID,
CAST(TRIM(SRC.MBRS_CH_DIGIT_NR) AS INT64),
SRC.MBRS_DNR_REN_CD,
SRC.MBRS_REN_METH_CD,
SRC.MBRS_GFT_TYP_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_EXPIR_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CRD_EXPIR_DT),
SRC.MBRS_INC_CNTR_CT,
SRC.MBRS_INCM_CAT_CD,
SRC.MBRS_XFND_DISP_CD,
SRC.MBRS_CRD_SHP_CD,
CAST(TRIM(SRC.MBRS_DUP_CRD_CT) AS INT64),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_DUPCD_REQ_DT),
CAST(TRIM(SRC.MBRS_DUP_STICK_CT) AS INT64),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_DUPSTK_REQ_DT),
SRC.MBRS_BIL_PRT_CD,
SRC.MBRS_STS_CD,
SRC.MBRS_STS_DSC_CD,
SRC.MBRS_BIL_CAT_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_RTN_CHK_DT),
CAST(TRIM(SRC.MBRS_RTN_CHK_CT) AS INT64),
CAST(TRIM(SRC.MBRS_DUES_COST_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_PMT_APPLY_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_PMT_PEND_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_PMT_RFND_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_DUES_ADJ_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_CRED_APPLY_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_CRED_PEND_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_RTNCK_FEE_AT) AS FLOAT64),
CAST(TRIM(SRC.MBRS_OTH_FEE_AT) AS FLOAT64),
SRC.MBRS_PLS_IN,
SRC.MBRS_OCP_CD,
SRC.MBRS_INC_LVL_CD,
SRC.MBRS_RTN_CHK_NR,
SRC.MBRS_MKT_TRK_CD,
SRC.MBRS_GFT_MSG_TX,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_DNR_EFF_DT),
SRC.MBRS_KIT_ISSD_IN,
SRC.MBRS_MICRVSN_CD,
CAST(TRIM(SRC.BRN_KY) AS INT64),
CAST(TRIM(SRC.MBRS_REFR_KY) AS INT64),
CAST(TRIM(SRC.MBRS_DNR_KY) AS INT64),
CAST(TRIM(SRC.MBRS_SPSR_KY) AS INT64),
SRC.MBRS_INCENT_TYP_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CANC_DT),
SRC.MBRS_REN_METH_IN,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_REN_METH_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_ACTV_DT),
CAST(TRIM(SRC.MBRS_ENT_AT) AS FLOAT64),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_D2K_UPDT_DT),
SRC.MBRS_SAFE_APPL_IN,
CAST(TRIM(SRC.MBRS_SAFE_AT) AS FLOAT64) ,
SRC.MBRS_PREV_STS_DSC_CD,
CAST(TRIM(SRC.MBRS_ENT_COL_AT) AS FLOAT64),
SRC.MBRS_KIT,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_RTD2K_UPDT_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CRD_PRT_DT),
SRC.MBRS_DO_NOT_REN_RSN,
SRC.MBRS_FUTURE_EFFECT_IN,
CAST(TRIM(SRC.SAFE_FUND_DNT_AMOUNT) AS FLOAT64),
SRC.SAFE_FUND_DNT_ANNUAL,
SRC.BEMI_CD,
SRC.EXTRACT_HANDLE_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.AUTOREN_DISC_APPLIED_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_CURR_EFFECT_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBRS_REINST_DT),
SRC.MBRS_OLJ_TEMP_IN,
SRC.PAY_PLAN_TYPE_CD,
CURRENT_DATETIME(),
CURRENT_DATE(),
'jobrunid',
'CONNECT SUITE')