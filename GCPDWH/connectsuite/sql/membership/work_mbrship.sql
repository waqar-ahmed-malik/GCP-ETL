SELECT 
TRIM(CONVERT(CHAR,MBRSHIP.MBRS_KY)) AS MBRS_KY,
TRIM(MBRS_ID) AS MBRS_ID,
TRIM(CONVERT(CHAR,MBRS_CH_DIGIT_NR)) AS MBRS_CH_DIGIT_NR,
TRIM(MBRS_DNR_REN_CD) AS MBRS_DNR_REN_CD,
TRIM(MBRS_REN_METH_CD) AS MBRS_REN_METH_CD,
TRIM(MBRS_GFT_TYP_CD) AS MBRS_GFT_TYP_CD,
TRIM(CONVERT(CHAR,MBRS_EXPIR_DT,121)) AS MBRS_EXPIR_DT,
TRIM(CONVERT(CHAR,MBRS_CRD_EXPIR_DT,121)) AS MBRS_CRD_EXPIR_DT,
TRIM(MBRS_INC_CNTR_CT) AS MBRS_INC_CNTR_CT,
TRIM(MBRS_INCM_CAT_CD) AS MBRS_INCM_CAT_CD,
TRIM(MBRS_XFND_DISP_CD) AS MBRS_XFND_DISP_CD,
TRIM(MBRS_CRD_SHP_CD) AS MBRS_CRD_SHP_CD,
TRIM(CONVERT(CHAR,MBRS_DUP_CRD_CT)) AS MBRS_DUP_CRD_CT,
TRIM(CONVERT(CHAR,MBRS_DUPCD_REQ_DT,121)) AS MBRS_DUPCD_REQ_DT,
TRIM(CONVERT(CHAR,MBRS_DUP_STICK_CT)) AS MBRS_DUP_STICK_CT,
TRIM(CONVERT(CHAR,MBRS_DUPSTK_REQ_DT,121)) AS MBRS_DUPSTK_REQ_DT,
TRIM(MBRS_BIL_PRT_CD) AS MBRS_BIL_PRT_CD,
TRIM(MBRS_STS_CD) AS MBRS_STS_CD,
TRIM(MBRS_STS_DSC_CD) AS MBRS_STS_DSC_CD,
TRIM(MBRS_BIL_CAT_CD) AS MBRS_BIL_CAT_CD,
TRIM(CONVERT(CHAR,MBRS_RTN_CHK_DT,121)) AS MBRS_RTN_CHK_DT,
TRIM(CONVERT(CHAR,MBRS_RTN_CHK_CT)) AS MBRS_RTN_CHK_CT,
TRIM(CONVERT(CHAR,MBRS_DUES_COST_AT)) AS MBRS_DUES_COST_AT,
TRIM(CONVERT(CHAR,MBRS_PMT_APPLY_AT)) AS MBRS_PMT_APPLY_AT,
TRIM(CONVERT(CHAR,MBRS_PMT_PEND_AT)) AS MBRS_PMT_PEND_AT,
TRIM(CONVERT(CHAR,MBRS_PMT_RFND_AT)) AS MBRS_PMT_RFND_AT,
TRIM(CONVERT(CHAR,MBRS_DUES_ADJ_AT)) AS MBRS_DUES_ADJ_AT,
TRIM(CONVERT(CHAR,MBRS_CRED_APPLY_AT)) AS MBRS_CRED_APPLY_AT,
TRIM(CONVERT(CHAR,MBRS_CRED_PEND_AT)) AS MBRS_CRED_PEND_AT,
TRIM(CONVERT(CHAR,MBRS_RTNCK_FEE_AT)) AS MBRS_RTNCK_FEE_AT,
TRIM(CONVERT(CHAR,MBRS_OTH_FEE_AT)) AS MBRS_OTH_FEE_AT,
TRIM(MBRS_PLS_IN) AS MBRS_PLS_IN,
TRIM(MBRS_OCP_CD) AS MBRS_OCP_CD,
TRIM(MBRS_INC_LVL_CD) AS MBRS_INC_LVL_CD,
TRIM(MBRS_RTN_CHK_NR) AS MBRS_RTN_CHK_NR,
TRIM(MBRS_MKT_TRK_CD) AS MBRS_MKT_TRK_CD,
TRIM(MBRS_GFT_MSG_TX) AS MBRS_GFT_MSG_TX,
TRIM(CONVERT(CHAR,MBRS_DNR_EFF_DT,121)) AS MBRS_DNR_EFF_DT,
TRIM(MBRS_KIT_ISSD_IN) AS MBRS_KIT_ISSD_IN,
TRIM(MBRS_MICRVSN_CD) AS MBRS_MICRVSN_CD,
TRIM(CONVERT(CHAR,BRN_KY)) AS BRN_KY,
TRIM(CONVERT(CHAR,MBRS_REFR_KY)) AS MBRS_REFR_KY,
TRIM(CONVERT(CHAR,MBRS_DNR_KY)) AS MBRS_DNR_KY,
TRIM(CONVERT(CHAR,MBRS_SPSR_KY)) AS MBRS_SPSR_KY,
TRIM(MBRS_INCENT_TYP_CD) AS MBRS_INCENT_TYP_CD,
TRIM(CONVERT(CHAR,MBRS_CANC_DT,121)) AS MBRS_CANC_DT,
TRIM(MBRS_REN_METH_IN) AS MBRS_REN_METH_IN,
TRIM(CONVERT(CHAR,MBRS_REN_METH_DT,121)) AS MBRS_REN_METH_DT,
TRIM(CONVERT(CHAR,MBRS_ACTV_DT,121)) AS MBRS_ACTV_DT,
TRIM(CONVERT(CHAR,MBRS_ENT_AT)) AS MBRS_ENT_AT,
TRIM(CONVERT(CHAR,MBRS_D2K_UPDT_DT,121)) AS MBRS_D2K_UPDT_DT,
TRIM(MBRS_SAFE_APPL_IN) AS MBRS_SAFE_APPL_IN,
TRIM(CONVERT(CHAR,MBRS_SAFE_AT)) AS MBRS_SAFE_AT,
TRIM(MBRS_PREV_STS_DSC_CD) AS MBRS_PREV_STS_DSC_CD,
TRIM(CONVERT(CHAR,MBRS_ENT_COL_AT)) AS MBRS_ENT_COL_AT,
TRIM(MBRS_KIT) AS MBRS_KIT,
TRIM(CONVERT(CHAR,MBRS_RTD2K_UPDT_DT,121)) AS MBRS_RTD2K_UPDT_DT,
TRIM(CONVERT(CHAR,MBRS_CRD_PRT_DT,121)) AS MBRS_CRD_PRT_DT,
TRIM(CONVERT(CHAR,MBRS_DO_NOT_REN_RSN)) AS MBRS_DO_NOT_REN_RSN,
TRIM(CONVERT(CHAR,MBRS_FUTURE_EFFECT_IN)) AS MBRS_FUTURE_EFFECT_IN,
TRIM(CONVERT(CHAR,SAFE_FUND_DNT_AMOUNT)) AS SAFE_FUND_DNT_AMOUNT,
TRIM(SAFE_FUND_DNT_ANNUAL) AS SAFE_FUND_DNT_ANNUAL,
TRIM(BEMI_CD) AS BEMI_CD,
TRIM(EXTRACT_HANDLE_CD) AS EXTRACT_HANDLE_CD,
TRIM(CONVERT(CHAR,AUTOREN_DISC_APPLIED_DT,121)) AS AUTOREN_DISC_APPLIED_DT,
TRIM(CONVERT(CHAR,MBRS_CURR_EFFECT_DT,121)) AS MBRS_CURR_EFFECT_DT,
TRIM(CONVERT(CHAR,MBRS_REINST_DT,121)) AS MBRS_REINST_DT,
TRIM(MBRS_OLJ_TEMP_IN) AS MBRS_OLJ_TEMP_IN,
TRIM(PAY_PLAN_TYPE_CD) AS PAY_PLAN_TYPE_CD
FROM [M_PRD].[DBO].[MBRSHIP] MBRSHIP
  INNER JOIN (
    SELECT DISTINCT
      MBRS_KY,
      MAX(MBRS_UPDT_DT) AS  MBRS_UPDT_DT    
     FROM (
        SELECT
          MBRS_KY,
          MBRS_RTD2K_UPDT_DT AS MBRS_UPDT_DT
        FROM
          [M_PRD].[DBO].[MBRSHIP]
        WHERE

 WHERE
 CONVERT(DATE,
 MBRSHIP.MBRS_RTD2K_UPDT_DT) >= CONVERT(DATE,
           '2020-01-10' ) AND CONVERT(DATE,
   MBRSHIP.MBRS_RTD2K_UPDT_DT) <= CONVERT(DATE,
           GETDATE() - 1)
        
          -- CONVERT(DATE, MBRSHIP.MBRS_RTD2K_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
UNION
        SELECT
          DISTINCT MBRSHIP_COMMENT.MBRS_KY AS MBRS_KY,
          MBRSHIP_COMMENT.MCMT_UPDT_DT AS MBRS_UPDT_DT
        FROM
          [M_PRD].[DBO].[MBR],
          [M_PRD].[DBO].[MBRSHIP_COMMENT]
         WHERE
          -- CONVERT(DATE,MBRSHIP_COMMENT.MCMT_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
           WHERE
 CONVERT(DATE,
 MBRSHIP.MBRS_RTD2K_UPDT_DT) >= CONVERT(DATE,
           '2020-01-10' ) AND CONVERT(DATE,
   MBRSHIP.MBRS_RTD2K_UPDT_DT) <= CONVERT(DATE,
           GETDATE() - 1)
		  ) SQ  GROUP BY MBRS_KY ) CDC
  ON
    CDC.MBRS_KY=MBRSHIP.MBRS_KY