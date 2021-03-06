MERGE OPERATIONAL.MBR AS TGT
USING LANDING.WORK_MBR AS SRC
ON TGT.MBR_KY = CAST(TRIM(SRC.MBR_KY) AS INT64)
WHEN MATCHED
THEN UPDATE SET
TGT.MBRS_ID = SRC.MBRS_ID,
TGT.MBR_ASSOC_ID = SRC.MBR_ASSOC_ID,
TGT.MBR_JN_AAA_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_JN_AAA_DT),
TGT.MBR_JN_CLB_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_JN_CLB_DT),
TGT.MBR_DUP_CRD_CT = CAST(TRIM(SRC.MBR_DUP_CRD_CT) AS INT64),
TGT.MBR_STS_DSC_CD = SRC.MBR_STS_DSC_CD,
TGT.MBR_STS_CD = SRC.MBR_STS_CD,
TGT.MBR_SLTN_CD = SRC.MBR_SLTN_CD,
TGT.MBR_RELAT_CD = SRC.MBR_RELAT_CD,
TGT.MBR_RSN_JOIN_CD = SRC.MBR_RSN_JOIN_CD,
TGT.MBR_PREV_CLUB_CD = SRC.MBR_PREV_CLUB_CD,
TGT.MBR_PREV_MBRS_ID = SRC.MBR_PREV_MBRS_ID,
TGT.MBR_DO_NOT_REN_IN = SRC.MBR_DO_NOT_REN_IN,
TGT.MBR_SLCT_CD = SRC.MBR_SLCT_CD,
TGT.MBR_SRC_SLS_CD = SRC.MBR_SRC_SLS_CD,
TGT.MBR_CANC_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_CANC_DT),
TGT.MBR_COMM_CD = SRC.MBR_COMM_CD,
TGT.SLS_AGT_KY = CAST(TRIM(SRC.SLS_AGT_KY) AS INT64),
TGT.MBRS_KY = CAST(TRIM(SRC.MBRS_KY) AS INT64),
TGT.MBR_TYP_CD = SRC.MBR_TYP_CD,
TGT.MBR_PAID_BY_CD = SRC.MBR_PAID_BY_CD,
TGT.MBR_FREE_ASSOC_IN = SRC.MBR_FREE_ASSOC_IN,
TGT.MBR_RETN_ASGD_IN = SRC.MBR_RETN_ASGD_IN,
TGT.MBR_DUPCD_REQ_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_DUPCD_REQ_DT),
TGT.MBR_SEC_SAGT_KY = CAST(TRIM(SRC.MBR_SEC_SAGT_KY) AS INT64),
TGT.MBR_DUP_STCK_CT = CAST(TRIM(SRC.MBR_DUP_STCK_CT) AS INT64),
TGT.MBR_DUPSTCK_REQ_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_DUPSTCK_REQ_DT),
TGT.MBR_BIL_CAT_CD = SRC.MBR_BIL_CAT_CD,
TGT.MBR_CERT_TX = SRC.MBR_CERT_TX,
TGT.MBR_CRED_CYCLE_NR = CAST(TRIM(SRC.MBR_CRED_CYCLE_NR) AS INT64),
TGT.MBR_ACTV_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_ACTV_DT),
TGT.MBR_ERS_USAGE_YR1 = CAST(TRIM(SRC.MBR_ERS_USAGE_YR1) AS INT64),
TGT.MBR_ERS_USAGE_YR2 = CAST(TRIM(SRC.MBR_ERS_USAGE_YR2) AS INT64),
TGT.MBR_ERS_USAGE_YR3 = CAST(TRIM(SRC.MBR_ERS_USAGE_YR3) AS INT64),
TGT.MBR_PREV_STS_DSC_CD = SRC.MBR_PREV_STS_DSC_CD,
TGT.MBR_REINST_PRNT_CRD_IN = SRC.MBR_REINST_PRNT_CRD_IN,
TGT.DUP_CARD_REASON = SRC.DUP_CARD_REASON,
TGT.MBR_DO_NOT_REN_RSN = SRC.MBR_DO_NOT_REN_RSN,
TGT.MBR_LANGUAGE = SRC.MBR_LANGUAGE,
TGT.CARD_REQT_SKIP_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.CARD_REQT_SKIP_DT),
TGT.MBR_EFF_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_EFF_DT),
TGT.MBR_PREV_COMM_CD = SRC.MBR_PREV_COMM_CD,
TGT.MBR_DNR_EXPIR_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_DNR_EXPIR_DT),
TGT.MBR_MITA_PENDING = SRC.MBR_MITA_PENDING,
TGT.MBR_ADD_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_ADD_DT),
TGT.MBR_RENEW_CT = CAST(TRIM(SRC.MBR_RENEW_CT) AS INT64),
TGT.MBR_OVERAGE_TRAN_IN = SRC.MBR_OVERAGE_TRAN_IN,
TGT.MBR_REINST_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_REINST_DT),
TGT.CARD_REQT_SKIP_REASON = SRC.CARD_REQT_SKIP_REASON,
TGT.MBR_EXPIR_DT = PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_EXPIR_DT),
TGT.PREF_METH_CNT = SRC.PREF_METH_CNT,
TGT.LAST_UPDATE_DTTIME = CURRENT_DATETIME()
WHEN NOT MATCHED THEN INSERT
(MBR_KY,
MBRS_ID,
MBR_ASSOC_ID,
MBR_JN_AAA_DT,
MBR_JN_CLB_DT,
MBR_DUP_CRD_CT,
MBR_STS_DSC_CD,
MBR_STS_CD,
MBR_SLTN_CD,
MBR_RELAT_CD,
MBR_RSN_JOIN_CD,
MBR_PREV_CLUB_CD,
MBR_PREV_MBRS_ID,
MBR_DO_NOT_REN_IN,
MBR_SLCT_CD,
MBR_SRC_SLS_CD,
MBR_CANC_DT,
MBR_COMM_CD,
SLS_AGT_KY,
MBRS_KY,
MBR_TYP_CD,
MBR_PAID_BY_CD,
MBR_FREE_ASSOC_IN,
MBR_RETN_ASGD_IN,
MBR_DUPCD_REQ_DT,
MBR_SEC_SAGT_KY,
MBR_DUP_STCK_CT,
MBR_DUPSTCK_REQ_DT,
MBR_BIL_CAT_CD,
MBR_CERT_TX,
MBR_CRED_CYCLE_NR,
MBR_ACTV_DT,
MBR_ERS_USAGE_YR1,
MBR_ERS_USAGE_YR2,
MBR_ERS_USAGE_YR3,
MBR_PREV_STS_DSC_CD,
MBR_REINST_PRNT_CRD_IN,
DUP_CARD_REASON,
MBR_DO_NOT_REN_RSN,
MBR_LANGUAGE,
CARD_REQT_SKIP_DT,
MBR_EFF_DT,
MBR_PREV_COMM_CD,
MBR_DNR_EXPIR_DT,
MBR_MITA_PENDING,
MBR_ADD_DT,
MBR_RENEW_CT,
MBR_OVERAGE_TRAN_IN,
MBR_REINST_DT,
CARD_REQT_SKIP_REASON,
MBR_EXPIR_DT,
PREF_METH_CNT,
LAST_UPDATE_DTTIME,
CREATED_DT,
JOB_RUN_ID,
SOURCE_SYSTEM_CD
)
VALUES
(
CAST(TRIM(SRC.MBR_KY) AS INT64),
SRC.MBRS_ID,
SRC.MBR_ASSOC_ID,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_JN_AAA_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_JN_CLB_DT),
CAST(TRIM(SRC.MBR_DUP_CRD_CT) AS INT64),
SRC.MBR_STS_DSC_CD,
SRC.MBR_STS_CD,
SRC.MBR_SLTN_CD,
SRC.MBR_RELAT_CD,
SRC.MBR_RSN_JOIN_CD,
SRC.MBR_PREV_CLUB_CD,
SRC.MBR_PREV_MBRS_ID,
SRC.MBR_DO_NOT_REN_IN,
SRC.MBR_SLCT_CD,
SRC.MBR_SRC_SLS_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_CANC_DT),
SRC.MBR_COMM_CD,
CAST(TRIM(SRC.SLS_AGT_KY) AS INT64),
CAST(TRIM(SRC.MBRS_KY) AS INT64),
SRC.MBR_TYP_CD,
SRC.MBR_PAID_BY_CD,
SRC.MBR_FREE_ASSOC_IN,
SRC.MBR_RETN_ASGD_IN,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_DUPCD_REQ_DT),
CAST(TRIM(SRC.MBR_SEC_SAGT_KY) AS INT64),
CAST(TRIM(SRC.MBR_DUP_STCK_CT) AS INT64),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_DUPSTCK_REQ_DT),
SRC.MBR_BIL_CAT_CD,
SRC.MBR_CERT_TX,
CAST(TRIM(SRC.MBR_CRED_CYCLE_NR) AS INT64),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_ACTV_DT),
CAST(TRIM(SRC.MBR_ERS_USAGE_YR1) AS INT64),
CAST(TRIM(SRC.MBR_ERS_USAGE_YR2) AS INT64),
CAST(TRIM(SRC.MBR_ERS_USAGE_YR3) AS INT64),
SRC.MBR_PREV_STS_DSC_CD,
SRC.MBR_REINST_PRNT_CRD_IN,
SRC.DUP_CARD_REASON,
SRC.MBR_DO_NOT_REN_RSN,
SRC.MBR_LANGUAGE,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.CARD_REQT_SKIP_DT),
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_EFF_DT),
SRC.MBR_PREV_COMM_CD,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_DNR_EXPIR_DT),
SRC.MBR_MITA_PENDING,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_ADD_DT),
CAST(TRIM(SRC.MBR_RENEW_CT) AS INT64),
SRC.MBR_OVERAGE_TRAN_IN,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_REINST_DT),
SRC.CARD_REQT_SKIP_REASON,
PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',SRC.MBR_EXPIR_DT),
SRC.PREF_METH_CNT,
CURRENT_DATETIME(),
CURRENT_DATE(),
'jobrunid',
'CONNECT SUITE'
)