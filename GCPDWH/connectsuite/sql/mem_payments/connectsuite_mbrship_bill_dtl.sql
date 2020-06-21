MERGE INTO 
  OPERATIONAL.CONNECTSUITE_MBRSHIP_BILL_DTL TGT
USING
(SELECT 
CAST(BDTL_KY AS INT64) AS BDTL_KY,
CAST(BDTL_LN_ITM_NR AS INT64) AS BDTL_LN_ITM_NR ,
CAST(BDTL_AT AS FLOAT64) AS BDTL_AT,
BDTL_SLTN_CD,
BDTL_CMPT_CD,
CAST(BILL_KY AS INT64) AS BILL_KY,
CAST(MBR_KY AS INT64) AS MBR_KY,
CAST(RIDE_KY AS INT64) AS RIDE_KY,
CAST(MBRS_KY AS INT64) AS MBRS_KY,
CAST(BDTL_PD_AT AS FLOAT64) AS BDTL_PD_AT,
BDTL_BIL_CAT_CD 
FROM LANDING.WORK_CS_MBRSHIP_BILL_DTL) STG
on TGT.BDTL_KY=STG.BDTL_KY
WHEN MATCHED  THEN  UPDATE   SET
TGT.BDTL_KY=STG.BDTL_KY,
TGT.BDTL_LN_ITM_NR=STG.BDTL_LN_ITM_NR,
TGT.BDTL_AT=STG.BDTL_AT,
TGT.BDTL_SLTN_CD=STG.BDTL_SLTN_CD,
TGT.BDTL_CMPT_CD=STG.BDTL_CMPT_CD,
TGT.BILL_KY=STG.BILL_KY,
TGT.MBR_KY=STG.MBR_KY,
TGT.RIDE_KY=STG.RIDE_KY,
TGT.MBRS_KY=STG.MBRS_KY,
TGT.BDTL_PD_AT=STG.BDTL_PD_AT,
TGT.BDTL_BIL_CAT_CD=STG.BDTL_BIL_CAT_CD,
TGT.JOB_RUN_ID ='jobrunid', -- Need to change
TGT.LAST_UPDATE_DT=CURRENT_DATETIME()
WHEN NOT MATCHED
  THEN
INSERT(
BDTL_KY,
BDTL_LN_ITM_NR,
BDTL_AT,
BDTL_SLTN_CD,
BDTL_CMPT_CD,
BILL_KY,
MBR_KY,
RIDE_KY,
MBRS_KY,
BDTL_PD_AT,
BDTL_BIL_CAT_CD,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DT,
LAST_UPDATE_DT
)  
VALUES
(
STG.BDTL_KY,
STG.BDTL_LN_ITM_NR,
STG.BDTL_AT,
STG.BDTL_SLTN_CD,
STG.BDTL_CMPT_CD,
STG.BILL_KY,
STG.MBR_KY,
STG.RIDE_KY,
STG.MBRS_KY,
STG.BDTL_PD_AT,
STG.BDTL_BIL_CAT_CD,
'jobrunid', -- Need to Change
'CONNECT SUITE',
CURRENT_DATETIME(),
CURRENT_DATETIME()
)  
  