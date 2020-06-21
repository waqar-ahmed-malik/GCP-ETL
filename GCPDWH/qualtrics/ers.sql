INSERT INTO OPERATIONAL.QUALTRICS_SURVEY_RESPONSE_ERS(
START_DT,
END_DT  ,
STATUS  ,
IP_ADDRESS,
PROGRESS,
DURATION,
FINISHED,
RECORDED_DT ,
RESPONSE_ID ,
RECIPIENT_LAST_NM           ,
RECIPIENT_FIRST_NM          ,
RECIPIENT_EMAIL,
EXTERNAL_REFERENCE          ,
LOCATION_LAT,
LOCATION_LNG,
DISTRIBUTION_CHANNEL        ,
USER_LANGUAGE ,
OUTC1   ,
Q24     ,
Q26     ,
DESC2   ,
DESC2_4_TEXT,
DRIVER5 ,
DRIVER10,
NPS_NPS_GROUP ,
NPS     ,
RENEW_NPS_GROUP,
RENEW   ,
Q30     ,
Q27     ,
Q28     ,
EVENT__DT ,
ACCD_INSUR,
AAR_FAC_NM,
AAR_FAC_ID,
SC_AARCOB_IND_IC            ,
ACES_B_VEH_ID ,
ACES_VEH_ID ,
MBR_ADDR_1,
MBR_ADDR_2,
ALT_TOW_DEST,
AQS12_ELI ,
SC_OL_DTTIME,
TRK_AVL_INDIC ,
SC_BATT_REPL_INDIC          ,
SC_BAT_SVC_IND,
BATT_SVC_VEH_DISP_IND       ,
SC_BATT_TST_CD,
SC_BATT_TEST_RESULTS        ,
SC_AAIA_BATT_TYPE           ,
BIKE_TYPE ,
BL_HOME_INDIC ,
BL_LAT  ,
BL_LONG ,
BL_STATE,
CHRG_ENTITLEMENT            ,
SC_CALL_ID,
CALL_MOVER,
SC_RECV_DTTM,
SC_MBR_CB_INDIC,
CAN_SER_IND ,
CAS_CLL_IND ,
CL_PAY_DT ,
SC_CL_DTTM,
CLB_OPT_FLD_1 ,
CLB_OPT_FLD_2 ,
CLB_OPT_FLD_3 ,
CLB_OPT_FLD_4 ,
CLB_OPT_FLD_5 ,
COL_CLL ,
SC_COMM_CTR_SUB,
SC_COMM_CTR ,
MBR_COMPLAINT ,
MBR_COMPLIMENT,
PR_DOL  ,
BA_DOL  ,
PL_DOL  ,
RV_DOL  ,
OTH_DOL ,
CON_ACC_HWY ,
MBR_COUNTRY_NM,
FAC_DIR_CALL,
SC_DI_DTTIME,
DISP_UNIT_TYPE,
SC_DISPOSITION,
DRIVER_ID ,
DUP_AS_SRV_TCK,
DUP_CLL_VEH_CHG,
DUP_CALL_ID ,
DUP_SVC_CALL_DATE,
ER_MILES,
SC_ER_DTTIME,
AAR_OR_CO_FAC_INDIC,
NT_CALL_IND ,
ERS_CC_ID ,
ERS_CC_ROLE ,
ERS_REIMB ,
ERS_SER_REC ,
SC_ETA  ,
MBR_FST_NM,
GPS_IND ,
HM_LKOUT,
ITU_BBT ,
ITU_NAV ,
ITU_ODB2,
LANG_IND,
SC_LST_DI_DTTIME,
MBR_LST_NM,
MBR_NO  ,
SC_MBR_CB_PH_CL,
MBR_CTY_NM,
SC_CLUB_CD,
MBR_JOIN_DT ,
RESPONSE_REG,
MBR_ST_CD ,
MBR_MBR_TYPE,
VEH_MANF_NM ,
VEH_MDL_NM,
VEH_ODMTR_QY,
SC_VEH_TYPE ,
VEH_ID_NR ,
VEH_MANF_YR_DT,
MBR_PSTL_CD ,
SC_MILES_TW ,
SC_MOB_DIAG_CD,
TRK_MOBDIAG_INDIC,
BL_NEAR_CTY_NM,
NETWORK_PVD ,
N_MBR_IND ,
OB_BC_SCANNER ,
ODB_POS ,
OB_PRINTER,
SC_CLR_CD ,
SC_PROB_CD,
SC_REV_LS_OR_TW_CD,
SC_LS_OR_TW_CD,
SC_RES_CD ,
SC_RES_CD2,
PRIM_RPR_PERFDA,
PRIM_RPR_PERFDB,
SC_PRM_ARR_DTTIME,
RAP_IND ,
REC_IND ,
SC_RSO  ,
RPST_DT ,
RPST_TRAINED,
SDAY_SSVC ,
SC_DT   ,
SC_SVC_PROV_TYPE ,
SP_FAC_ID ,
TRK_TYPE,
SC_SVC_CLB_CD ,
TECH_ASSIST ,
SC_TW_DTTIME,
TTL_COST_ALL_REPS,
TOW_DEST_LAT,
TOW_DEST_LNG,
TOW_DEST_NEAR_CTY_NM,
TT_STATE,
TRC_CALL,
OTG_SOL_CD,
TRK_WARE,
TRK_DISP,
TRK_ID  ,
TOW_DEST_IN_RECORD,
RESPONSE_LOC_REQ,
RESPONSE_LOC_USED,
ELAPSED_TIME,
RESPONSE_REQUESTS,
TSP_REQUEST ,
IDENTIFIER,
IVR_AQS12_ELI ,
IVR_REQUESTED ,
SMS_OPTED_IN,
SMS_ELIG,
SMS_SENT,
SMS_CLICK ,
MBR_EMAIL ,
MBR_PHONE ,
MBR_PHONE_TYPE,
SMS_NOTIFY,
VIRT_STAT ,
CLUBTIER,
Q_URL   ,
SERVICE_MONTH ,
SERVICE_YEAR,
INVITE_SC_DT,
Q_SC_DT ,
--ATA,
--ETA,
--PTA,
ACCREDITATION ,
TYPE_OF_SERVICE,
BATTERY_SERVICE_TYPE,
EVENT_TYPE,
SERVICE_PROVIDER,
WEIGHTED_SEGMENT,
DIRECTIONAL_ERROR,
AGE_GROUP)

SELECT
SAFE_CAST(START_DT AS TIMESTAMP),
SAFE_CAST(END_DT  AS TIMESTAMP) ,
SAFE_CAST(STATUS AS INT64) ,
IP_ADDRESS,
SAFE_CAST(PROGRESS  AS INT64),
SAFE_CAST(DURATION  AS INT64),
SAFE_CAST(FINISHED AS INT64),
SAFE_CAST(RECORDED_DT  AS TIMESTAMP),
RESPONSE_ID ,
RECIPIENT_LAST_NM           ,
RECIPIENT_FIRST_NM          ,
RECIPIENT_EMAIL,
EXTERNAL_REFERENCE          ,
SAFE_CAST(LOCATION_LAT AS FLOAT64),
SAFE_CAST(LOCATION_LNG AS FLOAT64),
DISTRIBUTION_CHANNEL        ,
USER_LANGUAGE ,
SAFE_CAST(OUTC1  AS INT64)  ,
SAFE_CAST(Q24  AS INT64)    ,
SAFE_CAST(Q26  AS INT64)    ,
SAFE_CAST(DESC2  AS INT64)  ,
DESC2_4_TEXT,
SAFE_CAST(DRIVER5  AS INT64),
SAFE_CAST(DRIVER10  AS INT64),
SAFE_CAST(NPS_NPS_GROUP  AS INT64),
SAFE_CAST(NPS  AS INT64)     ,
SAFE_CAST(RENEW_NPS_GROUP  AS INT64),
SAFE_CAST(RENEW   AS INT64) ,
Q30     ,
SAFE_CAST(Q27  AS INT64)    ,
SAFE_CAST(Q28  AS INT64)    ,
EVENT__DT ,
ACCD_INSUR,
AAR_FAC_NM,
SAFE_CAST(AAR_FAC_ID  AS INT64),
SC_AARCOB_IND_IC            ,
ACES_B_VEH_ID ,
ACES_VEH_ID ,
MBR_ADDR_1,
MBR_ADDR_2,
ALT_TOW_DEST,
AQS12_ELI ,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_OL_DTTIME AS STRING),0,8),8,"0")) ,
TRK_AVL_INDIC ,
SC_BATT_REPL_INDIC          ,
SC_BAT_SVC_IND,
BATT_SVC_VEH_DISP_IND       ,
SC_BATT_TST_CD,
SC_BATT_TEST_RESULTS        ,
SC_AAIA_BATT_TYPE           ,
BIKE_TYPE ,
BL_HOME_INDIC ,
SAFE_CAST(BL_LAT  AS FLOAT64),
SAFE_CAST(BL_LONG AS FLOAT64),
BL_STATE,
CHRG_ENTITLEMENT            ,
SAFE_CAST(SC_CALL_ID AS INT64),
CALL_MOVER ,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_RECV_DTTM AS STRING),0,8),8,"0")),
SC_MBR_CB_INDIC,
CAN_SER_IND  ,
CAS_CLL_IND ,
CL_PAY_DT ,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_CL_DTTM AS STRING),0,8),8,"0")),
SAFE_CAST(CLB_OPT_FLD_1  AS INT64),
CLB_OPT_FLD_2 ,
CLB_OPT_FLD_3 ,
CLB_OPT_FLD_4 ,
CLB_OPT_FLD_5 ,
COL_CLL ,
SAFE_CAST(SC_COMM_CTR_SUB AS INT64),
SAFE_CAST(SC_COMM_CTR  AS INT64),
MBR_COMPLAINT ,
MBR_COMPLIMENT,
PR_DOL  ,
BA_DOL  ,
PL_DOL  ,
RV_DOL  ,
OTH_DOL ,
CON_ACC_HWY ,
MBR_COUNTRY_NM,
FAC_DIR_CALL ,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_DI_DTTIME AS STRING),0,8),8,"0")),
DISP_UNIT_TYPE,
SC_DISPOSITION,
SAFE_CAST(DRIVER_ID  AS INT64),
DUP_AS_SRV_TCK ,
DUP_CLL_VEH_CHG ,
SAFE_CAST(DUP_CALL_ID  AS INT64),
SAFE_CAST(DUP_SVC_CALL_DATE AS DATE),
SAFE_CAST(ER_MILES AS FLOAT64),
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_ER_DTTIME AS STRING),0,8),8,"0")) ,
AAR_OR_CO_FAC_INDIC,
NT_CALL_IND ,
ERS_CC_ID ,
ERS_CC_ROLE ,
ERS_REIMB ,
ERS_SER_REC ,
SAFE_CAST(SC_ETA   AS INT64),
MBR_FST_NM,
GPS_IND ,
HM_LKOUT ,
SAFE_CAST(ITU_BBT  AS INT64),
SAFE_CAST(ITU_NAV  AS INT64),
SAFE_CAST(ITU_ODB2  AS INT64),
LANG_IND,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_LST_DI_DTTIME AS STRING),0,8),8,"0")),
MBR_LST_NM,
SAFE_CAST(MBR_NO   AS INT64),
SC_MBR_CB_PH_CL,
MBR_CTY_NM,
SAFE_CAST(SC_CLUB_CD  AS INT64),
SAFE_CAST(MBR_JOIN_DT   AS INT64),
RESPONSE_REG,
MBR_ST_CD ,
MBR_MBR_TYPE,
VEH_MANF_NM ,
VEH_MDL_NM,
VEH_ODMTR_QY,
SC_VEH_TYPE ,
VEH_ID_NR ,
SAFE_CAST(VEH_MANF_YR_DT  AS INT64),
SAFE_CAST(MBR_PSTL_CD  AS INT64),
SAFE_CAST(SC_MILES_TW  AS INT64),
SC_MOB_DIAG_CD,
TRK_MOBDIAG_INDIC,
BL_NEAR_CTY_NM,
NETWORK_PVD ,
N_MBR_IND ,
OB_BC_SCANNER ,
ODB_POS ,
OB_PRINTER ,
SC_CLR_CD ,
SC_PROB_CD,
SC_REV_LS_OR_TW_CD,
SC_LS_OR_TW_CD,
SC_RES_CD ,
SC_RES_CD2,
PRIM_RPR_PERFDA,
PRIM_RPR_PERFDB,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_PRM_ARR_DTTIME AS STRING),0,8),8,"0")),
RAP_IND  ,
REC_IND  ,
SC_RSO   ,
RPST_DT ,
RPST_TRAINED  ,
SDAY_SSVC ,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_DT AS STRING),0,8),8,"0")),
SC_SVC_PROV_TYPE ,
SP_FAC_ID ,
TRK_TYPE,
SAFE_CAST(SC_SVC_CLB_CD AS INT64),
TECH_ASSIST ,
PARSE_DATE("%m%d%Y",LPAD(SUBSTR(SAFE_CAST(SC_TW_DTTIME AS STRING),0,8),8,"0")),
TTL_COST_ALL_REPS,
SAFE_CAST(TOW_DEST_LAT AS FLOAT64),
SAFE_CAST(TOW_DEST_LNG AS FLOAT64),
TOW_DEST_NEAR_CTY_NM,
TT_STATE,
TRC_CALL ,
OTG_SOL_CD,
TRK_WARE,
TRK_DISP,
TRK_ID  ,
TOW_DEST_IN_RECORD,
RESPONSE_LOC_REQ,
RESPONSE_LOC_USED,
ELAPSED_TIME,
RESPONSE_REQUESTS,
TSP_REQUEST ,
IDENTIFIER,
IVR_AQS12_ELI ,
IVR_REQUESTED ,
SMS_OPTED_IN ,
SMS_ELIG ,
SMS_SENT,
SMS_CLICK ,
MBR_EMAIL ,
SAFE_CAST(MBR_PHONE  AS INT64),
MBR_PHONE_TYPE,
SMS_NOTIFY,
VIRT_STAT ,
CLUBTIER,
Q_URL   ,
SERVICE_MONTH ,
SERVICE_YEAR,
SAFE_CAST(INVITE_SC_DT AS DATE),
SAFE_CAST(Q_SC_DT AS TIMESTAMP),
--ATA,
--ETA,
--PTA,
ACCREDITATION ,
TYPE_OF_SERVICE,
BATTERY_SERVICE_TYPE,
EVENT_TYPE,
SERVICE_PROVIDER,
WEIGHTED_SEGMENT,
DIRECTIONAL_ERROR ,
AGE_GROUP
FROM LANDING.WORK_SURVEY_ERS
