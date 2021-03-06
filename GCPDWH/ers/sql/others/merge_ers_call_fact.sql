MERGE INTO CUSTOMER_PRODUCT.ERS_CALL_FACT A
USING
(
WITH CALL_DATA AS (
SELECT 
 AC.SC_ID AS SC_ID
,AC.SC_DT AS SC_DT
,AC.COMM_CTR_ID AS COMM_CENTER_ID
,AC.SC_CALL_CLB_CD AS SC_CALL_CLUB_CD
,AC.call_source AS CALL_SOURCE
,AC.SC_CALL_MBR_ID AS SC_CALL_MEMBERSHIP_NUM
,AC.SC_CALL_ASC_ID AS SC_CALL_ASSOCIATE_ID
,M.MEMBER_NUM AS MEMBER_NUM
,M.CUSTOMER_MDM_KEY AS CUSTOMER_MDM_KEY
,ACE.SVC_FACL_ID AS SC_FACILITY_ID
,CASE WHEN Ftrk.sc_trk_id is null then SC_A_SVC_TRK_ID ELSE Ftrk.sc_trk_id END AS SC_TRUCK_ID
,AC.STATUS_CD AS STATUS_CD
,AC.SC_STS_RSN_CD AS SC_STATUS_REASON_CD
,AC.SC_A_SVC_TRK_D_ID AS TRUCK_DRIVER_ID
,REPLACE(ACE.DRIVER_NAME,',',' ') AS DRIVER_NM
,ED.EMPLOYEE_ID 
,ED.LEGAL_FIRST_NM AS EMPLOYEE_FIRST_NM
,ED.LEGAL_LAST_NM AS EMPLOYEE_LAST_NM
,ED.LOCATION_ID AS EMPLOYEE_LOCATION_ID
,ED.SUPERVISOR_EMPLOYEE_ID AS SUPERVISOR_EMPLOYEE_ID
,ACE.BL_LAT AS BREAKDOWN_LOCATION_LATITUDE
,ACE.BL_LONG AS BREAKDOWN_LOCATION_LONGITUDE
,ACE.BL_N_LNDMRK_TX AS BREAKDOWN_LOCATION_ADDRESS
,AC.bl_typ_cd AS BREAKDOWN_LOCATION_TYPE_CD
,FRE.FSTRE_STS_TM AS RECEIVED_STATUS_TIME
,FSTDI_STS_TM AS DISPATCH_STATUS_TIME
,FSTER_STS_TM AS ENROUTE_STATUS_TIME
,FSTOL_STS_TM AS ON_LOCATION_STATUS_TIME
,fstcl_STS_TM AS CLOSED_STATUS_TIME
,AC.sc_prms_arr_tm AS PROMISSED_ETA_DTTIME
,ACE.AVL_ETA AS AUTO_DISPATCH_ETA
,AC.SC_WAIT_TM SC_WAIT_TIME
,ACE.AWD_VEHICLE AS ALL_WHEEL_DRIVE_VEHICLE_IND
,ACE.FOUR_WHEEL_DRIVE AS FOUR_WHEEL_DRIVE_VEHICLE_IND
,AC.sc_cntc_fst_nm AS SC_PRIMARY_CONTACT_FIRST_NAME
,AC.sc_cntc_lst_nm  AS SC_PRIMARY_CONTACT_LAST_NAME
,AC.sc_prty_cd AS SC_PRIORITY_CD
,ACE.FLATBED_INDIC AS FLATBED_REQUIRED_IND
,AC.DTL_PROB1_CD  AS SERVICE_TYPE_CD
,AC.DTL_PROB2_CD AS SERVICE_TYPE_CD_2
,AC.DTL_STS_RSN_CD  AS SERVICE_DETAIL_REASON_CD
,AC.PROB1_CD  AS SERVICE_PROBLEM_CD
,ACE.TD_LAT AS TOW_DESTINATION_LATITUDE
,ACE.TD_LONG AS TOW_DESTINATION_LONGITUDE
,CASE WHEN ACC.SC_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS TOW_DESTINATION_CHANGE_IND
,CASE WHEN SP.SC_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS BASIC_PLUSRV_COST_IND
,ACE.TOW_DEST_ID AS TOW_DESTINATION_ID
,TDL.TD_NAME AS TOW_DESTINATION_NM
,TDL.TD_STATE AS TOW_DESTINATION_STATE_CD
,SC_VEH_MANF_YR_DT AS SC_VEHICLE_MAKE_YEAR
,SC_VEH_MANFR_NM AS SC_VEHICLE_MAKE_NAME
,SC_VEH_MDL_NM AS SC_VEHICLE_MODLE_NAME
,SC_VEH_COLR_NM AS SC_VEHICLE_COLOR_NAME
,SC_VEH_TYPE AS SC_VEHICLE_TYPE
,ACE.RV_TOWING 
,AC.CALL_COST AS TOTAL_CALL_COST
,0.001 as CALL_COST
,CASE WHEN AC.STATUS_CD = 'RE' THEN 1  
      WHEN AC.STATUS_CD = 'SP' THEN 2
      WHEN AC.STATUS_CD = 'AS' THEN 3
      WHEN AC.STATUS_CD = 'DI' THEN 4
      WHEN AC.STATUS_CD = 'ER' THEN 5
      WHEN AC.STATUS_CD = 'OL' THEN 6
      WHEN AC.STATUS_CD = 'TW' THEN 7
      WHEN AC.STATUS_CD = 'CL' THEN 8
      ELSE NULL END AS STATUS_RANK
,CASE WHEN FRE.EMPLE_ID = 'RAP_SRVR' THEN 'MOBILE APP'
     WHEN FRE.EMPLE_ID = 'ICIVR' THEN 'ERSA'
     WHEN FRE.EMPLE_ID = 'FUSION' THEN 'APP/WEB PORTAL'
     ELSE 'CALL RECEIVER' END AS CALL_RECEIVE_CHANNEL
,ACE.SMS_OPT_IN	
FROM (SELECT * FROM OPERATIONAL.ERS_STAGE_ARCH_CALL  WHERE ARCH_DATETIME > '2020-01-10T00:00:00' /* v_incr_date */
     ) AC
INNER JOIN OPERATIONAL.ERS_STAGE_ARCH_CALL_EXTD ACE ON  AC.SC_ID = ACE.SC_ID AND AC.SC_DT = ACE.SC_DT AND AC.COMM_CTR_ID = ACE.COMM_CTR_ID
LEFT JOIN CUSTOMERS.CONNECTSUITE_MEMBER M ON AC.SC_CALL_MBR_ID = M.MEMBERSHIP_NUM AND AC.SC_CALL_ASC_ID = SAFE_CAST(M.ASSOCIATE_ID AS STRING)
LEFT JOIN (SELECT * from (select RE.comm_ctr_id, RE.sc_dt, RE.sc_id, RE.SC_STS_TM as FSTRE_STS_TM, EMPLE_ID, ROW_NUMBER()
                OVER (PARTITION BY RE.comm_ctr_id, RE.sc_dt, RE.sc_id,  RE.sc_sts_cd, RE.sc_sts_upd_cd  ORDER BY RE.SC_STS_TM) AS rownumber
                from OPERATIONAL.ERS_STAGE_SC_STATUS RE
                WHERE RE.SC_STS_CD='RE'
                AND RE.SC_STS_UPD_CD = 'RE'
                AND RE.sc_dt >  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 90 DAY))
                where rownumber = 1) FRE ON AC.COMM_CTR_ID = FRE.COMM_CTR_ID AND AC.SC_ID = FRE.SC_ID AND AC.SC_DT = FRE.SC_DT
LEFT JOIN (SELECT * from (select di.comm_ctr_id, di.sc_dt, di.sc_id, di.SC_STS_TM as FSTDI_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY di.comm_ctr_id, di.sc_dt, di.sc_id,  di.sc_sts_cd, di.sc_sts_upd_cd  ORDER BY di.SC_STS_TM) AS rownumber
                from OPERATIONAL.ERS_STAGE_SC_STATUS di
                WHERE di.SC_STS_CD='DI'
                AND di.SC_STS_UPD_CD = 'DI'
                AND di.sc_dt >  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 90 DAY))
                where rownumber = 1) FDI ON AC.COMM_CTR_ID = FDI.COMM_CTR_ID AND AC.SC_ID = FDI.SC_ID AND AC.SC_DT = FDI.SC_DT
LEFT JOIN (SELECT * from (select ER.comm_ctr_id, ER.sc_dt, ER.sc_id, ER.SC_STS_TM as FSTER_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY ER.comm_ctr_id, ER.sc_dt, ER.sc_id,  ER.sc_sts_cd, ER.sc_sts_upd_cd  ORDER BY ER.SC_STS_TM) AS rownumber
                from OPERATIONAL.ERS_STAGE_SC_STATUS ER
                WHERE ER.SC_STS_CD='ER'
                AND ER.SC_STS_UPD_CD = 'ER'
                AND ER.sc_dt >  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 90 DAY))
                where rownumber = 1) FER ON AC.COMM_CTR_ID = FER.COMM_CTR_ID AND AC.SC_ID = FER.SC_ID AND AC.SC_DT = FER.SC_DT
LEFT JOIN (SELECT * from (select cl.comm_ctr_id, cl.sc_dt, cl.sc_id,  cl.SC_STS_TM as fstcl_STS_TM,  ROW_NUMBER()
                OVER (PARTITION BY cl.comm_ctr_id, cl.sc_dt, cl.sc_id  ORDER BY cl.SC_STS_TM) AS rownumber
                from OPERATIONAL.ERS_STAGE_SC_STATUS cl
                WHERE cl.SC_STS_CD='CL'
                AND cl.SC_STS_UPD_CD = 'CL'
                AND cl.SC_DT >  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 90 DAY))
                where rownumber = 1) FCL ON AC.COMM_CTR_ID = FCL.COMM_CTR_ID AND AC.SC_ID = FCL.SC_ID AND AC.SC_DT = FCL.SC_DT
LEFT JOIN (SELECT * from (select ol.comm_ctr_id, ol.sc_dt, ol.sc_id,   ol.SC_STS_TM as FSTOL_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY ol.comm_ctr_id, ol.sc_dt, ol.sc_id ORDER BY ol.SC_STS_TM) AS rownumber
                from OPERATIONAL.ERS_STAGE_SC_STATUS ol
                WHERE ol.SC_STS_CD='OL'
                AND ol.SC_STS_UPD_CD = 'OL'
                AND ol.SC_DT > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 90 DAY))
                where rownumber = 1) FOL ON AC.COMM_CTR_ID = FOL.COMM_CTR_ID AND AC.SC_ID = FOL.SC_ID AND AC.SC_DT = FOL.SC_DT
LEFT JOIN (SELECT * from (select trk.comm_ctr_id, trk.sc_dt, trk.sc_id, sc_trk_id,  trk.SC_STS_TM as fsttrk_STS_TM,  ROW_NUMBER()
                OVER (PARTITION BY trk.comm_ctr_id, trk.sc_dt, trk.sc_id  ORDER BY trk.SC_STS_TM DESC) AS rownumber
                from OPERATIONAL.ERS_STAGE_SC_STATUS trk
                WHERE trk.SC_DT >  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 90 DAY)
                and sc_trk_id is not null)
                 where rownumber = 1 )  Ftrk ON AC.COMM_CTR_ID = Ftrk.COMM_CTR_ID AND AC.SC_ID = Ftrk.SC_ID AND DATE(AC.SC_DT) = DATE(Ftrk.SC_DT)
LEFT OUTER JOIN (SELECT DISTINCT SC_ID, SC_DT, COMM_CTR_ID FROM OPERATIONAL.ERS_STAGE_SC_CALL_COMMENT 
                 WHERE LOWER(SC_COMM_TX) LIKE '%tow destination info changed from:%') ACC ON AC.COMM_CTR_ID = ACC.COMM_CTR_ID AND AC.SC_DT = ACC.SC_DT AND AC.SC_ID = ACC.SC_ID 
LEFT OUTER JOIN OPERATIONAL.ERS_STAGE_TOW_DEST_LIST TDL ON ACE.TOW_DEST_ID = TDL.TD_ID
LEFT OUTER JOIN REFERENCE.ERS_STAGE_CAD_EMPLOYEE SCE ON FRE.EMPLE_ID = SCE.EMPLE_ID
LEFT OUTER JOIN CUSTOMER_PRODUCT.EMPLOYEE_DIM ED ON SCE.EMAIL = ED.EMPLOYEE_ID AND DATE(AC.SC_DT) BETWEEN ED.ROW_START_DT AND ED.ROW_END_DT
LEFT OUTER JOIN (SELECT DISTINCT COMM_CTR_ID, SC_DT, SC_ID FROM  OPERATIONAL.ERS_STAGE_SERVICE_PROVIDED
                 WHERE DELETE_FLG = 'N' AND  (SVCP_BASIC_COST <> 0 OR SVCP_PLUSRV_COST <> 0)) SP ON AC.COMM_CTR_ID = SP.COMM_CTR_ID AND AC.SC_ID = SP.SC_ID AND AC.SC_DT = SP.SC_DT

),
MESSAGES AS (SELECT MESSAGE_ID, SC_ID , SC_DT , MEMBER_NUM FROM 
            (SELECT ID.MESSAGE_ID, SC_ID,  DATETIME(SC_DT) AS SC_DT , MEMBER_NUM, ROW_NUMBER() OVER MSGWINDOW AS  ROW_RANK
             FROM (SELECT  MESSAGE_ID, SAFE_CAST(VALUE AS INT64) AS SC_ID FROM  LANDING.CUSTOMER_SMS_TAGS
                   WHERE NAME = 'serviceCallNumber') ID
              INNER JOIN (SELECT MESSAGE_ID, PARSE_DATE('%F', TRIM(VALUE)) AS SC_DT FROM  LANDING.CUSTOMER_SMS_TAGS
                   WHERE NAME = 'serviceCallDate') DT ON DT.MESSAGE_ID = ID.MESSAGE_ID
              LEFT JOIN (SELECT  VALUE  AS MEMBER_NUM,  MESSAGE_ID FROM  LANDING.CUSTOMER_SMS_TAGS
                   WHERE NAME = 'membershipNumber') M ON M.MESSAGE_ID = ID.MESSAGE_ID
                WINDOW MSGWINDOW AS (PARTITION BY SC_ID,  DATETIME(SC_DT)) ) WHERE ROW_RANK = 1   
                ),
ETA_STATUS_CHECKS AS (SELECT SC_ID,  SC_DT , MEMBER_NUM,  count(DISTINCT B.MESSAGE_ID) AS AAA_OTW_SMS_NO_OF_ETA_STATUS_CHECKS 
                      FROM  MESSAGES A, LANDING.CUSTOMER_SMS B
                WHERE A.MESSAGE_ID = B.MESSAGE_ID
                AND UPPER(MESSAGE_DIRECTION) = 'INCOMING'
                and (TRIM(UPPER(MESSAGE_BODY)) = 'STATUS.' OR  TRIM(UPPER(MESSAGE_BODY)) = 'STATUS') 
                GROUP BY  SC_ID,  SC_DT , MEMBER_NUM
                ),                
MOBILE_NUM AS ( SELECT * FROM (
                SELECT SC_ID,  SC_DT , MEMBER_NUM,  TO_PHONE_NUM AS MOBILE_NUM,
                CASE WHEN SSU.MESSAGE_ID IS NOT NULL THEN 'DELIVERED' ELSE 'UNDELIVERED' END AS DELIVERY_STATUS,
                ROW_NUMBER() OVER MSGWINDOW AS  ROW_RANK,
                FROM  MESSAGES A
                INNER JOIN LANDING.CUSTOMER_SMS B ON A.MESSAGE_ID = B.MESSAGE_ID
                LEFT JOIN LANDING.CUSTOMER_SMS_STATUS_UPDATES SSU ON A.MESSAGE_ID = SSU.MESSAGE_ID AND TRIM(UPPER(SSU.STATUS)) = 'DELIVERED' 
                WHERE UPPER(B.MESSAGE_DIRECTION) = 'OUTGOING'                
                WINDOW MSGWINDOW AS (PARTITION BY SC_ID,  SC_DT ORDER BY STATUS_UPDATE_DT ASC) ) WHERE ROW_RANK = 1                
                ),
CANCEL_SERVICE_IND AS (SELECT SC_ID,  SC_DT , MEMBER_NUM,  'Y' AS CANCEL_SERVICE_IND 
                FROM  MESSAGES A, LANDING.CUSTOMER_SMS B
                WHERE A.MESSAGE_ID = B.MESSAGE_ID
                AND UPPER(MESSAGE_DIRECTION) = 'OUTGOING'
                AND trim(lower(MESSAGE_BODY)) LIKE '%has been cancelled. thanks for being%'
                ),
POSITIVE_FEEDBACK_IND AS (SELECT SC_ID,  SC_DT , MEMBER_NUM,  'Y' AS POSITIVE_FEEDBACK_IND 
                         FROM  MESSAGES A, LANDING.CUSTOMER_SMS B
                WHERE A.MESSAGE_ID = B.MESSAGE_ID
                AND UPPER(B.MESSAGE_DIRECTION) = 'INCOMING'
                AND ( trim(lower(MESSAGE_BODY)) Like '%love%' OR trim(lower(MESSAGE_BODY)) like '%thank%' OR trim(lower(MESSAGE_BODY)) like '%success%' OR trim(lower(MESSAGE_BODY)) like '%great%'  )
                ),
NEGETIVE_FEEDBACK_IND AS (SELECT SC_ID,  SC_DT , MEMBER_NUM,  'Y' AS NEGETIVE_FEEDBACK_IND 
                         FROM  MESSAGES A, LANDING.CUSTOMER_SMS B
                WHERE A.MESSAGE_ID = B.MESSAGE_ID
                AND UPPER(B.MESSAGE_DIRECTION) = 'INCOMING'
                AND ( trim(lower(MESSAGE_BODY)) Like '%fuck%' OR trim(lower(MESSAGE_BODY)) like '%suck%' OR trim(lower(MESSAGE_BODY)) like '%not very good%' OR trim(lower(MESSAGE_BODY)) like '%not good%'  )
                )
SELECT  CD.*
,M.MOBILE_NUM AS AAA_OTW_MOBILE_NUM
,M.DELIVERY_STATUS AS AAA_OTW_SMS_DELIVERY_STATUS
,ESC.AAA_OTW_SMS_NO_OF_ETA_STATUS_CHECKS AS AAA_OTW_SMS_NO_OF_ETA_STATUS_CHECKS
,C.CANCEL_SERVICE_IND AS AAA_OTW_SMS_CANCEL_SERVICE_IND
,P.POSITIVE_FEEDBACK_IND AS AAA_OTW_SMS_POSITIVE_FEEDBACK_IND
,N.NEGETIVE_FEEDBACK_IND AS AAA_OTW_SMS_NEGETIVE_FEEDBACK_IND
,QS.OUTC1 AS QUALTRICS_ERS_SATISFACTION_SCORE
,QS.RECIPIENT_EMAIL AS QUALTRICS_EMAIL_ADDRESS 
,CAST(NULL AS INT64) AS JOB_RUN_ID
,CAST(NULL AS STRING) AS SOURCE_SYSTEM_CD
,CAST(NULL AS DATETIME) AS CREATE_DTTIME
FROM CALL_DATA CD
LEFT JOIN ETA_STATUS_CHECKS ESC ON CD.SC_ID = ESC.SC_ID AND CD.SC_DT = ESC.SC_DT
LEFT JOIN MOBILE_NUM M ON CD.SC_ID = M.SC_ID AND CD.SC_DT = M.SC_DT
LEFT JOIN CANCEL_SERVICE_IND C ON CD.SC_ID = C.SC_ID AND CD.SC_DT = C.SC_DT
LEFT JOIN POSITIVE_FEEDBACK_IND P ON CD.SC_ID = P.SC_ID AND CD.SC_DT = P.SC_DT
LEFT JOIN NEGETIVE_FEEDBACK_IND N ON CD.SC_ID = N.SC_ID AND CD.SC_DT = N.SC_DT
LEFT JOIN (SELECT DISTINCT SC_CALL_ID AS SC_ID, SC_RECV_DTTM AS SC_DT ,MAX(RECIPIENT_EMAIL) AS RECIPIENT_EMAIL  , max(OUTC1 ) AS OUTC1
           FROM `aaa-mwg-dwprod.OPERATIONAL.QUALTRICS_SURVEY_RESPONSE_ERS` WHERE SC_CALL_ID IS NOT NULL GROUP BY SC_CALL_ID, SC_RECV_DTTM ) QS ON CD.SC_ID = QS.SC_ID AND CD.SC_DT = DATETIME(QS.SC_DT)				 	 
				 
) TBL
ON A.SC_ID = TBL.SC_ID
AND A.SC_DT = TBL.SC_DT
AND A.COMM_CENTER_ID=TBL.COMM_CENTER_ID
 WHEN MATCHED THEN UPDATE SET
 SC_ID = TBL.SC_ID,	
SC_DT = TBL.SC_DT,
COMM_CENTER_ID = TBL.COMM_CENTER_ID,
SC_CALL_CLUB_CD = TBL.SC_CALL_CLUB_CD,
CALL_SOURCE = TBL.CALL_SOURCE,
SC_CALL_MEMBERSHIP_NUM = TBL.SC_CALL_MEMBERSHIP_NUM,
SC_CALL_ASSOCIATE_ID =  TBL.SC_CALL_ASSOCIATE_ID,
MEMBER_NUM = TBL.MEMBER_NUM,
CUSTOMER_MDM_KEY = TBL.CUSTOMER_MDM_KEY,
SC_FACILITY_ID = TBL.SC_FACILITY_ID,
SC_TRUCK_ID = TBL.SC_TRUCK_ID,
STATUS_CD = TBL.STATUS_CD,
SC_STATUS_REASON_CD = TBL.SC_STATUS_REASON_CD,
TRUCK_DRIVER_ID = TBL.TRUCK_DRIVER_ID,
DRIVER_NM = TBL.DRIVER_NM,
EMPLOYEE_ID = TBL.EMPLOYEE_ID,
EMPLOYEE_FIRST_NM = TBL.EMPLOYEE_FIRST_NM,
EMPLOYEE_LAST_NM = TBL.EMPLOYEE_LAST_NM,
EMPLOYEE_LOCATION_ID = TBL.EMPLOYEE_LOCATION_ID,
SUPERVISOR_EMPLOYEE_ID = TBL.SUPERVISOR_EMPLOYEE_ID,
BREAKDOWN_LOCATION_LATITUDE = TBL.BREAKDOWN_LOCATION_LATITUDE,
BREAKDOWN_LOCATION_LONGITUDE = TBL.BREAKDOWN_LOCATION_LONGITUDE,
BREAKDOWN_LOCATION_ADDRESS = TBL.BREAKDOWN_LOCATION_ADDRESS,
BREAKDOWN_LOCATION_TYPE_CD = TBL.BREAKDOWN_LOCATION_TYPE_CD,
RECEIVED_STATUS_TIME = TBL.RECEIVED_STATUS_TIME,
DISPATCH_STATUS_TIME = TBL.DISPATCH_STATUS_TIME,
ENROUTE_STATUS_TIME = TBL.ENROUTE_STATUS_TIME,
ON_LOCATION_STATUS_TIME = TBL.ON_LOCATION_STATUS_TIME,
CLOSED_STATUS_TIME = TBL.CLOSED_STATUS_TIME,
PROMISSED_ETA_DTTIME = TBL.PROMISSED_ETA_DTTIME,
AUTO_DISPATCH_ETA = TBL.AUTO_DISPATCH_ETA	,
SC_WAIT_TIME = TBL.SC_WAIT_TIME	,
ALL_WHEEL_DRIVE_VEHICLE_IND = TBL.ALL_WHEEL_DRIVE_VEHICLE_IND,
FOUR_WHEEL_DRIVE_VEHICLE_IND = TBL.FOUR_WHEEL_DRIVE_VEHICLE_IND,
SC_PRIMARY_CONTACT_FIRST_NAME = TBL.SC_PRIMARY_CONTACT_FIRST_NAME,
SC_PRIMARY_CONTACT_LAST_NAME = TBL.SC_PRIMARY_CONTACT_LAST_NAME,
SC_PRIORITY_CD = TBL.SC_PRIORITY_CD,
FLATBED_REQUIRED_IND = TBL.FLATBED_REQUIRED_IND,
SERVICE_TYPE_CD = TBL.SERVICE_TYPE_CD,
SERVICE_TYPE_CD_2 = TBL.SERVICE_TYPE_CD_2,
SERVICE_DETAIL_REASON_CD = TBL.SERVICE_DETAIL_REASON_CD,
SERVICE_PROBLEM_CD = TBL.SERVICE_PROBLEM_CD,
TOW_DESTINATION_LATITUDE = TBL.TOW_DESTINATION_LATITUDE,
TOW_DESTINATION_LONGITUDE = TBL.TOW_DESTINATION_LONGITUDE,
TOW_DESTINATION_CHANGE_IND = TBL.TOW_DESTINATION_CHANGE_IND,
BASIC_PLUSRV_COST_IND = TBL.BASIC_PLUSRV_COST_IND,
TOW_DESTINATION_ID = TBL.TOW_DESTINATION_ID,
TOW_DESTINATION_NM = TBL.TOW_DESTINATION_NM,
TOW_DESTINATION_STATE_CD = TBL.TOW_DESTINATION_STATE_CD,
SC_VEHICLE_MAKE_YEAR = TBL.SC_VEHICLE_MAKE_YEAR,
SC_VEHICLE_MAKE_NAME = TBL.SC_VEHICLE_MAKE_NAME,
SC_VEHICLE_MODLE_NAME = TBL.SC_VEHICLE_MODLE_NAME,
SC_VEHICLE_COLOR_NAME = TBL.SC_VEHICLE_COLOR_NAME,
SC_VEHICLE_TYPE = TBL.SC_VEHICLE_TYPE,
RV_TOWING = TBL.RV_TOWING,
TOTAL_CALL_COST = TBL.TOTAL_CALL_COST,
CALL_COST = TBL.CALL_COST,
STATUS_RANK = TBL.STATUS_RANK,
CALL_RECEIVE_CHANNEL = TBL.CALL_RECEIVE_CHANNEL,
SMS_OPT_IN = TBL.SMS_OPT_IN,
AAA_OTW_MOBILE_NUM = TBL.AAA_OTW_MOBILE_NUM,	
AAA_OTW_SMS_DELIVERY_STATUS = TBL.AAA_OTW_SMS_DELIVERY_STATUS,	
AAA_OTW_SMS_NO_OF_ETA_STATUS_CHECKS = TBL.AAA_OTW_SMS_NO_OF_ETA_STATUS_CHECKS,	
AAA_OTW_SMS_CANCEL_SERVICE_IND = TBL.AAA_OTW_SMS_CANCEL_SERVICE_IND,	
AAA_OTW_SMS_POSITIVE_FEEDBACK_IND = TBL.AAA_OTW_SMS_POSITIVE_FEEDBACK_IND,	
AAA_OTW_SMS_NEGETIVE_FEEDBACK_IND = TBL.AAA_OTW_SMS_NEGETIVE_FEEDBACK_IND,	
QUALTRICS_ERS_SATISFACTION_SCORE = TBL.QUALTRICS_ERS_SATISFACTION_SCORE,	
QUALTRICS_EMAIL_ADDRESS = TBL.QUALTRICS_EMAIL_ADDRESS,	
JOB_RUN_ID = 'v_job_run_id',
SOURCE_SYSTEM_CD = 'ERS'
WHEN NOT MATCHED THEN INSERT 
(
 SC_ID,
SC_DT,
COMM_CENTER_ID,
SC_CALL_CLUB_CD,
CALL_SOURCE,
SC_CALL_MEMBERSHIP_NUM,
SC_CALL_ASSOCIATE_ID,
MEMBER_NUM,
CUSTOMER_MDM_KEY,
SC_FACILITY_ID,
SC_TRUCK_ID,
STATUS_CD,
SC_STATUS_REASON_CD,
TRUCK_DRIVER_ID,
DRIVER_NM,
EMPLOYEE_ID,
EMPLOYEE_FIRST_NM,
EMPLOYEE_LAST_NM,
EMPLOYEE_LOCATION_ID,
SUPERVISOR_EMPLOYEE_ID,
BREAKDOWN_LOCATION_LATITUDE,
BREAKDOWN_LOCATION_LONGITUDE,
BREAKDOWN_LOCATION_ADDRESS,
BREAKDOWN_LOCATION_TYPE_CD,
RECEIVED_STATUS_TIME,
DISPATCH_STATUS_TIME,
ENROUTE_STATUS_TIME,
ON_LOCATION_STATUS_TIME,
CLOSED_STATUS_TIME,
PROMISSED_ETA_DTTIME,
AUTO_DISPATCH_ETA,
SC_WAIT_TIME,
ALL_WHEEL_DRIVE_VEHICLE_IND,
FOUR_WHEEL_DRIVE_VEHICLE_IND,
SC_PRIMARY_CONTACT_FIRST_NAME,
SC_PRIMARY_CONTACT_LAST_NAME,
SC_PRIORITY_CD,
FLATBED_REQUIRED_IND,
SERVICE_TYPE_CD,
SERVICE_TYPE_CD_2,
SERVICE_DETAIL_REASON_CD,
SERVICE_PROBLEM_CD,
TOW_DESTINATION_LATITUDE,
TOW_DESTINATION_LONGITUDE,
TOW_DESTINATION_CHANGE_IND,
BASIC_PLUSRV_COST_IND,
TOW_DESTINATION_ID,
TOW_DESTINATION_NM,
TOW_DESTINATION_STATE_CD,
SC_VEHICLE_MAKE_YEAR,
SC_VEHICLE_MAKE_NAME,
SC_VEHICLE_MODLE_NAME,
SC_VEHICLE_COLOR_NAME,
SC_VEHICLE_TYPE,
RV_TOWING,
TOTAL_CALL_COST,
CALL_COST,
STATUS_RANK,
CALL_RECEIVE_CHANNEL,
SMS_OPT_IN,
AAA_OTW_MOBILE_NUM,
AAA_OTW_SMS_DELIVERY_STATUS,
AAA_OTW_SMS_NO_OF_ETA_STATUS_CHECKS,
AAA_OTW_SMS_CANCEL_SERVICE_IND,
AAA_OTW_SMS_POSITIVE_FEEDBACK_IND,
AAA_OTW_SMS_NEGETIVE_FEEDBACK_IND,
QUALTRICS_ERS_SATISFACTION_SCORE,
QUALTRICS_EMAIL_ADDRESS,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DTTIME
)
VALUES
(
TBL.SC_ID,
TBL.SC_DT,
TBL.COMM_CENTER_ID,
TBL.SC_CALL_CLUB_CD,
TBL.CALL_SOURCE,
TBL.SC_CALL_MEMBERSHIP_NUM,
TBL.SC_CALL_ASSOCIATE_ID,
TBL.MEMBER_NUM,
TBL.CUSTOMER_MDM_KEY,
TBL.SC_FACILITY_ID,
TBL.SC_TRUCK_ID,
TBL.STATUS_CD,
TBL.SC_STATUS_REASON_CD,
TBL.TRUCK_DRIVER_ID,
TBL.DRIVER_NM,
TBL.EMPLOYEE_ID,
TBL.EMPLOYEE_FIRST_NM,
TBL.EMPLOYEE_LAST_NM,
TBL.EMPLOYEE_LOCATION_ID,
TBL.SUPERVISOR_EMPLOYEE_ID,
TBL.BREAKDOWN_LOCATION_LATITUDE,
TBL.BREAKDOWN_LOCATION_LONGITUDE,
TBL.BREAKDOWN_LOCATION_ADDRESS,
TBL.BREAKDOWN_LOCATION_TYPE_CD,
TBL.RECEIVED_STATUS_TIME,
TBL.DISPATCH_STATUS_TIME,
TBL.ENROUTE_STATUS_TIME,
TBL.ON_LOCATION_STATUS_TIME,
TBL.CLOSED_STATUS_TIME,
TBL.PROMISSED_ETA_DTTIME,
TBL.AUTO_DISPATCH_ETA,
TBL.SC_WAIT_TIME,
TBL.ALL_WHEEL_DRIVE_VEHICLE_IND,
TBL.FOUR_WHEEL_DRIVE_VEHICLE_IND,
TBL.SC_PRIMARY_CONTACT_FIRST_NAME,
TBL.SC_PRIMARY_CONTACT_LAST_NAME,
TBL.SC_PRIORITY_CD,
TBL.FLATBED_REQUIRED_IND,
TBL.SERVICE_TYPE_CD,
TBL.SERVICE_TYPE_CD_2,
TBL.SERVICE_DETAIL_REASON_CD,
TBL.SERVICE_PROBLEM_CD,
TBL.TOW_DESTINATION_LATITUDE,
TBL.TOW_DESTINATION_LONGITUDE,
TBL.TOW_DESTINATION_CHANGE_IND,
TBL.BASIC_PLUSRV_COST_IND,
TBL.TOW_DESTINATION_ID,
TBL.TOW_DESTINATION_NM,
TBL.TOW_DESTINATION_STATE_CD,
TBL.SC_VEHICLE_MAKE_YEAR,
TBL.SC_VEHICLE_MAKE_NAME,
TBL.SC_VEHICLE_MODLE_NAME,
TBL.SC_VEHICLE_COLOR_NAME,
TBL.SC_VEHICLE_TYPE,
TBL.RV_TOWING,
TBL.TOTAL_CALL_COST,
TBL.CALL_COST,
TBL.STATUS_RANK,
TBL.CALL_RECEIVE_CHANNEL,
TBL.SMS_OPT_IN,
TBL.AAA_OTW_MOBILE_NUM,
TBL.AAA_OTW_SMS_DELIVERY_STATUS,
TBL.AAA_OTW_SMS_NO_OF_ETA_STATUS_CHECKS,
TBL.AAA_OTW_SMS_CANCEL_SERVICE_IND,
TBL.AAA_OTW_SMS_POSITIVE_FEEDBACK_IND,
TBL.AAA_OTW_SMS_NEGETIVE_FEEDBACK_IND,
TBL.QUALTRICS_ERS_SATISFACTION_SCORE,
TBL.QUALTRICS_EMAIL_ADDRESS,
'v_job_run_id',
'ERS',
CURRENT_DATETIME()
)