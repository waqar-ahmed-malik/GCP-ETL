select SC.SC_ID AS SC_ID
,SC.SC_DT AS SC_DT
,'005' AS COMM_CENTER_ID
,SC.SC_CALL_CLB_CD AS SC_CALL_CLUB_CD
,CASE WHEN SC.EDS_INDIC = 'Y' THEN 'EDS' ELSE 'ERS' END AS CALL_SOURCE
,SC_CALL_ID_DA AS MEMBERSHIP_NUM
,CASE WHEN SC.SC_FACLTRK IS NULL THEN FER.sc_facl_id ELSE SUBSTR(SC.SC_FACLTRK,1, instr(SC.SC_FACLTRK,'/') -1) END AS SC_FACILITY_ID
,CASE WHEN instr(SC.SC_FACLTRK,'/') = 0 THEN FER.sc_trk_id ELSE SUBSTR(SC.SC_FACLTRK, instr(SC.SC_FACLTRK,'/') +1) END AS SC_TRUCK_ID
,SC.STATUS_CD AS STATUS_CD
,NULL AS SC_STATUS_REASON_CD
,NULL AS TRUCK_DRIVER_ID
,BL.BL_LAT AS BREAKDOWN_LOCATION_LATITUDE
,BL.BL_LONG AS BREAKDOWN_LOCATION_LONGITUDE
,BL.BL_N_LNDMRK_TX AS BREAKDOWN_LOCATION_ADDRESS
,BL.bl_typ_cd AS BREAKDOWN_LOCATION_TYPE_CD
,FRE.FSTRE_STS_TM AS RECEIVED_STATUS_TIME
,FSTDI_STS_TM AS DISPATCH_STATUS_TIME
,FER.FSTER_STS_TM AS ENROUTE_STATUS_TIME
,FSTOL_STS_TM AS ON_LOCATION_STATUS_TIME
,fstcl_STS_TM AS CLOSED_STATUS_TIME
,sc_prms_arr_tm AS PROMISSED_ETA_DTTIME
,SCE.AVL_ETA AS AUTO_DISPATCH_ETA
,SC.SC_WAIT_TM SC_WAIT_TIME
,SV.awd_vehicle AS ALL_WHEEL_DRIVE_VEHICLE_IND
,SV.four_wheel_drive AS FOUR_WHEEL_DRIVE_VEHICLE_IND
,SC.sc_cntc_fst_nm AS SC_PRIMARY_CONTACT_FIRST_NAME
,SC.sc_cntc_lst_nm  AS SC_PRIMARY_CONTACT_LAST_NAME
,SC.sc_prty_cd AS SC_PRIORITY_CD
,SCE.flatbed_req_indic AS FLATBED_REQUIRED_IND
,SCE.sc_tlc_prob1_cd_orig AS SERVICE_TYPE_CD
,SCE.sc_tlc_prob2_cd_orig AS SERVICE_TYPE_CD_2
,NULL  AS SERVICE_DETAIL_REASON_CD
,SC_PROB_CD  AS SERVICE_PROBLEM_CD
,TD.TOW_DEST_LATITUDE AS TOW_DESTINATION_LATITUDE
,TD.TOW_DEST_LONGITUDE AS TOW_DESTINATION_LONGITUDE
,SV.SC_VEH_MANF_YR_DT AS SC_VEHICLE_MAKE_YEAR
,SV.SC_VEH_MANFR_NM AS SC_VEHICLE_MAKE_NAME
,SV.SC_VEH_MDL_NM AS SC_VEHICLE_MODLE_NAME
,SV.SC_VEH_COLR_NM AS SC_VEHICLE_COLOR_NAME
,SV.VEH_TYPE AS SC_VEHICLE_TYPE
,SV.RV_TOWING
--,0.0001 AS CALL_COST
,DECODE(SC.STATUS_CD, 'RE', 1, 'SP', 2, 'AS', 3, 'DI', 4, 'ER', 5, 'OL', 6, 'TW', 7, 'CL', 8,NULL) AS STATUS_RANK
,CASE WHEN FRE.EMPLE_ID = 'RAP_SRVR' THEN 'MOBILE APP'
     WHEN FRE.EMPLE_ID = 'ICIVR' THEN 'ERSA'
     WHEN FRE.EMPLE_ID = 'FUSION' THEN 'APP/WEB PORTAL'
     ELSE 'CALL RECEIVER' END AS CALL_RECEIVE_CHANNEL
,SCE.EMAIL_OPT_IN
,SCE.SMS_OPT_IN	
,SCE.SMS_CONTACT_MBR	 
FROM NCA_CAD.service_call SC
LEFT JOIN NCA_CAD.service_call_extd SCE ON  SC.SC_ID = SCE.SC_ID AND SC.SC_DT = SCE.SC_DT 
LEFT JOIN NCA_CAD.BREAKDOWN_LOC BL ON SC.SC_ID = BL.SC_ID AND SC.SC_DT = BL.SC_DT
LEFT JOIN NCA_CAD.SC_PROBLEM SP  ON SC.SC_ID = SP.SC_ID AND SC.SC_DT = SP.SC_DT
LEFT JOIN (SELECT * from (select ER.sc_dt, ER.sc_id, SC_FACL_ID, sc_trk_id, ER.SC_STS_TM as FSTER_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY ER.sc_dt, ER.sc_id, ER.sc_sts_cd, ER.sc_sts_upd_cd  ORDER BY ER.SC_STS_TM) AS rownumber
                from NCA_CAD.sc_status ER
                WHERE ER.SC_STS_CD='ER'
                AND ER.SC_STS_UPD_CD = 'ER')
                where rownumber = 1) FER ON SC.SC_ID = FER.SC_ID AND SC.SC_DT = FER.SC_DT
LEFT JOIN (SELECT * from (select   RE.sc_dt, RE.sc_id, RE.SC_STS_TM as FSTRE_STS_TM, EMPLE_ID, ROW_NUMBER()
                OVER (PARTITION BY RE.sc_dt, RE.sc_id,  RE.sc_sts_cd, RE.sc_sts_upd_cd  ORDER BY RE.SC_STS_TM) AS rownumber
                from NCA_CAD.sc_status RE
                WHERE RE.SC_STS_CD='RE'
                AND RE.SC_STS_UPD_CD = 'RE'
                AND RE.sc_dt >  TRUNC(SYSDATE-7))
                where rownumber = 1) FRE ON  SC.SC_ID = FRE.SC_ID AND SC.SC_DT = FRE.SC_DT
LEFT JOIN (SELECT * from (select  di.sc_dt, di.sc_id, di.SC_STS_TM as FSTDI_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY   di.sc_dt, di.sc_id,  di.sc_sts_cd, di.sc_sts_upd_cd  ORDER BY di.SC_STS_TM) AS rownumber
                from NCA_CAD.sc_status di
                WHERE di.SC_STS_CD = 'DI'
                AND di.SC_STS_UPD_CD = 'DI'
                AND di.sc_dt >  TRUNC(SYSDATE-7))
                where rownumber = 1) FDI ON SC.SC_ID = FDI.SC_ID AND SC.SC_DT = FDI.SC_DT
LEFT JOIN (SELECT * from (select   ER.sc_dt, ER.sc_id, ER.SC_STS_TM as FSTER_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY   ER.sc_dt, ER.sc_id,  ER.sc_sts_cd, ER.sc_sts_upd_cd  ORDER BY ER.SC_STS_TM) AS rownumber
                from NCA_CAD.sc_status ER
                WHERE ER.SC_STS_CD='ER'
                AND ER.SC_STS_UPD_CD = 'ER'
                AND ER.sc_dt >  TRUNC(SYSDATE-7))
                where rownumber = 1) FER ON  SC.SC_ID = FER.SC_ID AND SC.SC_DT = FER.SC_DT
LEFT JOIN (SELECT * from (select   cl.sc_dt, cl.sc_id,  cl.SC_STS_TM as fstcl_STS_TM,  ROW_NUMBER()
                OVER (PARTITION BY  cl.sc_dt, cl.sc_id  ORDER BY cl.SC_STS_TM) AS rownumber
                from NCA_CAD.sc_status cl
                WHERE cl.SC_STS_CD='CL'
                AND cl.SC_STS_UPD_CD = 'CL'
                AND cl.SC_DT >  TRUNC(SYSDATE-7))
                where rownumber = 1) FCL ON   SC.SC_ID = FCL.SC_ID AND SC.SC_DT = FCL.SC_DT
LEFT JOIN (SELECT * from (select   ol.sc_dt, ol.sc_id,   ol.SC_STS_TM as FSTOL_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY   ol.sc_dt, ol.sc_id ORDER BY ol.SC_STS_TM) AS rownumber
                from NCA_CAD.sc_status ol
                WHERE ol.SC_STS_CD='OL'
                AND ol.SC_STS_UPD_CD = 'OL'
                AND ol.SC_DT > TRUNC(SYSDATE-7))
                where rownumber = 1) FOL ON  SC.SC_ID = FOL.SC_ID AND SC.SC_DT = FOL.SC_DT
LEFT OUTER JOIN NCA_CAD.SVC_VEHICLE SV  ON SC.SC_ID = SV.SC_ID AND SC.SC_DT = SV.SC_DT 
LEFT OUTER JOIN NCA_CAD.TOW_DEST TD  ON SC.SC_ID = TD.SC_ID AND SC.SC_DT = TD.SC_DT 
WHERE SC.LAST_UPDATE  >  v_incr_date;