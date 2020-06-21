SELECT AC.SC_ID AS SC_ID
,AC.SC_DT AS SC_DT
,AC.COMM_CTR_ID AS COMM_CENTER_ID
,AC.SC_CALL_CLB_CD AS SC_CALL_CLUB_CD
,AC.call_source AS CALL_SOURCE
,AC.SC_CALL_MBR_ID || AC.SC_CALL_ASC_ID AS MEMBERSHIP_NUM
,ACE.SVC_FACL_ID AS SC_FACILITY_ID
,CASE WHEN Ftrk.sc_trk_id is null then SC_A_SVC_TRK_ID ELSE Ftrk.sc_trk_id END AS SC_TRUCK_ID
,AC.STATUS_CD AS STATUS_CD
,AC.SC_STS_RSN_CD AS SC_STATUS_REASON_CD
,AC.SC_A_SVC_TRK_D_ID AS TRUCK_DRIVER_ID
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
,SC_VEH_MANF_YR_DT AS SC_VEHICLE_MAKE_YEAR
,SC_VEH_MANFR_NM AS SC_VEHICLE_MAKE_NAME
,SC_VEH_MDL_NM AS SC_VEHICLE_MODLE_NAME
,SC_VEH_COLR_NM AS SC_VEHICLE_COLOR_NAME
,SC_VEH_TYPE AS SC_VEHICLE_TYPE
,ACE.RV_TOWING 
,AC.CALL_COST AS CALL_COST
,AC.ARCH_DATE AS ARCH_DATE FROM
(SELECT * FROM NCA_ERS_HIST.ARCH_CALL WHERE ARCH_DATE > v_incr_date ) AC
INNER JOIN NCA_ERS_HIST.ARCH_call_extd ACE ON  AC.SC_ID = ACE.SC_ID AND AC.SC_DT = ACE.SC_DT AND AC.COMM_CTR_ID = ACE.COMM_CTR_ID
LEFT JOIN (SELECT * from (select RE.comm_ctr_id, RE.sc_dt, RE.sc_id, RE.SC_STS_TM as FSTRE_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY RE.comm_ctr_id, RE.sc_dt, RE.sc_id,  RE.sc_sts_cd, RE.sc_sts_upd_cd  ORDER BY RE.SC_STS_TM) AS rownumber
                from NCA_ERS_HIST.sc_status RE
                WHERE RE.SC_STS_CD='RE'
                AND RE.SC_STS_UPD_CD = 'RE'
                AND RE.sc_dt >  TRUNC(SYSDATE-30))
                where rownumber = 1) FRE ON AC.COMM_CTR_ID = FRE.COMM_CTR_ID AND AC.SC_ID = FRE.SC_ID AND AC.SC_DT = FRE.SC_DT
LEFT JOIN (SELECT * from (select di.comm_ctr_id, di.sc_dt, di.sc_id, di.SC_STS_TM as FSTDI_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY di.comm_ctr_id, di.sc_dt, di.sc_id,  di.sc_sts_cd, di.sc_sts_upd_cd  ORDER BY di.SC_STS_TM) AS rownumber
                from NCA_ERS_HIST.sc_status di
                WHERE di.SC_STS_CD='DI'
                AND di.SC_STS_UPD_CD = 'DI'
                AND di.sc_dt >  TRUNC(SYSDATE-30))
                where rownumber = 1) FDI ON AC.COMM_CTR_ID = FDI.COMM_CTR_ID AND AC.SC_ID = FDI.SC_ID AND AC.SC_DT = FDI.SC_DT
LEFT JOIN (SELECT * from (select ER.comm_ctr_id, ER.sc_dt, ER.sc_id, ER.SC_STS_TM as FSTER_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY ER.comm_ctr_id, ER.sc_dt, ER.sc_id,  ER.sc_sts_cd, ER.sc_sts_upd_cd  ORDER BY ER.SC_STS_TM) AS rownumber
                from NCA_ERS_HIST.sc_status ER
                WHERE ER.SC_STS_CD='ER'
                AND ER.SC_STS_UPD_CD = 'ER'
                AND ER.sc_dt >  TRUNC(SYSDATE-30))
                where rownumber = 1) FER ON AC.COMM_CTR_ID = FER.COMM_CTR_ID AND AC.SC_ID = FER.SC_ID AND AC.SC_DT = FER.SC_DT
LEFT JOIN (SELECT * from (select cl.comm_ctr_id, cl.sc_dt, cl.sc_id,  cl.SC_STS_TM as fstcl_STS_TM,  ROW_NUMBER()
                OVER (PARTITION BY cl.comm_ctr_id, cl.sc_dt, cl.sc_id  ORDER BY cl.SC_STS_TM) AS rownumber
                from NCA_ERS_HIST.sc_status cl
                WHERE cl.SC_STS_CD='CL'
                AND cl.SC_STS_UPD_CD = 'CL'
                AND cl.SC_DT >  TRUNC(SYSDATE-30))
                where rownumber = 1) FCL ON AC.COMM_CTR_ID = FCL.COMM_CTR_ID AND AC.SC_ID = FCL.SC_ID AND AC.SC_DT = FCL.SC_DT
LEFT JOIN (SELECT * from (select ol.comm_ctr_id, ol.sc_dt, ol.sc_id,   ol.SC_STS_TM as FSTOL_STS_TM, ROW_NUMBER()
                OVER (PARTITION BY ol.comm_ctr_id, ol.sc_dt, ol.sc_id ORDER BY ol.SC_STS_TM) AS rownumber
                from NCA_ERS_HIST.sc_status ol
                WHERE ol.SC_STS_CD='OL'
                AND ol.SC_STS_UPD_CD = 'OL'
                AND ol.SC_DT > TRUNC(SYSDATE-30))
                where rownumber = 1) FOL ON AC.COMM_CTR_ID = FOL.COMM_CTR_ID AND AC.SC_ID = FOL.SC_ID AND AC.SC_DT = FOL.SC_DT
LEFT JOIN (SELECT * from (select trk.comm_ctr_id, trk.sc_dt, trk.sc_id, sc_trk_id,  trk.SC_STS_TM as fsttrk_STS_TM,  ROW_NUMBER()
                OVER (PARTITION BY trk.comm_ctr_id, trk.sc_dt, trk.sc_id  ORDER BY trk.SC_STS_TM DESC) AS rownumber
                from NCA_ERS_HIST.sc_status trk
                WHERE trk.SC_DT >  TRUNC(SYSDATE-30)
                and sc_trk_id is not null)
                 where rownumber = 1 )  Ftrk ON AC.COMM_CTR_ID = Ftrk.COMM_CTR_ID AND AC.SC_ID = Ftrk.SC_ID AND TRUNC(AC.SC_DT) = TRUNC(Ftrk.SC_DT)
WHERE  AC.ARCH_DATE > v_incr_date;