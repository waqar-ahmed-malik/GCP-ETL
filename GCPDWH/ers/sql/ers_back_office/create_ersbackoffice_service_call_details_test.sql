CREATE OR REPLACE TABLE `aaa-mwg-ersbackofficetest.OPERATIONAL.ERS_SERVICE_CALL_DETAILS` AS  
SELECT
  first.COMM_CTR_ID,
  first.SC_DT,
  first.SC_ID,
  first.ARCH_DATETIME,
  first.SC_CALL_CLB_CD,
  first.SC_CALL_MBR_ID,
  first.SC_CNTC_FST_NM,
  first.SC_CNTC_LST_NM,
  first.SC_CNTC_SUFFIX,
  first.SC_WAIT_TM,
  first.SC_RED_FLAG_IN,
  first.STATUS_CD,
  first.SC_STS_RSN_CD,
  first.SC_RCVD_TM,
  first.SC_VEH_MANF_YR_DT,
  first.SC_VEH_MANFR_NM,
  first.SC_VEH_MDL_NM,
  first.BL_NEAR_CTY_NM,
  first.BL_N_LNDMRK_TX,
  first.BL_STATE_CD,
  first.FACIL_INT_ID,
  first.SC_A_SVC_TRK_ID,
  first.SC_A_SVC_TRK_D_ID,
  first.PROB1_CD,
  first.PROB2_CD,
  first.EMPLE_ID,
  first.CALL_SOURCE,
  first.DTL_PROB1_CD,
  first.DTL_PROB2_CD,
  first.DTL_STS_RSN_CD,
  first.SC_VEH_TYPE,
  first.BL_ZIP,
  first.BL_ZIP4,
  first.HIGHWAY_CALL,
  first.BL_VERIFIED_LOC,
  first.SC_REASGN_RSN_CD,
  second.SCOC_INDIC,
  second.CUC_INDIC,
  second.FLATBED_REQ_INDIC,
  second.APT_CALL_INDIC,
  second.TRK_ZONE,
  second.BL_LAT,
  second.BL_LONG,
  second.TD_LAT,
  second.TD_LONG,
  second.FLATBED_INDIC,
  second.FOUR_WHEEL_DRIVE,
  second.REC_VEHICLE,
  second.AWD_VEHICLE,
  second.SVC_FACL_ID,
  second.AUDIT_FLAG,
  second.BATT_SVC_RESULT,
  second.BATTERY_TESTED,
  second.BATTERY_TESTNUMBER,
  second.USED_LONG_TOW,
  second.LONG_TOW_ELIG_INDIC,
  second.LONG_TOW_MILES_ALLOW,
  second.BATTERY_PRICE,
  second.BLEND_RATE_INDIC,
  second.DTL_PROB1_CD_ORIG,
  second.DTL_PROB2_CD_ORIG,
  second.TWO_WHEEL_DRIVE,
  second.DOLLY_INDIC,
  second.LOCKSMITH_INDIC,
  second.WHEEL_LIFT_REQ_INDIC,
  second.WHEEL_LIFT_INDIC,
  second.RV_LENGTH,
  second.RV_HEIGHT,
  second.HYBRID_VEHICLE,
  second.ELECTRIC_VEHICLE,
  second.IS_DIESEL,
  second.AIR_SUSPENSION,
  second.ADDR2_CITY,
  second.ADDR2_STATE_CD,
  second.ADDR2_ZIP,
  second.ADDR2_ZIP4,
  second.ODOMETER_READING,
  third.SVCP_ID,
  third.SVCP_CD,
  third.SVCP_UNITS,
  third.SVCP_UNIT_COST,
  third.SVCP_UOM_CD,
  third.SVCP_ADD_CHRG,
  third.SVCP_BASIC_COST,
  third.SVCP_OTHER_COST,
  third.SVCP_PLUS_COST,
  third.SVCP_ADJ_COST,
  third.SVCP_TOT_COST,
  third.SVCP_DISP_APP,
  third.SVCP_DISP_PRICE,
  third.SVCP_RATEC_CD,
  third.SVCP_TOD_CD,
  third.SC_PROB_CD,
  fifth.MEMBERSHIP_LEVEL_DESC,
  fourth.SEGMENT,
  new1.MILES_ER_DISP,
  new1.MILES_TW_DISP,
  new1.MILES_ER_CALC,
  new1.MILES_TW_CALC,
  new1.MILES_ER_CPMS,
  new1.MILES_TW_CPMS,
  FRE.FSTRE_STS_TM AS RECEIVED_STATUS_TIME,
  FSTDI_STS_TM AS DISPATCH_STATUS_TIME,
  FSTER_STS_TM AS ENROUTE_STATUS_TIME,
  FSTOL_STS_TM AS ON_LOCATION_STATUS_TIME,
  fstcl_STS_TM AS CLOSED_STATUS_TIME,
  third.DELETE_FLG
FROM (
  SELECT
    *
  FROM
    `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_ARCH_CALL`
  WHERE
    SC_DT >= DATETIME_SUB(CURRENT_DATETIME(),
      INTERVAL 100 DAY)) first
LEFT JOIN (
  SELECT
    *
  FROM
    `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_ARCH_CALL_EXTD`
  WHERE
    SC_DT >= DATETIME_SUB(CURRENT_DATETIME(),
      INTERVAL 100 DAY)) second
ON
  first.SC_ID = second.SC_ID
  AND first.COMM_CTR_ID = second.COMM_CTR_ID
  AND first.SC_DT = second.SC_DT
LEFT JOIN (
  SELECT
    *
  FROM
    `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_SERVICE_PROVIDED`
  WHERE
    SC_DT >= DATETIME_SUB(CURRENT_DATETIME(),
      INTERVAL 100 DAY)) third
ON
  third.SC_ID = second.SC_ID
  AND third.COMM_CTR_ID = second.COMM_CTR_ID
  AND third.SC_DT = second.SC_DT
LEFT JOIN (
  SELECT
    *
  FROM
    `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_MILEAGE_HIST`
  WHERE
    SC_DT >= DATETIME_SUB(CURRENT_DATETIME(),
      INTERVAL 100 DAY)) new1
ON
  new1.SC_ID = third.SC_ID
  AND new1.COMM_CTR_ID = third.COMM_CTR_ID
  AND new1.SC_DT = third.SC_DT
LEFT JOIN (
  SELECT
    *
  FROM
    `aaa-mwg-dwprod.REFERENCE.ERS_SERVICE_FACILITY_LOCAL`
  WHERE
    SVC_FACL_ID IS NOT NULL) fourth
ON
  fourth.SVC_FACL_ID = second.SVC_FACL_ID
LEFT JOIN (
  SELECT
    MEMBERSHIP_NUM,
    MEMBERSHIP_LEVEL_DESC
  FROM
    `aaa-mwg-dwprod.OPERATIONAL.CONNECTSUITE_MEMBER`
  WHERE
    ASSOCIATE_ID = 1) fifth
ON
  fifth.MEMBERSHIP_NUM = first.SC_CALL_MBR_ID
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      RE.comm_ctr_id,
      RE.sc_dt,
      RE.sc_id,
      RE.SC_STS_TM AS FSTRE_STS_TM,
      ROW_NUMBER() OVER (PARTITION BY RE.comm_ctr_id, RE.sc_dt, RE.sc_id, RE.sc_sts_cd, RE.sc_sts_upd_cd ORDER BY RE.SC_STS_TM) AS rownumber
    FROM
      `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_SC_STATUS` RE
    WHERE
      RE.SC_STS_CD='RE'
      AND RE.SC_STS_UPD_CD = 'RE'
      AND RE.sc_dt > DATETIME_SUB(CURRENT_DATETIME(),
        INTERVAL 100 DAY))
  WHERE
    rownumber = 1) FRE
ON
  first.COMM_CTR_ID = FRE.COMM_CTR_ID
  AND first.SC_ID = FRE.SC_ID
  AND first.SC_DT = FRE.SC_DT
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      di.comm_ctr_id,
      di.sc_dt,
      di.sc_id,
      di.SC_STS_TM AS FSTDI_STS_TM,
      ROW_NUMBER() OVER (PARTITION BY di.comm_ctr_id, di.sc_dt, di.sc_id, di.sc_sts_cd, di.sc_sts_upd_cd ORDER BY di.SC_STS_TM) AS rownumber
    FROM
      `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_SC_STATUS` DI
    WHERE
      di.SC_STS_CD = 'DI'
      AND di.SC_STS_UPD_CD = 'DI'
      AND di.sc_dt > DATETIME_SUB(CURRENT_DATETIME(),
        INTERVAL 100 DAY))
  WHERE
    rownumber = 1) FDI
ON
  first.COMM_CTR_ID = FDI.COMM_CTR_ID
  AND first.SC_ID = FDI.SC_ID
  AND first.SC_DT = FDI.SC_DT
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      ER.comm_ctr_id,
      ER.sc_dt,
      ER.sc_id,
      ER.SC_STS_TM AS FSTER_STS_TM,
      ROW_NUMBER() OVER (PARTITION BY ER.comm_ctr_id, ER.sc_dt, ER.sc_id, ER.sc_sts_cd, ER.sc_sts_upd_cd ORDER BY ER.SC_STS_TM) AS rownumber
    FROM
      `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_SC_STATUS` ER
    WHERE
      ER.SC_STS_CD='ER'
      AND ER.SC_STS_UPD_CD = 'ER'
      AND ER.sc_dt > DATETIME_SUB(CURRENT_DATETIME(),
        INTERVAL 100 DAY))
  WHERE
    rownumber = 1) FER
ON
  first.COMM_CTR_ID = FER.COMM_CTR_ID
  AND first.SC_ID = FER.SC_ID
  AND first.SC_DT = FER.SC_DT
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      cl.comm_ctr_id,
      cl.sc_dt,
      cl.sc_id,
      cl.SC_STS_TM AS fstcl_STS_TM,
      ROW_NUMBER() OVER (PARTITION BY cl.comm_ctr_id, cl.sc_dt, cl.sc_id ORDER BY cl.SC_STS_TM) AS rownumber
    FROM
      `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_SC_STATUS` cl
    WHERE
      cl.SC_STS_CD='CL'
      AND cl.SC_STS_UPD_CD = 'CL'
      AND cl.SC_DT > DATETIME_SUB(CURRENT_DATETIME(),
        INTERVAL 100 DAY) )
  WHERE
    rownumber = 1) FCL
ON
  first.COMM_CTR_ID = FCL.COMM_CTR_ID
  AND first.SC_ID = FCL.SC_ID
  AND first.SC_DT = FCL.SC_DT
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      ol.comm_ctr_id,
      ol.sc_dt,
      ol.sc_id,
      ol.SC_STS_TM AS FSTOL_STS_TM,
      ROW_NUMBER() OVER (PARTITION BY ol.comm_ctr_id, ol.sc_dt, ol.sc_id ORDER BY ol.SC_STS_TM) AS rownumber
    FROM
      `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_SC_STATUS` ol
    WHERE
      ol.SC_STS_CD='OL'
      AND ol.SC_STS_UPD_CD = 'OL'
      AND ol.SC_DT > DATETIME_SUB(CURRENT_DATETIME(),
        INTERVAL 100 DAY))
  WHERE
    rownumber = 1) FOL
ON
  first.COMM_CTR_ID = FOL.COMM_CTR_ID
  AND first.SC_ID = FOL.SC_ID
  AND first.SC_DT = FOL.SC_DT
  AND first.SC_DT > DATETIME_SUB(CURRENT_DATETIME(),
    INTERVAL 100 DAY)