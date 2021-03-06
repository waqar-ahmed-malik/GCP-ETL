UPDATE
  LANDING.DWH_SOURCE_TO_TARGET_AUDIT A
SET
  A.GCP_COUNT = B.GCP_COUNT
FROM (
  SELECT
    GCP_COUNT,
    GCP_TABLE_NAME
  FROM (
    SELECT
      COUNT(*) AS GCP_COUNT,
      'ERS_AAA_AUTO_APPROVED_REPAIR' AS GCP_TABLE_NAME
    FROM
      REFERENCE.ERS_AAA_AUTO_APPROVED_REPAIR
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_CAD_EMPLOYEE'
    FROM
      REFERENCE.ERS_STAGE_CAD_EMPLOYEE
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_CAD_CLUB_INFO'
    FROM
      REFERENCE.ERS_STAGE_CAD_CLUB_INFO
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_SERVICE_FACILITY'
    FROM
      REFERENCE.ERS_STAGE_SERVICE_FACILITY
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_SERVICE_TRUCK'
    FROM
      REFERENCE.ERS_STAGE_SERVICE_TRUCK
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_SVC_FAC_DBA'
    FROM
      REFERENCE.ERS_STAGE_SVC_FAC_DBA
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_CAD_SERVICE_FACILITY'
    FROM
      REFERENCE.ERS_STAGE_CAD_SERVICE_FACILITY
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_HIST_DUP_CALL_LST'
    FROM
      OPERATIONAL.ERS_STAGE_HIST_DUP_CALL_LST
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_LOOK_UP'
    FROM
      REFERENCE.ERS_LOOK_UP
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_LOOK_UP_NAMES'
    FROM
      REFERENCE.ERS_LOOK_UP_NAMES
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_SVC_TRK_DRIVER'
    FROM
      REFERENCE.ERS_SVC_TRK_DRIVER
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_PREDICTIVE_TRUCK_ASSIGNMENT'
    FROM
      OPERATIONAL.ERS_PREDICTIVE_TRUCK_ASSIGNMENT
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_CAD_VEHICLE_DATA'
    FROM
      REFERENCE.ERS_STAGE_CAD_VEHICLE_DATA
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_APD_REMB_DETAIL'
    FROM
      OPERATIONAL.ERS_STAGE_APD_REMB_DETAIL
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_APD_SVC_FAC_ADJ_ITEM'
    FROM
      OPERATIONAL.ERS_STAGE_APD_SVC_FAC_ADJ_ITEM
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_HIST_AAR_REFERRAL'
    FROM
      OPERATIONAL.ERS_STAGE_HIST_AAR_REFERRAL
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_INQ_ENTITLEMENT_PERIODS'
    FROM
      OPERATIONAL.ERS_STAGE_INQ_ENTITLEMENT_PERIODS
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_PAYMENT'
    FROM
      OPERATIONAL.ERS_STAGE_PAYMENT
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_REPORTS_DATA'
    FROM
      OPERATIONAL.ERS_STAGE_REPORTS_DATA
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_STAT_GRP'
    FROM
      OPERATIONAL.ERS_STAGE_STAT_GRP
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_APD_REIMBURSEMENT'
    FROM
      OPERATIONAL.ERS_STAGE_APD_REIMBURSEMENT
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_HIST_CALL_ADJ'
    FROM
      OPERATIONAL.ERS_STAGE_HIST_CALL_ADJ
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_SPOT_GRID'
    FROM
      REFERENCE.ERS_STAGE_SPOT_GRID
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_ARCH_CALL_EXTD'
    FROM
      OPERATIONAL.ERS_STAGE_ARCH_CALL_EXTD
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_ARCH_CALL'
    FROM
      OPERATIONAL.ERS_STAGE_ARCH_CALL
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_SC_STATUS'
    FROM
      OPERATIONAL.ERS_STAGE_SC_STATUS
    UNION ALL
    SELECT
      COUNT(*),
      'ERS_STAGE_MILEAGE_HIST'
    FROM
      OPERATIONAL.ERS_STAGE_MILEAGE_HIST ) ) B
WHERE
  A.GCP_TABLE_NAME = B.GCP_TABLE_NAME
  AND A.CREATE_DTTIME = (
  SELECT
    MAX(CREATE_DTTIME)
  FROM
    LANDING.DWH_SOURCE_TO_TARGET_AUDIT)