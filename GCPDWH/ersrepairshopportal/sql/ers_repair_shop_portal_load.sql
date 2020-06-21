INSERT INTO
  OPERATIONAL.ERS_REPAIR_SHOP_PORTAL ( ORDER_ID,
    CLUB_CD,
    SC_DT,
    SC_ID,
    SHOP_ID,
    CALL_KEY,
    ORDER_NUM,
    TYPE,
    PACESETTER_CODE,
    RESOLUTION_CODE,
    UNSEEN,
    TIME_CREATED,
    ARCHIVE_TIME,
    ARCHIVE_REASON,
    CREATED_BY_SHOP,
    SERVICE_CATEGORY,
    SERVICE_SUBCATEGORY,
    NOTES,
    MEMBER_NUMBER,
    CUSTOMER_FIRST_NAME,
    CUSTOMER_LAST_NAME,
    CUSTOMER_PHONE,
    MEMBERSHIP_VERIFIED,
    VEHICLE_YEAR,
    VEHICLE_MAKE,
    VEHICLE_MODEL,
    VEHICLE_TRIM,
    VEHICLE_COLOR,
    VEHICLE_TAG_NUMBER,
    VEHICLE_TAG_STATE,
    VEHICLE_VIN,
    VEHICLE_ODOMETER,
    REASON_CD,
    DETAILS,
    STATUS_INBOUND,
    STATUS_REFERRED,
    STATUS_NOT_ACKNOWLEDGED,
    STATUS_DELETED,
    STATUS_ARRIVED,
    STATUS_NOT_ARRIVED,
    STATUS_INCOMPLETE,
    STATUS_IN_PROGRESS,
    STATUS_COMPLETE,
    STATUS_CANCELLED,
    CALC_MEMBERSHIP_VERIFIED,
    CALC_AAA_TOW_IN,
    CALC_SHOP_CREATED,
    CALC_TOTAL_TOW_IN,
    CALC_WALK_IN,
    REPAIR_COST,
    REPAIR_DISCOUNT,
    TOTAL_COST,
    ERS_MATCH,
    ACCEPTING_TOWS,
    IS_BATTERY_SERVICE,
    BATTERY_SERVICE_TYPE,
    BATTERY_SERVICE_SUBTYPE,
    BATTERY_SERIAL_NUMBER,
    BATTERY_REFILLED_ELECTROLYTE,
    BATTERY_CLEANED_TERMINALS,
    BATTERY_TIGHTENED_TERMINALS,
    BATTERY_REPAIRED_TERMINALS,
    BATTERY_REPLACED_TERMINALS,
    BATTERY_STRINGGED_BATTERY,
    BATTERY_PRORATED_TEST_RESULT,
    BATTERY_PRORATED_MONTHS_USED,
    NEW_BATTERY_SERIAL_NUMBER,
    NEW_BATTERY_TYPE,
    NEW_BATTERY_TERMINAL,
    NEW_BATTERY_CCA,
    NEW_BATTERY_RC_MIN,
    NEW_BATTERY_WHOLESALE_PRICE,
    NEW_BATTERY_MEMBER_PRICE,
    NEW_BATTERY_SUGGESTED_RETAIL,
    NEW_BATTERY_PRICE_BEFORE_TAX,
    NEW_BATTERY_LABOR_COST,
    NEW_BATTERY_LABOR_INTEGERENSIVE,
    NEW_BATTERY_FREE_WARRANTY_MONTHS,
    NEW_BATTERY_WARRANTY_MONTHS,
    NEW_BATTERY_COMMENTS,
    VOUCHER_CODE,
    VOUCHER_VALID,
    VOUCHER_VALUE,
    VOUCHER_REDEEMED,
    VOUCHER_REDEEMED_AMOUNT,
    VOUCHER_REDEEMED_TIME,
    EXTENDED_UNTIL,
    CONTACTED_MEMBER,
    CARD_WAS_SWIPED,
    VEHICLE_ENGINE,
    APPOINTEGERMENT_CREATED,
    APPOINTEGERMENT_LAST_UPDATED,
    APPOINTEGERMENT_PREV_TIME,
    APPOINTEGERMENT_TIME,
    APPOINTEGERMENT_RELEASED,
    APPOINTEGERMENT_RELEASE_REASON,
    LEGACY_CLUB_CODE,
    INSPECTION_PERFORMED,
    CREATED_BY,
    MEMBER_EMAIL,
    BATTERY_IS_INSTALL,
    BATTERY_WARRANTY_REPLACEMENT,
    BATTERY_CORRECT_BATTERY_INSTALLED,
    BATTERY_PASSED_TEST,
    BATTERY_IS_PRORATED,
    NEW_BATTERY_IS_AAA_BATTERY,
    INSERT_DTTIME )
SELECT
  ORDER_ID,
  CLUB_CODE AS CLUB_CD,
  PARSE_DATE('%Y%m%d',SC_DT) AS SC_DT,
  CAST(SC_CALL_ID AS INT64) AS SC_ID,
  SHOP_ID,
  CALL_KEY,
  ORDER_NUMBER AS ORDER_NUM,
  TYPE,
  PACESETTER_CODE,
  RESOLUTION_CODE,
  UNSEEN,
  CAST(TIME_CREATED AS DATETIME) AS TIME_CREATED,
  CAST(ARCHIVE_TIME AS DATETIME) AS ARCHIVE_TIME,
  ARCHIVE_REASON,
  CREATED_BY_SHOP,
  SERVICE_CATEGORY,
  SERVICE_SUBCATEGORY,
  NOTES,
  MEMBER_NUMBER,
  CUSTOMER_FIRST_NAME,
  CUSTOMER_LAST_NAME,
  CUSTOMER_PHONE,
  MEMBERSHIP_VERIFIED,
  VEHICLE_YEAR,
  VEHICLE_MAKE,
  VEHICLE_MODEL,
  VEHICLE_TRIM,
  VEHICLE_COLOR,
  VEHICLE_TAG_NUMBER,
  VEHICLE_TAG_STATE,
  VEHICLE_VIN,
  CAST(VEHICLE_ODOMETER AS INT64) AS VEHICLE_ODOMETER,
  REASON_CODE AS REASON_CD,
  DETAILS,
  CAST(STATUS_INBOUND AS INT64) AS STATUS_INBOUND,
  CAST(STATUS_REFERRED AS INT64) AS STATUS_REFERRED,
  CAST(STATUS_NOT_ACKNOWLEDGED AS INT64) AS STATUS_NOT_ACKNOWLEDGED,
  CAST(STATUS_DELETED AS INT64) AS STATUS_DELETED,
  CAST(STATUS_ARRIVED AS INT64) AS STATUS_ARRIVED,
  CAST(STATUS_NOT_ARRIVED AS INT64) AS STATUS_NOT_ARRIVED,
  CAST(STATUS_INCOMPLETE AS INT64) AS STATUS_INCOMPLETE,
  CAST(STATUS_IN_PROGRESS AS INT64) AS STATUS_IN_PROGRESS,
  CAST(STATUS_COMPLETE AS INT64) AS STATUS_COMPLETE,
  CAST(STATUS_CANCELLED AS INT64) AS STATUS_CANCELLED,
  CAST(CALC_MEMBERSHIP_VERIFIED AS INT64) AS CALC_MEMBERSHIP_VERIFIED,
  CAST(CALC_AAA_TOW_IN AS INT64) AS CALC_AAA_TOW_IN,
  CAST(CALC_SHOP_CREATED AS INT64) AS CALC_SHOP_CREATED,
  CAST(CALC_TOTAL_TOW_IN AS INT64) AS CALC_TOTAL_TOW_IN,
  CAST(CALC_WALK_IN AS INT64) AS CALC_WALK_IN,
  CAST(REPAIR_COST AS FLOAT64) AS REPAIR_COST,
  CAST(REPAIR_DISCOUNT AS FLOAT64) AS REPAIR_DISCOUNT,
  CAST(TOTAL_COST AS FLOAT64) AS TOTAL_COST,
  ERS_MATCH,
  ACCEPTING_TOWS,
  IS_BATTERY_SERVICE,
  BATTERY_SERVICE_TYPE,
  BATTERY_SERVICE_SUBTYPE,
  BATTERY_SERIAL_NUMBER,
  BATTERY_REFILLED_ELECTROLYTE,
  BATTERY_CLEANED_TERMINALS,
  BATTERY_TIGHTENED_TERMINALS,
  BATTERY_REPAIRED_TERMINALS,
  BATTERY_REPLACED_TERMINALS,
  BATTERY_CHARGED_BATTERY,
  BATTERY_PRORATED_TEST_RESULT,
  CAST(BATTERY_PRORATED_MONTHS_USED AS INT64) AS BATTERY_PRORATED_MONTHS_USED,
  NEW_BATTERY_SERIAL_NUMBER,
  NEW_BATTERY_TYPE,
  NEW_BATTERY_TERMINAL,
  CAST(NEW_BATTERY_CCA AS INT64) AS NEW_BATTERY_CCA,
  CAST(NEW_BATTERY_RC_MIN AS INT64) AS NEW_BATTERY_RC_MIN,
  CAST(NEW_BATTERY_WHOLESALE_PRICE AS FLOAT64) AS NEW_BATTERY_WHOLESALE_PRICE,
  CAST(NEW_BATTERY_MEMBER_PRICE AS FLOAT64) AS NEW_BATTERY_MEMBER_PRICE,
  CAST(NEW_BATTERY_SUGGESTED_RETAIL AS FLOAT64) AS NEW_BATTERY_SUGGESTED_RETAIL,
  CAST(NEW_BATTERY_PRICE_BEFORE_TAX AS FLOAT64) AS NEW_BATTERY_PRICE_BEFORE_TAX,
  CAST(NEW_BATTERY_LABOR_COST AS FLOAT64) AS NEW_BATTERY_LABOR_COST,
  NEW_BATTERY_LABOR_INTENSIVE AS NEW_BATTERY_LABOR_INTENSIVE,
  CAST(NEW_BATTERY_FREE_WARRANTY_MONTHS AS INT64) AS NEW_BATTERY_FREE_WARRANTY_MONTHS,
  CAST(NEW_BATTERY_WARRANTY_MONTHS AS INT64) AS NEW_BATTERY_WARRANTY_MONTHS,
  NEW_BATTERY_COMMENTS,
  VOUCHER_CODE,
  VOUCHER_VALID,
  CAST(VOUCHER_VALUE AS FLOAT64) AS VOUCHER_VALUE,
  VOUCHER_REDEEMED,
  CAST(VOUCHER_REDEEMED_AMOUNT AS FLOAT64) AS VOUCHER_REDEEMED_AMOUNT,
  CAST(VOUCHER_REDEEMED_TIME AS DATETIME) AS VOUCHER_REDEEMED_TIME,
  CAST(EXTENDED_UNTIL AS DATETIME) AS EXTENDED_UNTIL,
  CONTACTED_MEMBER,
  CARD_WAS_SWIPED,
  VEHICLE_ENGINE,
  CAST(APPOINTMENT_CREATED AS DATETIME) AS APPOINTMENT_CREATED,
  CAST(APPOINTMENT_LAST_UPDATED AS DATETIME) AS APPOINTMENT_LAST_UPDATED,
  CAST(APPOINTMENT_PREV_TIME AS DATETIME) AS APPOINTMENT_PREV_TIME,
  CAST(APPOINTMENT_TIME AS DATETIME) AS APPOINTMENT_TIME,
  APPOINTMENT_RELEASED,
  APPOINTMENT_RELEASE_REASON,
  LEGACY_CLUB_CODE,
  INSPECTION_PERFORMED,
  CREATED_BY,
  MEMBER_EMAIL,
  BATTERY_IS_INSTALL,
  BATTERY_WARRANTY_REPLACEMENT,
  BATTERY_CORRECT_BATTERY_INSTALLED,
  BATTERY_PASSED_TEST,
  BATTERY_IS_PRORATED,
  NEW_BATTERY_IS_AAA_BATTERY,
  CURRENT_DATETIME() AS INSERT_DTTIME
FROM
  LANDING.WORK_ERS_REPAIR_SHOP_PORTAL