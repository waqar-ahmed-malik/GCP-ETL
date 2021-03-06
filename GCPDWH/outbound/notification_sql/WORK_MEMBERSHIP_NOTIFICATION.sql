SELECT
  MEMBERSHIP_ID,
  FIRST_NAME,
  LAST_NAME,
  MIDDLE_INITIAL,
  ARIA_ACCOUNT_ID,
  MEMBER_SEQUENCE_NUM,
  MEMBER_NUM,
  MEMBERSHIP_NUM,
  MEMBER_SINCE,
  TERM_EXPIRATION_DT,
  PLAN,
  EVENT_TYPE,
  -- GIFTED_BY,
  CORPORATE_GROUP_TYPE,
  SOURCE_CREATE_DTTIME,
  SOURCE_LAST_UPDATE_DTTIME,
  -- NAME_ON_MEMBERSHIP_CARD,
  DAILY_EVENT_KEY,
  EVENT_DATE,
  PRM_BILLING_ADDRESS_LINE1,
  PRM_BILLING_CITY,
  PRM_BILLING_STATE,
  PRM_BILLING_ZIP,
  PRM_ARIA_ACCOUNT_ID,
  PRM_MEMBER_NUM,
  PRM_FIRST_NAME,
  PRM_LAST_NAME,
  PRM_MIDDLE_INITIAL,
  PRM_EMAIL_ADDRESS,
  PRM_HOME_PHONE_NUM,
  PRM_MEMBER_SINCE,
  PATTERN_ID,
  TRANSACTION_ID,
  ADDITIONAL_DETAILS,
  SOURCE_SYSTEM_CD
FROM (
  SELECT
    NOTF.MEMBERSHIP_ID,
    NOTF.FIRST_NAME,
    NOTF.LAST_NAME,
    NOTF.MIDDLE_INITIAL,
    NOTF.ARIA_ACCOUNT_ID,
    NOTF.MEMBER_SEQUENCE_NUM,
    NOTF.MEMBER_NUM,
    NOTF.MEMBERSHIP_NUM,
    NOTF.MEMBER_SINCE,
    NOTF.TERM_EXPIRATION_DT,
    NOTF.PLAN,
    NOTF.EVENT_TYPE,
    -- NOTF.GIFTED_BY,
    NOTF.CORPORATE_GROUP_TYPE,
    NOTF.SOURCE_CREATE_DTTIME,
    NOTF.SOURCE_LAST_UPDATE_DTTIME,
    --  NOTF.NAME_ON_MEMBERSHIP_CARD,
    NOTF.DAILY_EVENT_KEY,
    NOTF.EVENT_DATE,
    NOTF.PRM_BILLING_ADDRESS_LINE1,
    NOTF.PRM_BILLING_CITY,
    NOTF.PRM_BILLING_STATE,
    NOTF.PRM_BILLING_ZIP,
    NOTF.PRM_ARIA_ACCOUNT_ID,
    NOTF.PRM_MEMBER_NUM,
    NOTF.PRM_FIRST_NAME,
    NOTF.PRM_LAST_NAME,
    NOTF.PRM_MIDDLE_INITIAL,
    NOTF.PRM_EMAIL_ADDRESS,
    NOTF.PRM_HOME_PHONE_NUM,
    NOTF.PRM_MEMBER_SINCE,
    CONCAT(CASE
        WHEN NOTF.PRM_MEMBER_NUM IS NULL THEN '1'
        ELSE '0' END,'',
      CASE
        WHEN NOTF.MEMBERSHIP_ID <> NOTF.MBRS_ID THEN '1'
        ELSE '0' END,'','00000000','',
      CASE
        WHEN UPPER(NOTF.PLAN)='CLASSIC' AND NOTF.EVENT_TYPE IN ('RENEW-UPGRADE',  'UPGRADE') THEN '1'
        ELSE '0'END,'',
      CASE
        WHEN UPPER(NOTF.PLAN)='PREMIER' AND NOTF.EVENT_TYPE IN ('DOWNGRADE',  'RENEW-DOWNGRADE') THEN '1'
        ELSE '0' END,'','00000000') AS PATTERN_ID,
    NOTF.TRANSACTION_ID,
    CASE
      WHEN NOTF.MEMBER_NUM IS NULL THEN CONCAT('MEMBERSHIP_ID - ','', IFNULL(LPAD(NOTF.MEMBERSHIP_ID,  8,  '0'), ''))
      ELSE ''
    END AS ADDITIONAL_DETAILS,
    'CONNECT SUITE' AS SOURCE_SYSTEM_CD,
    ROW_NUMBER() OVER (PARTITION BY MEMBER_NUM ORDER BY EVENT_TYPE_NUM ASC) ROW_NUM
  FROM (
    SELECT
      TRNS.MEMBERSHIP_ID AS MEMBERSHIP_ID,
      TRNS.TRANSACTION_TYPE_CD AS EVENT_TYPE,
      TRNS.CREATE_DTTIME AS SOURCE_CREATE_DTTIME,
      TRNS.LAST_UPDATE_DTTIME AS SOURCE_LAST_UPDATE_DTTIME,
      TRNS.DAILY_EVENT_KEY,
      TRNS.TRANSACTION_ID,
      TRNS.TRANSACTION_EFFECTIVE_DT AS EVENT_DATE,
      CASE UPPER(TRNS.TRANSACTION_TYPE_CD)
        WHEN 'CREATE' THEN 1
        WHEN 'ADD' THEN 2
        WHEN 'UPGRADE' THEN 3
        WHEN 'DOWNGRADE' THEN 4
        WHEN 'REPLACE' THEN 5
        WHEN 'RENEW-UPGRADE' THEN 6
        WHEN 'RENEW-DOWNGRADE' THEN 7
        ELSE 9
      END AS EVENT_TYPE_NUM,
      PRM.FIRST_NAME AS FIRST_NAME,
      PRM.LAST_NAME AS LAST_NAME,
      PRM.MIDDLE_INITIAL AS MIDDLE_INITIAL,
      0 AS ARIA_ACCOUNT_ID,
      MSH.MEMBERSHIP_NUM AS MBRS_ID,
      -- Need to Check
      MEM.ASSOCIATE_ID AS MEMBER_SEQUENCE_NUM,
      MEM.MEMBER_NUM AS MEMBER_NUM,
      MSH.MEMBERSHIP_NUM AS MEMBERSHIP_NUM,
      MEM.MEMBER_SINCE AS MEMBER_SINCE,
      MSH.TERM_EXPIRATION_DT AS TERM_EXPIRATION_DT,
      UPPER(MSH.MEMBERSHIP_LEVEL_DESC) AS PLAN,
      -- MSH.GIFTED_BY                   AS GIFTED_BY,
      MEM.CORPORATE_GROUP_TYPE AS CORPORATE_GROUP_TYPE,
      --MEM.NAME_ON_MEMBERSHIP_CARD         AS NAME_ON_MEMBERSHIP_CARD,
      UPPER(PRM.PRM_BILLING_ADDRESS_LINE1) AS PRM_BILLING_ADDRESS_LINE1,
      UPPER(PRM.PRM_BILLING_CITY) AS PRM_BILLING_CITY,
      UPPER(PRM.PRM_BILLING_STATE) AS PRM_BILLING_STATE,
      PRM.PRM_BILLING_ZIP AS PRM_BILLING_ZIP,
      PRM.PRM_ARIA_ACCOUNT_ID,
      PRM.PRM_MEMBER_NUM,
      PRM.PRM_FIRST_NAME,
      PRM.PRM_LAST_NAME,
      PRM.PRM_MIDDLE_INITIAL,
      PRM.PRM_EMAIL_ADDRESS,
      PRM.PRM_HOME_PHONE_NUM,
      CASE
        WHEN MEM.ASSOCIATE_ID=1 THEN MEM.MEMBER_SINCE
        ELSE NULL
      END AS PRM_MEMBER_SINCE
    FROM (
      SELECT
        TRN.MEMBER_NUM AS MEMBER_NUM,
        TRN.MEMBER_KEY AS MEMBER_KEY,
        TRN.MEMBERSHIP_NUM AS MEMBERSHIP_ID,
        UPPER(TRN.TRANSACTION_TYPE_CD) AS TRANSACTION_TYPE_CD,
        TRN.TRANSACTION_DT AS CREATE_DTTIME,
        TRN.UPDATE_DTTIME AS LAST_UPDATE_DTTIME,
        TRN.SOURCE_TRANSACTION_ID AS DAILY_EVENT_KEY,
        TRN.TRANSACTION_EFFECTIVE_DT,
        TRN.SOURCE_TRANSACTION_ID AS TRANSACTION_ID -- Used the DLY_KY
      FROM
        CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_TRANSACTIONS_FACT TRN
      WHERE
        UPPER(TRN.TRANSACTION_TYPE_CD) IN ('CREATE',
          'ADD',
          'UPGRADE',
          'DOWNGRADE')
        AND UPPER(TRN.SOURCE_SYSTEM_CD) = 'CONNECT SUITE'
        AND TRANSACTION_EFFECTIVE_DT>=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) -- Incremntal date
        ) TRNS
    LEFT OUTER JOIN
      CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_DIM AS MEM
    ON
      TRNS.MEMBER_NUM=MEM.MEMBER_NUM
      AND MEM.ACTIVE_FLG='Y'
    LEFT OUTER JOIN
      CUSTOMER_PRODUCT.CONNECTSUITE_MEMBERSHIP_DIM AS MSH
    ON
      MEM.MEMBERSHIP_NUM = MSH.MEMBERSHIP_NUM
      AND MSH.ACTIVE_FLG='Y'
    LEFT OUTER JOIN (
      SELECT
        MEMBERSHIP_NUM AS MEMBERSHIP_ID,
        MEMBER_NUM AS PRM_MEMBER_NUM,
        FIRST_NM AS FIRST_NAME,
        LAST_NM AS LAST_NAME,
        MID_INITIAL_NM AS MIDDLE_INITIAL,
        CASE
          WHEN ASSOCIATE_ID=1 THEN FIRST_NM
          ELSE NULL
        END AS PRM_FIRST_NAME,
        CASE
          WHEN ASSOCIATE_ID=1 THEN LAST_NM
          ELSE NULL
        END AS PRM_LAST_NAME,
        CASE
          WHEN ASSOCIATE_ID=1 THEN MID_INITIAL_NM
          ELSE NULL
        END AS PRM_MIDDLE_INITIAL,
        EMAIL_ADDRESS AS PRM_EMAIL_ADDRESS,
        HOME_PHONE_NUM AS PRM_HOME_PHONE_NUM,
        '0' AS PRM_ARIA_ACCOUNT_ID,
        -- Need to confirm
        NULL PRM_MEMBER_SINCE,
        -- Need to Map
        CASE
          WHEN CURRENT_DATE() BETWEEN TEMPORARY_ADDRESS_START_DT AND TEMPORARY_ADDRESS_END_DT THEN CONCAT(IFNULL(TEMPORARY_ADDRESS_LINE1, ''),'',IFNULL(CASE
              WHEN TEMPORARY_ADDRESS_LINE2 IS NULL THEN NULL
              ELSE ', ' END,
            ''),'',IFNULL(TEMPORARY_ADDRESS_LINE2,
            ''))
          ELSE CONCAT(IFNULL(BILLING_ADDRESS_LINE1,
            ''),'',IFNULL(CASE
              WHEN BILLING_ADDRESS_LINE2 IS NULL THEN NULL
              ELSE ', ' END,
            ''),'', IFNULL(BILLING_ADDRESS_LINE2,
            ''))
        END AS PRM_BILLING_ADDRESS_LINE1,
        CASE
          WHEN CURRENT_DATE() BETWEEN TEMPORARY_ADDRESS_START_DT AND TEMPORARY_ADDRESS_END_DT THEN TEMPORARY_CITY
          ELSE BILLING_CITY
        END AS PRM_BILLING_CITY,
        CASE
          WHEN CURRENT_DATE() BETWEEN TEMPORARY_ADDRESS_START_DT AND TEMPORARY_ADDRESS_END_DT THEN TEMPORARY_STATE
          ELSE BILLING_STATE
        END AS PRM_BILLING_STATE,
        CASE
          WHEN CURRENT_DATE() BETWEEN TEMPORARY_ADDRESS_START_DT AND TEMPORARY_ADDRESS_END_DT THEN TEMPORARY_ZIP
          ELSE BILLING_ZIP
        END AS PRM_BILLING_ZIP
      FROM
        CUSTOMERS.MEMBERSHIP_CUSTOMER_DIM AS CUST_DIM
      WHERE
        CUST_DIM.ACTIVE_FLG='Y'
        --AND UPPER(SOURCE_SYSTEM_CD)='CONNECT SUITE'
        ) PRM
    ON
      PRM.PRM_MEMBER_NUM=TRNS.MEMBER_NUM
    WHERE
      MEM.STATUS_CD IN ('A',
        'P') ) NOTF)
WHERE
  ROW_NUM=1;