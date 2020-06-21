  ## Code to create v1 of Peak Season Campaign Dash.
  ## This code only attributes revenue/metrics to sessions with a campaign entrance.  v2 should attribute for at least full day or until next campaign entrance.
  ## Also want to stitch session entrances across midnight with prev day, and fix Paypal attribution issue
  ## Need to expand stamps
CREATE TEMP FUNCTION
  formatRevenue(x INT64) AS ( ROUND(COALESCE(x,
        0) / 1E6, 2));
CREATE OR REPLACE TABLE
  `usps-digital-sandbox-246219.peak_dash.holiday_2019_full_campaigns_v1` AS (
  WITH
    PARAMS AS (
    SELECT
      #DATE_SUB(@run_date, INTERVAL 1 DAY) AS PROCESS_DATE
      DATE(2019, 10, 1) AS MIN_DATE,
      DATE(2019, 12, 31) AS MAX_DATE ),
    CAMPAIGN_LIST AS (
    SELECT
      campaigns
    FROM
      UNNEST( ['holidaydigital19', 'holidayaddressable19', 'holidayid19', 'holidaynational19', 'holidaypostalstore19', 'holidaystamps19', 'holidaymail19', 'holidaycns19', 'holidaydigital19es', 'holidaypr19', 'holidaygu19', 'Equity_Consumer_Brand_Holiday_Broad', 'Equity_Consumer_Brand_Holiday_BMM', 'Equity_Consumer_Brand_Holiday_Phrase', 'Equity_Consumer_Brand_Holiday_Exact', 'Equity_Consumer_NonBrand_Holiday_Broad', 'Equity_Consumer_NonBrand_Holiday_BMM', 'Equity_Consumer_NonBrand_Holiday_Phrase', 'Equity_Consumer_NonBrand_Holiday_Exact', 'Equity_Consumer_NonBrand_Audience_Holiday_Broad', 'Equity_Consumer_NonBrand_Audience_Holiday_BMM' ]) campaigns ),
    STAMP_LIST AS (
    SELECT
      skus
    FROM
      UNNEST([ '556204' 	#Eid Greetings
        , '556604' 	#Kwanzaa
        , '559904' 	#Hanukkah
        , '565604' 	#Hanukkah (Joint Issue with Israel Post)
        , '565804' 	#Kwanzaa
        , '565904' 	#Global:  Poinsettia
        , '588804' 	#Diwali
        , '677304' 	#Christmas Carols
        , '681304' 	#Florentine Madonna and Child
        , '681604'	#Winter Berries
        , '682104'	#Sparkling Holidays
        , '682204' 	#"Madonna and Child" by Bachiacca
        , '682404'	#Holiday Wreaths
        ]) skus ),
    A AS (
    SELECT
      * EXCEPT(hits),
      ARRAY(
      SELECT
        AS STRUCT *,
        (
        SELECT
          MAX(value)
        FROM
          UNNEST(h.customDimensions)
        WHERE
          index=2) queryString,
        (
        SELECT
          MAX(value)
        FROM
          UNNEST(h.customDimensions)
        WHERE
          index=1) docReferrer
      FROM
        UNNEST(hits) h) hits
    FROM
      `usps-digital-sandbox-246219.ga_holiday_campaign_sessions.ga_sessions_v1`
    WHERE
      #DATE = (SELECT PROCESS_DATE FROM PARAMS)
      DATE BETWEEN (
      SELECT
        MIN_DATE
      FROM
        PARAMS)
      AND (
      SELECT
        MAX_DATE
      FROM
        PARAMS) #note this is a date-partitioned table
      AND trafficSource.campaign IN (
      SELECT
        campaigns
      FROM
        CAMPAIGN_LIST)
      AND (trafficSource.isTrueDirect IS NULL) )
  SELECT
    date,
    device.deviceCategory,
    trafficSource.campaign,
    trafficSource.source,
    trafficSource.medium,
    trafficSource.adContent,
    (
    SELECT
      page.pagePath
    FROM
      UNNEST(hits)
    WHERE
      hitNumber=1) AS landingPage,
    (
    SELECT
      REGEXP_EXTRACT(queryString, r"iom=([^&]*)")
    FROM
      UNNEST(hits)
    WHERE
      hitNumber=1) AS iom,
    (
    SELECT
      REGEXP_EXTRACT(docReferrer, r"www.(usps\.com/[^/?]*)")
    FROM
      UNNEST(hits)
    WHERE
      hitNumber=1) AS vanity,
    (
    SELECT
      REGEXP_EXTRACT(docReferrer, r"://([^/]*)")
    FROM
      UNNEST(hits)
    WHERE
      hitNumber=1) AS landingReferrerHost,
    COUNT(1) AS sessions,
    formatRevenue(SUM(totals.totalTransactionRevenue)) AS totalTransactionRevenue,
    formatRevenue(SUM((
        SELECT
          SUM(h.transaction.transactionRevenue)
        FROM
          UNNEST(hits) h ))) AS rev_total,
    formatRevenue(SUM((
        SELECT
          SUM(h.transaction.transactionRevenue)
        FROM
          UNNEST(hits) h
        WHERE
          REGEXP_CONTAINS(h.page.pagePath, r'store.usps.com')))) AS rev_store,
    formatRevenue(SUM((
        SELECT
          SUM(h.transaction.transactionRevenue)
        FROM
          UNNEST(hits) h
        WHERE
          REGEXP_CONTAINS(h.page.pagePath, r'cns.usps.com')))) AS rev_cns,
    formatRevenue(SUM((
        SELECT
          SUM((
            SELECT
              SUM(p.productRevenue)
            FROM
              UNNEST(h.product) p
            WHERE
              p.productSKU IN (
              SELECT
                skus
              FROM
                STAMP_LIST) ))
        FROM
          UNNEST(hits) h))) AS rev_holiday_stamps,
    SUM((
      SELECT
        COUNT(DISTINCT h.transaction.transactionId)
      FROM
        UNNEST(hits) h )) AS n_trans,
    SUM((
      SELECT
        COUNT(DISTINCT h.transaction.transactionId)
      FROM
        UNNEST(hits) h
      WHERE
        REGEXP_CONTAINS(h.page.pagePath, r'store.usps.com'))) AS n_trans_store,
    SUM((
      SELECT
        COUNT(DISTINCT h.transaction.transactionId)
      FROM
        UNNEST(hits) h
      WHERE
        REGEXP_CONTAINS(h.page.pagePath, r'cns.usps.com'))) AS n_trans_cns,
    SUM((
      SELECT
        COUNT(DISTINCT h.transaction.transactionId)
      FROM
        UNNEST(hits) h
      WHERE
        REGEXP_CONTAINS(h.page.pagePath, r'store.usps.com')
        AND h.transaction.transactionRevenue IS NULL)) AS n_trans_free,
    SUM((
      SELECT
        COUNT(DISTINCT h.transaction.transactionId)
      FROM
        UNNEST(hits) h
      WHERE
        REGEXP_CONTAINS(h.page.pagePath, r'store.usps.com')
        AND EXISTS(
        SELECT
          1
        FROM
          UNNEST(h.product) p
        WHERE
          p.productSku IN (
          SELECT
            skus
          FROM
            STAMP_LIST)) )) AS n_trans_holiday_stamps,
    COUNTIF(EXISTS(
      SELECT
        1
      FROM
        UNNEST(hits)
      WHERE
        page.pagePath IN ('reg.usps.com/entreg/secure/registrationsuccessaction_input',
          'reg.usps.com/entreg/secure/registrationportalsuccessaction_input') )) AS n_account_creations,
    COUNTIF(EXISTS(
      SELECT
        1
      FROM
        UNNEST(hits)
      WHERE
        page.pagePath IN ('reg.usps.com/entreg/secure/registrationcompletesuccessaction_input',
          'reg.usps.com/entreg/secure/registrationidbmsuccessaction_input')
        OR ( (page.pagePath = 'reg.usps.com/entreg/secure/identityconfirmaction_input'
            OR (page.pagePath,
              queryString) = ('reg.usps.com/entreg/secure/registrationportalsuccessaction_input',
              '?ivs=true'))
          AND docReferrer IN ( 'https://ips.usps.com/ipsweb/4verification_questions.xhtml',
            'https://ips.usps.com/ipsweb/verification_successful.xhtml',
            'https://ips.usps.com/ipsweb/verification_enter_passcode.xhtml') ) )) AS n_id_signups #note the idmbsuccess page for ID code may also imply an account creation, so these may be undercounted currently
    ,
    SUM((
      SELECT
        COUNT(DISTINCT transaction.transactionId)
      FROM
        UNNEST(hits)
      WHERE
        eventInfo.eventCategory = 'Ecommerce'
        AND eventInfo.eventAction = 'Purchase'
        AND eventInfo.eventLabel = 'Hold Mail' )) AS n_trans_holdmail
  FROM
    A
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10
  HAVING
    landingPage IS NOT NULL )