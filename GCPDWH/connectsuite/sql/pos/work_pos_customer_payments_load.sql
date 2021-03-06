SELECT
LTRIM(RTRIM( ASSOC_CD)) AS ASSOC_CD,
LTRIM(RTRIM( CLB_CD)) AS CLB_CD,
LTRIM(RTRIM( OFC_ID)) AS OFC_ID,
LTRIM(RTRIM(CONVERT(CHAR, rcpt.SALE_ID))) AS SALE_ID,
LTRIM(RTRIM(CONVERT(CHAR, CUST_RECPT_NR))) AS CUST_RECPT_NR,
LTRIM(RTRIM(CONVERT(CHAR, CUST_ID,121))) AS CUST_ID,
LTRIM(RTRIM(CONVERT(CHAR, CUST_VST_DT,121))) AS CUST_VST_DT,
LTRIM(RTRIM(CONVERT(CHAR, SALE_COMPLETE_DT,121))) AS SALE_COMPLETE_DT,
LTRIM(RTRIM(CONVERT(CHAR, ORD_FULFILLMENT_DT,121))) AS ORD_FULFILLMENT_DT,
LTRIM(RTRIM( ORD_FULFILLMENT_ST)) AS ORD_FULFILLMENT_ST,
LTRIM(RTRIM(CONVERT(CHAR, CUST_TYP_CD))) AS CUST_TYP_CD,
LTRIM(RTRIM( CUST_FST_NM)) AS CUST_FST_NM,
LTRIM(RTRIM( CUST_LST_NM)) AS CUST_LST_NM,
LTRIM(RTRIM( CUST_MID_INIT_NM)) AS CUST_MID_INIT_NM,
LTRIM(RTRIM(CONVERT(CHAR, CUST_PYMT_AMT))) AS CUST_PYMT_AMT,
LTRIM(RTRIM(CONVERT(CHAR, CUST_RECPT_TOT_SLS_AMT))) AS CUST_RECPT_TOT_SLS_AMT,
LTRIM(RTRIM(CONVERT(CHAR, CUST_RECPT_TAX_AMT))) AS CUST_RECPT_TAX_AMT,
LTRIM(RTRIM(CONVERT(CHAR, CUST_RECPT_SUBTOT_AMT))) AS CUST_RECPT_SUBTOT_AMT,
LTRIM(RTRIM(CONVERT(CHAR, CUST_RECPT_SAVINGS_AMT))) AS CUST_RECPT_SAVINGS_AMT,
LTRIM(RTRIM(CONVERT(CHAR, SHIPPING_AMT))) AS SHIPPING_AMT,
LTRIM(RTRIM(CONVERT(CHAR, HANDLING_AMT))) AS HANDLING_AMT,
LTRIM(RTRIM( WKST_ID)) AS WKST_ID,
LTRIM(RTRIM( CONSULTANT_ID)) AS CONSULTANT_ID,
LTRIM(RTRIM( CUST_RECPT_VOID_RET_IND)) AS CUST_RECPT_VOID_RET_IND,
LTRIM(RTRIM(CONVERT(CHAR, CNTY_TAX_AMT))) AS CNTY_TAX_AMT,
LTRIM(RTRIM(CONVERT(CHAR, LCL_TAX_AMT))) AS LCL_TAX_AMT,
LTRIM(RTRIM(CONVERT(CHAR, ST_TAX_AMT))) AS ST_TAX_AMT,
LTRIM(RTRIM(CONVERT(CHAR, OTH_TAX_AMT))) AS OTH_TAX_AMT,
LTRIM(RTRIM(CONVERT(CHAR, UPLOAD_TRANS_NR))) AS UPLOAD_TRANS_NR,
LTRIM(RTRIM(CONVERT(CHAR, SHIP_TO_ID))) AS SHIP_TO_ID,
TRANSLATE(TRANSLATE(TRANSLATE(LTRIM(RTRIM( ADDITIONAL_INFO_TXT)),CHAR(10),' '),CHAR(11),' '),CHAR(13),' ') AS ADDITIONAL_INFO_TXT,
LTRIM(RTRIM(CONVERT(CHAR, LAST_UPDATE,121))) AS LAST_UPDATE,
LTRIM(RTRIM( SUSPECIOUS)) AS SUSPECIOUS,
LTRIM(RTRIM( SUSPECIOUS_NOTE)) AS SUSPECIOUS_NOTE,
LTRIM(RTRIM(CONVERT(CHAR, CUST_RECPT_PROMOTIONS_AMT))) AS CUST_RECPT_PROMOTIONS_AMT,
LTRIM(RTRIM( CONTACT_OBJECT_ID)) AS CONTACT_OBJECT_ID,
LTRIM(RTRIM( EXTERNAL_ID)) AS EXTERNAL_ID,
LTRIM(RTRIM(CONVERT(CHAR, STATE_TAX_PERCENT))) AS STATE_TAX_PERCENT,
LTRIM(RTRIM(CONVERT(CHAR, COUNTY_TAX_PERCENT))) AS COUNTY_TAX_PERCENT,
LTRIM(RTRIM(CONVERT(CHAR, LOCAL_TAX_PERCENT))) AS LOCAL_TAX_PERCENT,
LTRIM(RTRIM(CONVERT(CHAR, OTHER_TAX_PERCENT))) AS OTHER_TAX_PERCENT,
CONVERT(CHAR,
    ISNULL(pmt.CASH_AMOUNT,
      0)) AS CASH_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.CHECK_AMOUNT,
      0)) AS CHECK_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.AMEX_AMOUNT,
      0)) AS AMEX_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.VISA_AMOUNT,
      0)) AS VISA_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.MASTERCARD_AMOUNT,
      0)) AS MASTERCARD_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.DISCOVER_AMOUNT,
      0)) AS DISCOVER_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.DEBIT_AMOUNT,
      0)) AS DEBIT_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.CASH_NET,
      0)) AS CASH_NET,
  CONVERT(CHAR,
    ISNULL(pmt.CHECK_NET,
      0)) AS CHECK_NET,
  CONVERT(CHAR,
    ISNULL(pmt.CC_NET,
      0)) AS CC_NET,
  CONVERT(CHAR,
    ISNULL(pmt.MISC_AMOUNT,
      0)) AS MISC_AMT,
  CONVERT(CHAR,
    ISNULL(pmt.MISC_NET,
      0)) AS MISC_NET,
  CONVERT(CHAR,
    ISNULL(pmt.TENDER_TOTAL,
      0)) AS TENDER_TOTAL
FROM
  pos_prd.dbo.cust_recpt rcpt
LEFT OUTER JOIN (
  SELECT
    SALE_ID,
    SUM(CASH_AMOUNT) AS CASH_AMOUNT,
    SUM(CHECK_AMOUNT) AS CHECK_AMOUNT,
    SUM(AMEX_AMOUNT) AS AMEX_AMOUNT,
    SUM(VISA_AMOUNT) AS VISA_AMOUNT,
    SUM(MASTERCARD_AMOUNT) AS MASTERCARD_AMOUNT,
    SUM(DISCOVER_AMOUNT) AS DISCOVER_AMOUNT,
    SUM(DEBIT_AMOUNT) AS DEBIT_AMOUNT,
    SUM(CASH_NET) AS CASH_NET,
    SUM(CHECK_NET) AS CHECK_NET,
    SUM(CC_NET) AS CC_NET,
    SUM(MISC_AMOUNT) AS MISC_AMOUNT,
    SUM(MISC_NET) AS MISC_NET,
    SUM(CASH_AMOUNT)+SUM(CHECK_AMOUNT)+SUM(DEBIT_AMOUNT)+SUM(AMEX_AMOUNT)+SUM(VISA_AMOUNT)+SUM(MASTERCARD_AMOUNT)+SUM(DISCOVER_AMOUNT)+SUM(MISC_AMOUNT) AS TENDER_TOTAL
  FROM (
    SELECT
      SALE_ID,
      OFC_ID,
      CLB_CD,
      CAST (CUST_PYMT_DT AS date ) AS CUST_PYMT_DT,
      CUST_PYMT_TYP_ID,
      CASE
        WHEN CUST_PYMT_TYP_ID=231 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS CASH_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID=232 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS CHECK_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID=222 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS AMEX_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID=227 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS VISA_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID=225 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS MASTERCARD_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID IN (401, 403) THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS DISCOVER_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID=481 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS DEBIT_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID=231 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS CASH_NET,
      CASE
        WHEN CUST_PYMT_TYP_ID=232 THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS CHECK_NET,
      CASE
        WHEN CUST_PYMT_TYP_ID IN (222, 225, 227, 481, 401, 403) THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS CC_NET,
      CASE
        WHEN CUST_PYMT_TYP_ID NOT IN (231, 232, 222, 227, 225, 401, 403, 481) THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS MISC_AMOUNT,
      CASE
        WHEN CUST_PYMT_TYP_ID NOT IN (231, 232, 222, 227, 225, 401, 403, 481) THEN SUM(CUST_PYMT_AMT)
        ELSE 0
      END AS MISC_NET
    FROM
      pos_prd.dbo.CUST_PYMT
    GROUP BY
      SALE_ID,
      OFC_ID,
      CLB_CD,
      CAST (CUST_PYMT_DT AS date ),
      CUST_PYMT_TYP_ID ) a
  GROUP BY
    SALE_ID) pmt
ON
  pmt.sale_id= rcpt.sale_id
WHERE

CONVERT(CHAR,rcpt.LAST_UPDATE,127) >= 'max_date' -- Changed the Incremental date column and added extra condition
and SALE_COMPLETE_DT is not null

-- Applied the trim function to all required columns