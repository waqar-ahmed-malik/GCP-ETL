CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.POS_TRANSACTION_DETAIL_DIM 
AS (
SELECT DISTINCT 
  CUST_RECPT.SALE_COMPLETE_DTTIME  AS TRANSACTION_DTTIME,
  CUST_RECPT.SALE_ID AS TRANSACTION_NUM,
  CUST_RECPT.SALE_ID AS POS_TRANSACTION_ID,
  CUST_RECEIPT_LN_ITM_ID AS ITEM_NUM,
  '' AS LINE_ITEM_VOID_ITEM_NUM,
  '' AS RETURN_TRANSACTION_NUM,
  CUST_RECPT.OFFICE_ID AS LOCATION_ID,
  '' AS REGISTER_NUM,
  CASE
    WHEN REGEXP_CONTAINS(TRIM(CONSULTANT_ID), "[a-zA-Z]") THEN ENT.EMPLOYEE_ID
    ELSE EMP_ID.EMPLOYEE_ID
  END AS EMPLOYEE_ID,
  CASE WHEN REGEXP_CONTAINS(TRIM(CONSULTANT_ID), "[a-zA-Z]") THEN ENT.EMPLOYEE_NAME
       ELSE EMP_ID.EMPLOYEE_NAME END AS EMPLOYEE_NM,
  '' AS ENTRY_METHOD,
  ITEM_TYPE_TX AS ITEM_DESCRIPTION,
  ITEM_TYPE_ID AS ITEM_SKU,
  '' AS JPOINT_ITEM_SKU,
  CASE
    WHEN PRODUCT_CATALOG.COMPANY IS NOT NULL THEN PRODUCT_CATALOG.COMPANY
    ELSE ' '
  END AS SKU_COMPANY,
  CASE
    WHEN TRIM(UPPER(ITEM.ITEM_TYPE_ID)) IN ('CA-UT WU PROP-OFF',  'NV WU PROP-OFF',  'WU AUTO-OFFLINE',  'CA PUP-OFFLINE') THEN CONCAT('insurerName=',"",IFNULL(TRIM(PRODUCT_CATALOG.INSURER_NAME),  ""),'|riskState=',IFNULL(TRIM(PRODUCT_CATALOG.RISK_STATE),  ""),'|productTypeCode=',IFNULL(TRIM(PRODUCT_CATALOG.PRODUCT_TYPE_CD),  ""),'|sourceSystem=',IFNULL(CASE
        WHEN TRIM(UPPER(PRODUCT_CATALOG.SOURCE_SYSTEM))='EXIGEN' THEN 'PAS'
        WHEN TRIM(UPPER(PRODUCT_CATALOG.SOURCE_SYSTEM))='PUP' THEN 'PUPSYS'
        ELSE TRIM(PRODUCT_CATALOG.SOURCE_SYSTEM) END,
      ""),'|policyPrefix=',IFNULL(TRIM(PRODUCT_CATALOG.POLICY_PREFIX),
      ""))
    ELSE CASE
    WHEN ITEM.COMPANY_ID IS NULL THEN NULL
    ELSE CONCAT('insurerName=',"",IFNULL(TRIM(ITEM.COMPANY_ID),
      ""),'|riskState=',IFNULL(TRIM(ITEM.STATE_CD),
      ""),'|productTypeCode=',IFNULL(TRIM(ITEM.PRODUCT_TYPE),
      ""),'|sourceSystem=',IFNULL(CASE
        WHEN TRIM(UPPER(ITEM.SOURCE_SYSTEM))='EXIGEN' THEN 'PAS'
        WHEN TRIM(UPPER(ITEM.SOURCE_SYSTEM))='PUP' THEN 'PUPSYS'
        ELSE TRIM(ITEM.SOURCE_SYSTEM) END,
      ""),'|policyPrefix=',IFNULL(TRIM(ITEM.PRODUCT_PREFIX),
      ""))
  END END AS ITEM_PASSTHROUGH_DATA,
  ITEM.ADDITIONAL_TXT AS ITEM_SPECIAL_ORDER_NUM,
  CASE
    WHEN UPPER(SUBSTR(CUST_RECPT.CUST_ID,1,1)) = 'M' THEN CONCAT('429',"",SUBSTR(CUST_RECPT.CUST_ID,2,12))
    ELSE NULL
  END AS MEMBER_NUM_15,
  QUANTITY AS ITEM_SALE_QUANTITY,
  ROUND(CASE
    WHEN ITEM.LIST_PRC = 0 THEN ITEM.SALE_PRC
    ELSE ITEM.LIST_PRC
  END,2) AS UNIT_PRICE,
  ROUND(CASE
    WHEN PRODUCT_CATALOG.Taxable='Y'
  THEN CUST_RECPT.CUST_RECPT_TAX_AMT/TQ.TAXABLE_QTY
    ELSE 0
  END,2) AS ITEM_TAX_AMT,
  ROUND(CASE
    WHEN ITEM.LIST_PRC - ITEM.SALE_PRC < 0 THEN 0
    ELSE ITEM.LIST_PRC - ITEM.SALE_PRC
  END,2) AS ITEM_DISCOUNT_AMT,
  ITEM.SALE_PRC * ITEM.QUANTITY +
  CASE
    WHEN PRODUCT_CATALOG.Taxable='Y'
  THEN round(CUST_RECPT.CUST_RECPT_TAX_AMT/TQ.TAXABLE_QTY,2)
    ELSE 0
  END AS TOTAL_ITEM_SALES_AMT,
  CASE WHEN CUST_RECPT_VD_RET_IND='V' THEN 'Y' ELSE 'N' END AS LINE_ITEM_VOID_FLAG,
  CASE WHEN UPPER(CUST_RECPT.CUST_RECPT_VOID_RET_IND)='V' THEN 'Y' ELSE 'N' END AS FULL_TICKET_VOID_FLAG,
  CASE WHEN CUST_RECPT_VD_RET_IND='R' THEN 'Y' ELSE 'N' END AS RETURN_FLAG,
  CUST_RECPT.SALE_COMPLETE_DTTIME  as CREATE_DTTIME
FROM
  OPERATIONAL.POS_CUSTOMER_PAYMENTS AS CUST_RECPT
INNER JOIN
  OPERATIONAL.POS_CUSTOMER_RECEIPT_LINE_ITEMS AS ITEM
ON
  CUST_RECPT.SALE_ID=ITEM.SALE_ID
INNER JOIN
  REFERENCE.CONNECTSUITE_PRODUCT_DIM AS PRODUCT_CATALOG
ON
  UPPER(TRIM(ITEM.ITEM_TYPE_ID))=UPPER(PRODUCT_CATALOG.SKU)
  AND UPPER(PRODUCT_CATALOG.SKU_TYPE)='TRANSACTION'
LEFT OUTER JOIN (
  SELECT
    ITEM.SALE_ID AS SALE_ID,
    COUNT(*) AS TAXABLE_QTY
  FROM
    OPERATIONAL.POS_CUSTOMER_RECEIPT_LINE_ITEMS ITEM,
    REFERENCE.CONNECTSUITE_PRODUCT_DIM PC
  WHERE
    UPPER(TRIM(ITEM.ITEM_TYPE_ID))=UPPER(PC.SKU)
    AND UPPER(PC.SKU_TYPE)='TRANSACTION'
    AND TAXABLE='Y'
  GROUP BY
    SALE_ID ) TQ
ON
  CUST_RECPT.SALE_ID=TQ.SALE_ID
LEFT OUTER JOIN (
  SELECT
    DISTINCT TRIM(EMPLOYEE_ID) AS EMPLOYEE_ID,
    CONCAT(TRIM(LEGAL_FIRST_NM)," ",TRIM(LEGAL_LAST_NM)) AS EMPLOYEE_NAME
  FROM
    CUSTOMER_PRODUCT.EMPLOYEE_DIM
  WHERE
    ACTIVE_FLG='Y') AS EMP_ID
ON
  TRIM(CUST_RECPT.CONSULTANT_ID) = TRIM(EMP_ID.EMPLOYEE_ID)
LEFT OUTER JOIN (
  SELECT
    DISTINCT TRIM(EMPLOYEE_ID) AS EMPLOYEE_ID,
    TRIM(ENT_ID) AS ENT_ID,
    CONCAT(TRIM(LEGAL_FIRST_NM)," ",TRIM(LEGAL_LAST_NM)) AS EMPLOYEE_NAME
  FROM
    CUSTOMER_PRODUCT.EMPLOYEE_DIM
  WHERE
    ACTIVE_FLG='Y') AS ENT
ON
  TRIM(CUST_RECPT.CONSULTANT_ID) = TRIM(ENT.ENT_ID))
