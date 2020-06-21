SELECT
  CAST(ROID AS INT64) RO_ID,
  CAST(SEQNUM AS INT64) SEQ_NUM,
  CAST(INDENTLEVEL AS INT64) INDENT_LEVEL,
  CAST(ITEMFORMAT AS INT64) ITEM_FORMAT,
  CAST(ITEMID AS INT64) ITEM_ID,
  MFG,
  DESCRIPTION,
  UOM,
  CAST(QTY AS FLOAT64) QTY,
  NOTE,
  ITEM,
  ITEMTYPE AS ITEM_TYPE,
  TAX,
  MITCHELL,
  ASSETACCT AS ASSET_ACCT,
  COSTACCT AS COST_ACCT,
  SALESACCT AS SALES_ACCT,
  BASIS,
  CAST (BASISPCT AS INT64) BASIS_PCT,
  CAST(COST AS FLOAT64) COST,
  CAST (LIST AS FLOAT64)LIST,
  CAST (PRICE AS FLOAT64)PRICE,
  CATEGORY,
  CAST(RATEID AS INT64)RATE_ID,
  RATENAME AS RATE_NAME,
  TECH,
  CAST(BILLEDHRS AS FLOAT64)BILLED_HRS,
  COSTOVERRIDE AS COST_OVERRIDE,
  LISTOVERRIDE AS LIST_OVERRIDE,
  PRICEOVERRIDE AS PRICE_OVERRIDE,
  CONDITION,
  VENDOR,
  CAST(PRINTTOTAL AS FLOAT64) PRINT_TOTAL,
  CALCSUPPLIES AS CALC_SUPPLIES,
  CAST(TOTALCOST AS FLOAT64) TOTAL_COST,
  CAST(PRICEWODISCOUNT AS FLOAT64)PRICE_WO_DISCOUNT,
  TIREYN AS TIRE_YN,
  TIRESIZE AS TIRE_SIZE,
  ALTPARTNUMBER AS ALT_PART_NUMBER,
  TAMSMESSAGE AS TAMS_MESSAGE
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_ROITEMSUB6`