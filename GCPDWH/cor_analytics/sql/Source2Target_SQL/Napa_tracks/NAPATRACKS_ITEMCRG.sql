SELECT
  CAST(ITEMCRGID AS INT64) ITEM_CRG_ID,
  CAST(ITEMID AS INT64) ITEM_ID,
  CAST(ITEMORGID AS INT64) ITEM_ORG_ID,
  ITEMTYPE AS ITEM_TYPE,
  ITEM,
  DESCRIPTION,
  TAX,
  NOTE,
  CATEGORY,
  CAST(COST AS FLOAT64) COST,
  CAST(LIST AS FLOAT64) LIST,
  CONDITION,
  DISPLAYITEM AS DISPLAY_ITEM,
  CAST(QTY AS FLOAT64) QTY,
  MFG
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_ITEMCRG`