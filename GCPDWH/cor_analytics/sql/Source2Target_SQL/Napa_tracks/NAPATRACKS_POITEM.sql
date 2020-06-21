SELECT
  CAST(POID AS INT64)PO_ID,
  CAST(SEQNUM AS INT64)SEQ_NUM,
  CAST(ITEMID AS INT64) ITEM_ID,
  CAST(QTY AS FLOAT64) QTY,
  CAST(COST AS FLOAT64) COST,
  CAST(ACKQTY AS FLOAT64) ACK_QTY,
  ITEM,
  DESCRIPTION,
  MFG,
  UOM,
  CAST(BALQTY AS FLOAT64) BAL_QTY,
  CAST(RECVQTY AS FLOAT64) RECV_QTY,
  CAST(LIST AS FLOAT64) LIST,
  CAST(AVAILQTY AS FLOAT64) AVAIL_QTY,
  CAST(INDENTLEVEL AS INT64)INDENT_LEVEL,
  CAST(ITEMFORMAT AS INT64) ITEM_FORMAT,
  ITEMTYPE AS ITEM_TYPE,
  CAST(ORDERQTY AS FLOAT64) ORDER_QTY,
  CAST(ORDERTOTAL AS FLOAT64) ORDER_TOTAL,
  CAST(RECVTOTAL AS FLOAT64) RECV_TOTAL,
  ASSETACCT,
  TAMSMSG1,
  TAMSMSG2
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_POITEM`