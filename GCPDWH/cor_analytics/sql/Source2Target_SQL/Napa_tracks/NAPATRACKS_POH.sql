SELECT
  CAST (POID AS INT64) AS PO_ID,
  CAST(VENDID AS INT64)AS VEND_ID,
  EMPLCODE AS EMPL_CODE,
  NOTES,
  PARSE_DATETIME('%m/%d/%Y %H:%M %p',
    CREATEDATE) CREATE_DATE,
  PARSE_DATETIME('%m/%d/%Y %H:%M %p',
    RECEIVEDATE)RECEIVE_DATE,
  PARSE_DATETIME('%m/%d/%Y %H:%M %p',
    CANCELDATE)CANCEL_DATE,
  AUTHOR,
  AUTHORSTATION,
  POSTATUS AS POST_STATUS,
  CAST (ROID AS INT64) RO_ID,
  VENDNAME AS VEND_NAME,
  POTYPENAME AS PO_TYPE_NAME,
  CAST (XMITTED AS INT64) XMITTED,
  APACCT AS AP_ACCT,
  CAST(TOTALCOST AS FLOAT64) TOTAL_COST,
  CAST(TOTALRECV AS FLOAT64) TOTAL_RECV,
  CAST(PONUM AS INT64) PO_NUM,
  CAST(PRINTDEALERHEADERYN AS INT64) PRINT_DEALER_HEADER_YN,
  PARSE_DATETIME('%m/%d/%Y %H:%M %p',
    ORDERDATE)ORDER_DATE,
  CAST (NEGPOID AS INT64) NEG_PO_ID,
  INVOICENUMBER AS INVOICE_NUMBER,
  POBILLTYPE AS PO_BILL_TYPE
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_POH`