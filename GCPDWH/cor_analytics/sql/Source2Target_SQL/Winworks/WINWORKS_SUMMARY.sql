SELECT
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    W_DATE) AS W_DATE,
  CAST (STATUS AS int64) AS STATUS,
  CAST (CASH_DRAWER AS int64) AS CASH_DRAWER,
  CAST (LABOR_CHARGE AS FLOAT64) AS LABOR_CHARGE,
  CAST (LABOR_COST AS FLOAT64) AS LABOR_COST,
  CAST (LABOR_TAX AS FLOAT64) AS LABOR_TAX,
  CAST (PARTS_CHARGE AS FLOAT64) AS PARTS_CHARGE,
  CAST (PARTS_COST AS FLOAT64) AS PARTS_COST,
  CAST (PARTS_TAX AS FLOAT64) AS PARTS_TAX,
  CAST (SUBLET_CHARGE AS FLOAT64) AS SUBLET_CHARGE,
  CAST (SUBLET_COST AS FLOAT64) AS SUBLET_COST,
  CAST (HAZ_WASTE AS int64) AS HAZ_WASTE,
  CAST (OVERHEAD AS int64) AS OVERHEAD,
  CAST (WO_ACTIVITY AS int64) AS WO_ACTIVITY,
  CAST (INVOICE_ACTIVITY AS int64) AS INVOICE_ACTIVITY,
  CAST (CASH_IN AS FLOAT64) AS CASH_IN,
  CAST (CHECK_IN AS FLOAT64) AS CHECK_IN,
  CAST (ATM_IN AS FLOAT64) AS ATM_IN,
  CAST (MCARD_IN AS FLOAT64) AS MCARD_IN,
  CAST (VISA_IN AS FLOAT64) AS VISA_IN,
  CAST (AMEX_IN AS FLOAT64) AS AMEX_IN,
  CAST (DISC_IN AS FLOAT64) AS DISC_IN,
  CAST (ACCT_IN AS FLOAT64) AS ACCT_IN,
  CAST (OBCI_N AS FLOAT64) AS OBCI_N,
  CAST (OTHER_IN AS FLOAT64) AS OTHER_IN,
  CAST (WIP_LABOR AS FLOAT64) AS WIP_LABOR,
  CAST (IND_LABOR AS FLOAT64) AS IND_LABOR,
  CAST (AR_PAYMENTS AS FLOAT64) AS AR_PAYMENTS,
  CAST (SUPPLIES AS FLOAT64) AS SUPPLIES,
  CAST (RESTOCK AS FLOAT64) AS RESTOCK,
  CAST (FREIGHT AS FLOAT64) AS FREIGHT,
  CAST (MISC1 AS FLOAT64) AS MISC1,
  CAST (MISC2 AS FLOAT64) AS MISC2,
  CAST (NON_TAXABLE AS FLOAT64) AS NON_TAXABLE,
  CAST (ADJ_IN AS FLOAT64) AS ADJ_IN,
  CAST (UNITS AS int64) AS UNITS,
  CAST (SENT_TO_QB AS int64) AS SENT_TO_QB,
  PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p',
    CREATE_DTTIME) AS CREATE_DTTIME,
  CREATE_BY,
  SOURCE_SYSTEM_CD
FROM
  `aaadata-181822.COR_WINWORKS.STG_SUMMARY`