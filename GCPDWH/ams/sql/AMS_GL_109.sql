SELECT
   "1" AS JournalKey
  ,"COMPANY_REFERENCE_ID" AS CompanyReferenceIDType
  ,AGENCY_CD AS CompanyReferenceID
  ,"USD" AS Currency
  ,"ACTUALS" AS LedgerType
  ,CAST(NULL AS STRING) AS BookCode
  ,EFFECTIVE_DT AS AccountingDate
  ,"EPIC" AS JournalSource
  ,CAST(NULL AS STRING) AS BalancingWorktagReferenceIDType
  ,"CURRENT" AS CurrencyRateType
  ,CAST(NULL AS STRING) AS JournalEntryMemo
  ,CAST(NULL AS STRING) AS JournalExternalReferenceID
  ,ROW_NUMBER() OVER() AS JournalLineOrder
  ,"ACCOUNT_SET_ID" AS LedgerAccountReferenceID_ParentIDType
  ,"CORPORATE" AS LedgerAccountReferenceID_ParentID
  ,"LEDGER_ACCOUNT_ID" AS LedgerAccountReferenceIDType
  ,TRIM(IF(STRPOS(GL_ACCOUNT, '-')=0, GL_ACCOUNT, SUBSTR(GL_ACCOUNT, 0, STRPOS(GL_ACCOUNT, '-') - 1))) AS LedgerAccountReferenceID
  ,DEBIT_AMT AS DebitAmount
  ,CREDIT_AMT AS CreditAmount
  ,"USD" AS LineCurrency
  ,"COMPANY_REFERENCE_ID" AS LineCompanyReferenceIDType
  ,AGENCY_CD AS LineCompanyReferenceID
  ,CAST(NULL AS STRING) AS LineCurrencyRate
  ,CAST(NULL AS STRING) AS LedgerDebitAmount
  ,CAST(NULL AS STRING) AS LedgerCreditAmount
  ,GL_ENTRY_TYPE_DESC AS LineMemo
  ,CAST(NULL AS STRING) AS Worktag_Revenue_Category_ID
  ,CAST(NULL AS STRING) AS Worktag_Spend_Category_ID
  ,CAST(NULL AS STRING) AS Worktag_Cost_Center_Reference_ID
  ,IF(TRIM(BRANCH_CD)='', NULL, BRANCH_CD) AS Worktag_Location_ID
  ,CAST(NULL AS STRING) AS Worktag_Sales_Item_ID
  ,TRIM(IF(STRPOS(GL_ACCOUNT, '-')=0, NULL, SUBSTR(GL_ACCOUNT, STRPOS(GL_ACCOUNT, '-') + 1, LENGTH(GL_ACCOUNT) - STRPOS(GL_ACCOUNT, '-')))) AS SubAccount
FROM (
  SELECT
    AGENCY_CD, EFFECTIVE_DT, GL_ACCOUNT, GL_ENTRY_TYPE_DESC,
    BRANCH_CD, SUM(DEBIT_AMT) AS DEBIT_AMT, SUM(CREDIT_AMT) AS CREDIT_AMT
  FROM OPERATIONAL.AMS_GENERAL_LEDGER
  WHERE AGENCY_CD = '109'
  AND DATE(CREATE_DTTIME) = CURRENT_DATE()
  GROUP BY AGENCY_CD, EFFECTIVE_DT, GL_ACCOUNT, GL_ENTRY_TYPE_DESC, BRANCH_CD
)
ORDER BY JournalLineOrder
