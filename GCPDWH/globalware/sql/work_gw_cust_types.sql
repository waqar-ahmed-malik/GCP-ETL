SELECT 
TRIM(CONVERT(CHAR,Code)) AS CD,
TRIM(CONVERT(CHAR,"Desc")) AS DESCRIPTION,
TRIM(CONVERT(CHAR,CreateMarketId)) AS CREATE_MARKET_ID,
TRIM(CONVERT(CHAR,TypeForMarketID)) AS TYPE_FOR_MARKET_ID,
TRIM(CONVERT(CHAR,ExcludeFromAccountId)) AS EXCLUDE_FROM_ACCOUNT_ID
FROM  DBA.CustTypes