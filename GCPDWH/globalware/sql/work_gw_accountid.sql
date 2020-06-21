SELECT DISTINCT 
TRIM(CONVERT(CHAR,AccountId)) AS ACCOUNT_ID,
TRIM(CONVERT(CHAR,AccountType)) AS ACCOUNT_TYPE,
TRIM(CONVERT(CHAR,CustType)) AS CUST_TYPE,
TRIM(CONVERT(CHAR,ShortCode)) AS SHORT_CD,
TRIM(CONVERT(CHAR,SortName)) AS SORT_NM,
TRIM(CONVERT(CHAR,Name)) AS NAME,
TRIM(CONVERT(CHAR,Addr1)) AS ADDR1,
TRIM(CONVERT(CHAR,Addr2)) AS ADDR2,
TRIM(CONVERT(CHAR,Addr3)) AS ADDR3,
TRIM(CONVERT(CHAR,City)) AS CITY,
TRIM(CONVERT(CHAR,State)) AS STATE,
TRIM(CONVERT(CHAR,Zip)) AS ZIP,
TRIM(CONVERT(CHAR,Country)) AS COUNTRY,
TRIM(CONVERT(CHAR,EMail)) AS EMAIL,
TRIM(CONVERT(CHAR,BusPhone)) AS BUS_PHN,
TRIM(CONVERT(CHAR,FaxPhone)) AS FAX_PHN,
TRIM(CONVERT(CHAR,HomePhone)) AS HOME_PHN,
TRIM(CONVERT(CHAR,DateOpen,120)) AS DATE_OPEN,
TRIM(CONVERT(CHAR,DateChanged,120)) AS DATE_CHANGED,
TRIM(CONVERT(CHAR,Tag)) AS TAG,
TRIM(CONVERT(CHAR,Memo,3155)) AS MEMO,
TRIM(CONVERT(CHAR,ComPct)) AS COMPCT
FROM DBA.ACCOUNTID
WHERE  CONVERT(DATE,DateChanged) >= CONVERT(DATE,GETDATE()) -1
 