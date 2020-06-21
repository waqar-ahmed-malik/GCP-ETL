SELECT DISTINCT 
TRIM(CONVERT(CHAR,PAYID) ) AS PAY_ID,
TRIM(CONVERT(CHAR,ACCOUNTID) ) AS ACCOUNT_ID,
TRIM(CONVERT(CHAR,REPORTTOID) ) AS REPORT_TO_ID,
TRIM(CONVERT(CHAR,INVOICENUMBER) ) AS INVOICE_NUM,
TRIM(CONVERT(CHAR,INVOICEDATE) ) AS INVOICE_DT,
TRIM(CONVERT(CHAR,SALETYPE) ) AS SALE_TYPE,
TRIM(CONVERT(CHAR,SETTLE) ) AS SETTLE,
TRIM(CONVERT(CHAR,TRAVELTYPE) ) AS TRAVEL_TYPE,
TRIM(CONVERT(CHAR,SALENUM) ) AS SALE_NUM,
TRIM(CONVERT(CHAR,CUSTTYPE) ) AS CUST_TYPE,
TRIM(CONVERT(CHAR,TRAVELER) ) AS TRAVELER,
TRIM(CONVERT(CHAR,BKAGT) ) AS BKAGT,
TRIM(CONVERT(CHAR,TKTAGT) ) AS TKTAGT,
TRIM(CONVERT(CHAR,SELLAGT) ) AS SELLAGT,
TRIM(CONVERT(CHAR,BRANCH) ) AS BRANCH,
TRIM(CONVERT(CHAR,STP) ) AS STP,
TRIM(CONVERT(CHAR,DEPARTDATE) ) AS DEPART_DT,
TRIM(CONVERT(CHAR,RETURNDATE) ) AS RETURN_DT,
TRIM(CONVERT(CHAR,BASEFARE) ) AS BASEFARE,
TRIM(CONVERT(CHAR,TAX1) ) AS TAX1,
TRIM(CONVERT(CHAR,TAX2) ) AS TAX2,
TRIM(CONVERT(CHAR,TAX3) ) AS TAX3,
TRIM(CONVERT(CHAR,TAX4) ) AS TAX4,
TRIM(CONVERT(CHAR,MISCCHARGE) ) AS MISCCHARGE,
TRIM(CONVERT(CHAR,DISCOUNT) ) AS DISCOUNT,
TRIM(CONVERT(CHAR,EXCHANGE) ) AS EXCHANGE,
TRIM(CONVERT(CHAR,TOTALCOST) ) AS TOTAL_COST,
TRIM(CONVERT(CHAR,COMMPERCENT) ) AS COMM_PERCENT,
TRIM(CONVERT(CHAR,COMMAMOUNT) ) AS COMM_AMOUNT,
TRIM(CONVERT(CHAR,FOP) ) AS FOP,
TRIM(CONVERT(CHAR,MAXFARE) ) AS MAXFARE,
TRIM(CONVERT(CHAR,LOWFARE) ) AS LOWFARE,
TRIM(CONVERT(CHAR,DOMINT) ) AS DOMINT,
TRIM(CONVERT(CHAR,DESTINATION) ) AS DESTINATION,
TRIM(CONVERT(CHAR,DOCTYPE) ) AS DOC_TYPE,
TRIM(CONVERT(CHAR,AIRLINE) ) AS AIRLINE,
TRIM(CONVERT(CHAR,TICKETNUM) ) AS TICKET_NUM,
TRIM(CONVERT(CHAR,CONJUNCT) ) AS CONJUNCT,
TRIM(CONVERT(CHAR,ITINERARY) ) AS ITINERARY,
TRIM(CONVERT(CHAR,PROVIDER) ) AS PROVIDER,
TRIM(CONVERT(CHAR,SAVINGSCODE) ) AS SAVINGS_CODE,
TRIM(CONVERT(CHAR,SAVINGSCOMMENT) ) AS SAVINGS_COMMENT,
TRIM(CONVERT(CHAR,SORT1) ) AS SORT1,
TRIM(CONVERT(CHAR,SORT2) ) AS SORT2,
TRIM(CONVERT(CHAR,BOOKDATE) ) AS BOOK_DT,
TRIM(CONVERT(CHAR,CHANGEDATE) ) AS CHANGE_DT,
TRIM(CONVERT(CHAR,SALESPOSTEDDATE) ) AS SALES_POSTED_DT,
TRIM(CONVERT(CHAR,SALESPOSTEDAMOUNT) ) AS SALES_POSTED_AMT,
TRIM(CONVERT(CHAR,STATUS) ) AS STATUS,
TRIM(CONVERT(CHAR,TAG) ) AS TAG,
TRIM(CONVERT(CHAR,ARDUEDATE) ) AS ARDUE_DT,
TRIM(CONVERT(CHAR,APDUEDATE) ) AS APDUE_DT,
TRIM(CONVERT(CHAR,PROPERTY) ) AS PROPERTY,
TRIM(CONVERT(CHAR,LOSTSAVINGSCODE) ) AS LOST_SAVINGS_CD,
TRIM(CONVERT(CHAR,PNRLOCATOR) ) AS PNRLOCATOR,
TRIM(CONVERT(CHAR,TICKETTYPE) ) AS TICKET_TYPE,
TRIM(CONVERT(CHAR,RESSYSTEM) ) AS RES_SYSTEM,
TRIM(CONVERT(CHAR,BOOKEDUNITS) ) AS BOOKED_UNITS,
TRIM(CONVERT(CHAR,STATUSREASON) ) AS STATUS_REASON,
TRIM(CONVERT(CHAR,SORT3) ) AS SORT3,
TRIM(CONVERT(CHAR,SORT4) ) AS SORT4,
TRIM(CONVERT(CHAR,CHANGEAGENT) ) AS CHANGE_AGENT,
TRIM(CONVERT(CHAR,REVTYPE) ) AS REV_TYPE,
TRIM(CONVERT(CHAR,INVOICEPOSTED) ) AS INVOICE_POSTED,
TRIM(CONVERT(CHAR,CONVERTED) ) AS CONVERTED,
TRIM(CONVERT(CHAR,GROUPID) ) AS GROUP_ID,
TRIM(CONVERT(CHAR,PARTYID) ) AS PARTY_ID,
TRIM(CONVERT(CHAR,MARKETID) ) AS MARKET_ID,
TRIM(CONVERT(CHAR,URLOCATOR) ) AS URLOCATOR,
TRIM(CONVERT(CHAR,CREATEDATE) ) AS CREATE_DT,
TRIM(CONVERT(CHAR,MODIFIEDDATE) ) AS MODIFIED_DT,
TRIM(CONVERT(CHAR,VERSIONNUM) ) AS VERSION_NUM,
TRIM(CONVERT(CHAR,CURRENCY) ) AS CURRENCY,
TRIM(CONVERT(CHAR,INTERFACE) ) AS INTERFACE,
TRIM(CONVERT(CHAR,LOCKUPDATE) ) AS LOCKUP_DT,
TRIM(CONVERT(CHAR,SUPPLIER) ) AS SUPPLIER
FROM DBA.INVOICE
WHERE PAYID IN (SELECT DISTINCT PAYID FROM DBA.INVCHANGELOG WHERE UPPER(TableName) = 'INVOICE' AND CONVERT(DATE,ChangeDate) >= CONVERT(DATE,'v_startdate'))
OR PAYID > 'max_payid'