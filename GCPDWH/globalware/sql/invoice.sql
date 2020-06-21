MERGE OPERATIONAL.GLOBALWARE_INVOICE A
USING LANDING.WORK_GW_INVOICE B
ON A.PAY_ID = CAST(B.PAY_ID AS INT64)
WHEN MATCHED THEN
UPDATE SET
A.ACCOUNT_ID =  B.ACCOUNT_ID,
A.REPORT_TO_ID =  B.REPORT_TO_ID,
A.INVOICE_NUM =  B.INVOICE_NUM ,
A.INVOICE_DT =  EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.INVOICE_DT)),
A.SALE_TYPE =  B.SALE_TYPE,
A.SETTLE =  B.SETTLE,
A.TRAVEL_TYPE =  B.TRAVEL_TYPE,
A.SALE_NUM =  B.SALE_NUM,
A.CUSTOMER_TYPE =  B.CUST_TYPE,
A.TRAVELER =  B.TRAVELER,
A.BKAGT =  B.BKAGT,
A.TKTAGT =  B.TKTAGT,
A.SELLAGT =  B.SELLAGT,
A.BRANCH =  B.BRANCH,
A.STP =  B.STP,
A.DEPART_DT =  EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.DEPART_DT )), 
A.RETURN_DT =  EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.RETURN_DT)), 
A.BASEFARE =  SAFE_CAST(B.BASEFARE AS FLOAT64),
A.TAX1 =  SAFE_CAST(B.TAX1 AS FLOAT64),
A.TAX2 =  SAFE_CAST(B.TAX2 AS FLOAT64),
A.TAX3 =  SAFE_CAST(B.TAX3 AS FLOAT64),
A.TAX4 =  SAFE_CAST(B.TAX4 AS FLOAT64),
A.MISCCHARGE =  SAFE_CAST(B.MISCCHARGE AS FLOAT64),
A.DISCOUNT =  SAFE_CAST(B.DISCOUNT AS FLOAT64),
A.EXCHANGE =  SAFE_CAST(B.EXCHANGE AS FLOAT64),
A.TOTAL_COST =  SAFE_CAST(B.TOTAL_COST AS FLOAT64),
A.COMM_PERCENT =  SAFE_CAST(B.COMM_PERCENT AS FLOAT64),
A.COMM_AMOUNT =  SAFE_CAST(B.COMM_AMOUNT AS FLOAT64),
A.FOP =  B.FOP,
A.MAX_FARE =  SAFE_CAST(B.MAXFARE AS FLOAT64),
A.LOW_FARE =  SAFE_CAST(B.LOWFARE AS FLOAT64),
A.DOMINT =  B.DOMINT,
A.DESTINATION =  B.DESTINATION,
A.DOC_TYPE =  B.DOC_TYPE,
A.AIRLINE =  B.AIRLINE,
A.TICKET_NUM =  B.TICKET_NUM,
A.CONJUNCT =  B.CONJUNCT,
A.ITINERARY =  B.ITINERARY,
A.PROVIDER =  B.PROVIDER,
A.SAVINGS_CD =  B.SAVINGS_CODE,
A.SAVINGS_COMMENT =  B.SAVINGS_COMMENT,
A.SORT1 =  B.SORT1,
A.SORT2 =  B.SORT2,
A.BOOK_DT =   EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.BOOK_DT)),  
A.CHANGE_DT =   EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.CHANGE_DT)),  
A.SALES_POSTED_DT =   EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.SALES_POSTED_DT)),  
A.SALES_POSTED_AMOUNT =  SAFE_CAST(B.SALES_POSTED_AMT AS FLOAT64),
A.STATUS =  B.STATUS,
A.TAG =  B.TAG,
A.ARDUE_DT =  EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.ARDUE_DT)), 
A.APDUE_DT =  EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.APDUE_DT)),  
A.PROPERTY =  B.PROPERTY,
A.LOST_SAVINGS_CD =  B.LOST_SAVINGS_CD,
A.PNR_LOCATOR =  B.PNRLOCATOR,
A.TICKET_TYPE =  B.TICKET_TYPE,
A.RES_SYSTEM =  B.RES_SYSTEM,
A.BOOKED_UNITS =  B.BOOKED_UNITS,
A.STATUS_REASON =  B.STATUS_REASON,
A.SORT3 =  B.SORT3,
A.SORT4 =  B.SORT4,
A.CHANGE_AGENT =  B.CHANGE_AGENT,
A.REV_TYPE =  B.REV_TYPE,
A.INVOICE_POSTED =  B.INVOICE_POSTED,
A.CONVERTED =  B.CONVERTED,
A.GROUP_ID =  B.GROUP_ID,
A.PARTY_ID =  B.PARTY_ID,
A.MARKET_ID =  B.MARKET_ID,
A.URLOCATOR =  B.URLOCATOR,
A.CREATE_DT =  PARSE_DATETIME("%b %d %Y %I:%M%p",B.CREATE_DT),   
A.MODIFIED_DT =  PARSE_DATETIME("%b %d %Y %I:%M%p",B.MODIFIED_DT),
A.VERSION_NUM =  SAFE_CAST(B.VERSION_NUM AS INT64),
A.CURRENCY =  B.CURRENCY,
A.INTERFACE =  B.INTERFACE,
A.LOCKUP_DT =  B.LOCKUP_DT,
A.SUPPLIER =  B.SUPPLIER

WHEN NOT MATCHED THEN
  INSERT (

PAY_ID,
ACCOUNT_ID,
REPORT_TO_ID,
INVOICE_NUM,
INVOICE_DT,
SALE_TYPE,
SETTLE,
TRAVEL_TYPE,
SALE_NUM,
CUSTOMER_TYPE,
TRAVELER,
BKAGT,
TKTAGT,
SELLAGT,
BRANCH,
STP,
DEPART_DT,
RETURN_DT,
BASEFARE,
TAX1,
TAX2,
TAX3,
TAX4,
MISCCHARGE,
DISCOUNT,
EXCHANGE,
TOTAL_COST,
COMM_PERCENT,
COMM_AMOUNT,
FOP,
MAX_FARE,
LOW_FARE,
DOMINT,
DESTINATION,
DOC_TYPE,
AIRLINE,
TICKET_NUM,
CONJUNCT,
ITINERARY,
PROVIDER,
SAVINGS_CD,
SAVINGS_COMMENT,
SORT1,
SORT2,
BOOK_DT,
CHANGE_DT,
SALES_POSTED_DT,
SALES_POSTED_AMOUNT,
STATUS,
TAG,
ARDUE_DT,
APDUE_DT,
PROPERTY,
LOST_SAVINGS_CD,
PNR_LOCATOR,
TICKET_TYPE,
RES_SYSTEM,
BOOKED_UNITS,
STATUS_REASON,
SORT3,
SORT4,
CHANGE_AGENT,
REV_TYPE,
INVOICE_POSTED,
CONVERTED,
GROUP_ID,
PARTY_ID,
MARKET_ID,
URLOCATOR,
CREATE_DT,
MODIFIED_DT,
VERSION_NUM,
CURRENCY,
INTERFACE,
LOCKUP_DT,
SUPPLIER,
SURVEY_IND,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
LAST_UPDATE_DTTIME
)
VALUES(
CAST(B.PAY_ID AS INT64),
ACCOUNT_ID,
REPORT_TO_ID,
INVOICE_NUM,
EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",INVOICE_DT)),
SALE_TYPE,
SETTLE,
TRAVEL_TYPE,
SALE_NUM,
CUST_TYPE,
TRAVELER,
BKAGT,
TKTAGT,
SELLAGT,
BRANCH,
STP,
EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.DEPART_DT )), 
EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.RETURN_DT)), 
SAFE_CAST(B.BASEFARE AS FLOAT64),
SAFE_CAST(B.TAX1 AS FLOAT64),
SAFE_CAST(B.TAX2 AS FLOAT64),
SAFE_CAST(B.TAX3 AS FLOAT64),
SAFE_CAST(B.TAX4 AS FLOAT64),
SAFE_CAST(B.MISCCHARGE AS FLOAT64),
SAFE_CAST(B.DISCOUNT AS FLOAT64),
SAFE_CAST(B.EXCHANGE AS FLOAT64),
SAFE_CAST(B.TOTAL_COST AS FLOAT64),
SAFE_CAST(B.COMM_PERCENT AS FLOAT64),
SAFE_CAST(B.COMM_AMOUNT AS FLOAT64),
B.FOP,
SAFE_CAST(B.MAXFARE AS FLOAT64),
SAFE_CAST(B.LOWFARE AS FLOAT64),
DOMINT,
DESTINATION,
DOC_TYPE,
AIRLINE,
TICKET_NUM,
CONJUNCT,
ITINERARY,
PROVIDER,
SAVINGS_CODE,
SAVINGS_COMMENT,
SORT1,
SORT2,

EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.BOOK_DT)),  
EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.CHANGE_DT)),  
EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.SALES_POSTED_DT)),  
SAFE_CAST(B.SALES_POSTED_AMT AS FLOAT64),
B.STATUS,
B.TAG,
EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.ARDUE_DT)), 
EXTRACT (DATE FROM PARSE_DATETIME("%b %d %Y %I:%M%p",B.APDUE_DT)),  
PROPERTY,
LOST_SAVINGS_CD,
PNRLOCATOR,
TICKET_TYPE,
RES_SYSTEM,
BOOKED_UNITS,
STATUS_REASON,
SORT3,
SORT4,
CHANGE_AGENT,
REV_TYPE,
INVOICE_POSTED,
CONVERTED,
GROUP_ID,
PARTY_ID,
MARKET_ID,
URLOCATOR,
PARSE_DATETIME("%b %d %Y %I:%M%p",B.CREATE_DT),  
PARSE_DATETIME("%b %d %Y %I:%M%p",B.MODIFIED_DT),  
SAFE_CAST(VERSION_NUM AS INT64),
CURRENCY,
INTERFACE,
LOCKUP_DT,
SUPPLIER,
"N",
"jobrunid",
'GLOBALWARE' ,
CURRENT_DATETIME());













