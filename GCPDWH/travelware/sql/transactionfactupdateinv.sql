UPDATE CUSTOMERS.TRAVEL_TRANSACTION_FACT A
SET
TRAVELER_EMAIL=COMMENTS.EMAIL,
TST_TRIP_ID=NULL ,
INVENTORY_SUPPLIER=SAFE_CAST(NULL AS STRING) ,
MEMBERSHIP_NUM=CASE WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '438255' and LENGTH(CAST(INV.PAYID AS STRING)) > 12)
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),7,8)
	 WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '255' and LENGTH (CAST(INV.PAYID AS STRING)) > 10)
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),4,8)
	 WHEN LENGTH(CAST(INV.PAYID AS STRING)) = 8
	 THEN CAST(INV.PAYID AS STRING) 
	 WHEN LENGTH(CAST(INV.PAYID AS STRING)) = 9 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),1,8)
	 WHEN SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '005429' 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),7,8)
	 WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,9) = '005429005' and (LENGTH(CAST(INV.PAYID AS STRING))=17 OR LENGTH(CAST(INV.PAYID AS STRING))=19))
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),10,8)
	 WHEN SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '429005' 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),7,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 10 and (SUBSTR(CAST(INV.PAYID AS STRING),9,2)='14' OR SUBSTR(CAST(INV.PAYID AS STRING),9,2)='22') )
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),1,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 11 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),4,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 12 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005') 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),5,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 13 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),6,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 14 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),7,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 15 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),8,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) >= 15 and LENGTH(CAST(INV.PAYID AS STRING)) <= 16 and SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '005005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),8,8)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) > 16 and SUBSTR(CAST(INV.PAYID AS STRING),1,9) = '005005429')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),11,8)
	 ELSE NULL
   END,
ASSOCIATE_ID=CASE WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '438255' and LENGTH(CAST(INV.PAYID AS STRING)) > 12)
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),15,1)
	 WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '255' and LENGTH (CAST(INV.PAYID AS STRING)) > 10)
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),12,1)
     WHEN LENGTH(CAST(INV.PAYID AS STRING)) = 8
	 THEN '1'
	 WHEN LENGTH(CAST(INV.PAYID AS STRING)) = 9 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),1,1)
	 WHEN SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '005429' 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),15,1)
	 WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,9) = '005429005' and (LENGTH(CAST(INV.PAYID AS STRING))=17 OR LENGTH(CAST(INV.PAYID AS STRING))=19))
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),18,1)
	 WHEN SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '429005' 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),15,1)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 10 and (SUBSTR(CAST(INV.PAYID AS STRING),9,2)='14' OR SUBSTR(CAST(INV.PAYID AS STRING),9,2)='22') )
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),9,1)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 11 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN '1'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 12 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005') 
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),4,1)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 13 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),4,1)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 14 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),4,1)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 15 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN SUBSTR(CAST(INV.PAYID AS STRING),4,1)
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) >= 15 and LENGTH(CAST(INV.PAYID AS STRING)) <= 16 and SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '005005')
	 THEN '1'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) > 16 and SUBSTR(CAST(INV.PAYID AS STRING),1,9) = '005005429')
	 THEN '1'
	 ELSE NULL
   END,
CLUB_CD=CASE WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '438255' and LENGTH(CAST(INV.PAYID AS STRING)) > 12)
	 THEN '255'
	 WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '255' and LENGTH (CAST(INV.PAYID AS STRING)) > 10)
	 THEN  '255'
    WHEN LENGTH(CAST(INV.PAYID AS STRING)) = 8
	 THEN '005'
	 WHEN LENGTH(CAST(INV.PAYID AS STRING)) = 9 
	 THEN '005'
	 WHEN SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '005429' 
	 THEN '005'
	 WHEN (SUBSTR(CAST(INV.PAYID AS STRING),1,9) = '005429005' and (LENGTH(CAST(INV.PAYID AS STRING))=17 OR LENGTH(CAST(INV.PAYID AS STRING))=19))
	 THEN '005'
	 WHEN SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '429005' 
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 10 and (SUBSTR(CAST(INV.PAYID AS STRING),9,2)='14' OR SUBSTR(CAST(INV.PAYID AS STRING),9,2)='22') )
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 11 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 12 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005') 
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 13 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 14 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) = 15 and SUBSTR(CAST(INV.PAYID AS STRING),1,3) = '005')
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) >= 15 and LENGTH(CAST(INV.PAYID AS STRING)) <= 16 and SUBSTR(CAST(INV.PAYID AS STRING),1,6) = '005005')
	 THEN '005'
	 WHEN (LENGTH(CAST(INV.PAYID AS STRING)) > 16 and SUBSTR(CAST(INV.PAYID AS STRING),1,9) = '005005429')
	 THEN '005'
	 ELSE NULL
   END,	 
INVOICE_NUM=CAST(INVOICENUMBER AS INT64), 	
INVOICE_DT=INVOICEDATE ,
SALE_TYPE_CD=SALETYPE,
SALE_TYPE_DESC=(SELECT DESCRIPTION FROM LANDING.GLOBALWARE_CODES GW_CODE WHERE GW_CODE.CODE_VALUE=INV.SALETYPE AND GW_CODE.GLOBALWARE_COLUMN='SALETYPE' ),
SALE_POSTED_AMOUNT=SALESPOSTEDAMOUNT,
TRAVEL_TYPE_CD=TRAVELTYPE,
TRAVEL_TYPE_DESC=(SELECT DESCRIPTION FROM LANDING.GLOBALWARE_CODES GW_CODE WHERE GW_CODE.CODE_VALUE=INV.TRAVELTYPE AND GW_CODE.GLOBALWARE_COLUMN='TRAVELTYPE' ),
SALE_NUM=CAST(SALENUM AS INT64),
BOOKING_AGENT_ID=BKAGT,
TICKETING_AGENT_ID=TKTAGT,
SELLING_AGENT_ID=SELLAGT ,
NCNU_BRANCH_CD=CASE WHEN COMMENTS.AUTO_TRAVEL_FLAG ='T' THEN CAST(BRANCH.BRANCH AS STRING) ELSE SUBSTR(INV.BRANCH,2,3) END,
DEPARTURE_DT=INV.DEPARTDATE ,
RETURN_DT=RETURNDATE,
BASE_FARE=BASEFARE,
TOTAL_COST=TOTALCOST,
ANTICIPATED_COMMISSION_AMOUNT=CAST(COMMENTS.ANTICIPATED_COMMISSION_AMOUNT AS FLOAT64),
ANTICIPATED_SALE_AMOUNT=CAST(COMMENTS.ANTICIPATED_SALE_AMOUNT AS FLOAT64),
DESTINATION_CITY=DESTINATION,
PASSENGER_COUNT=CAST(COMMENTS.PASSENGER_COUNT AS INT64),
AIRLINE=INV.AIRLINE,
TICKET_NUM=INV.TICKETNUM,
PROVIDER=INV.PROVIDER,
BOOKING_DT=INV.BOOKDATE,
BOOKING_TYPE=COMMENTS.BOOKING_TYPE,
CHANGE_DT=INV.CHANGEDATE ,
CANCELLATION_DT=CAST(COMMENTS.CANCELLATION_DT AS DATE),
SALE_POSTING_DT=INV.SALESPOSTEDDATE,
STATUS_CD=STATUS,
STATUS_CD_DESC=(SELECT DESCRIPTION FROM LANDING.GLOBALWARE_CODES GW_CODE WHERE GW_CODE.CODE_VALUE=INV.STATUS AND GW_CODE.GLOBALWARE_COLUMN='STATUS' ),
PNR_LOCATOR=INV.PNRLOCATOR ,
MDM_CUSTOMER_KEY=MDM.CUSTOMER_MDM_KEY,
SETTLE_CD=SETTLE,
SETTLE_CD_DESC=(SELECT DESCRIPTION FROM LANDING.GLOBALWARE_CODES GW_CODE WHERE GW_CODE.CODE_VALUE=INV.SETTLE AND GW_CODE.GLOBALWARE_COLUMN='SETTLETYPE' ),
SOJOURN_CD=COMMENTS.SOJOURN_CD ,
CUSTOMER_TYPE_CD=CUSTTYPE,
CUSTOMER_TYPE_DESC=(SELECT DESCRIPTION FROM LANDING.GLOBALWARE_CODES GW_CODE WHERE GW_CODE.CODE_VALUE=INV.CUSTTYPE AND GW_CODE.GLOBALWARE_COLUMN='CUSTTYPE' ),
COMMISSION_PERCENT=COMMPERCENT,
COMMISSION_AMOUNT=COMMAMOUNT,
REVENUE_TYPE_CD=REVTYPE,
REVENUE_TYPE_DESC=(SELECT DESCRIPTION FROM LANDING.GLOBALWARE_CODES GW_CODE WHERE GW_CODE.CODE_VALUE=INV.REVTYPE AND GW_CODE.GLOBALWARE_COLUMN='REVTYPE' ),
INVOICE_POSTED_FLAG=INV.INVOICEPOSTED,
PROMOTION_CD=COMMENTS.PROMOTION_CD,
AUTO_TRAVEL_FLAG=COMMENTS.AUTO_TRAVEL_FLAG,  
SOURCE_SYSTEM_BRANCH_CD=INV.BRANCH,
STP=INV.STP ,
DESTINATION_REGION=COMMENTS.DESTINATION_REGION,
ITINERARY=INV.ITINERARY,
RECORD_LOAD_DT=CURRENT_DATE()
FROM 
LANDING.TRAVEL_INVOICE INV
LEFT JOIN CUSTOMERS.BRANCH_DIM BRANCH
ON INV.BRANCH =LPAD(BRANCH.BRANCH_NUMBER,1,'0')
LEFT JOIN (SELECT DISTINCT CUSTOMER_MDM_KEY, SOURCE_KEY1 FROM  CUSTOMERS.MDM_CUSTOMER_BRIDGE WHERE SOURCE_SYSTEM='COMMENTS') MDM
ON MDM.SOURCE_KEY1=INV.PAYID
LEFT JOIN (SELECT 
INVOICE_PAY_ID,
MAX(CASE WHEN LINE_NUM='28' THEN REPLACE(DATA,'//','@') ELSE NULL END) EMAIL,
MAX(CASE WHEN LINE_NUM='2' THEN DATA ELSE NULL END) ANTICIPATED_SALE_AMOUNT,
MAX(CASE WHEN LINE_NUM='82' THEN DATA ELSE NULL END )SOJOURN_CD,
MAX(CASE WHEN LINE_NUM='10' THEN DATA ELSE NULL END) ANTICIPATED_COMMISSION_AMOUNT,
MAX(CASE WHEN LINE_NUM='102' THEN DATA ELSE NULL END) DESTINATION_REGION,
MAX(CASE WHEN LINE_NUM='1' THEN DATA ELSE NULL END) PASSENGER_COUNT,
MAX(CASE WHEN LINE_NUM='31' THEN DATA ELSE NULL END) BOOKING_TYPE,
MAX(CASE WHEN LINE_NUM='30' THEN DATA ELSE NULL END) CANCELLATION_DT,
MAX(CASE WHEN LINE_NUM='8' THEN DATA ELSE NULL END) PROMOTION_CD,
MAX(CASE WHEN LINE_NUM='14' THEN DATA ELSE NULL END) AUTO_TRAVEL_FLAG
FROM 
LANDING.TRAVEL_COMMENTS
GROUP BY 1
) COMMENTS
ON INV.PAYID=COMMENTS.INVOICE_PAY_ID
WHERE 
A.SOURCE_SYSTEM_CD="GLOBALWARE" and A.SOURCE_SYSTEM_ID = INV.PAYID