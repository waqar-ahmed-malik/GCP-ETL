CREATE OR REPLACE TABLE LANDING.WORK_TRAVEL_TST_TRANSACTIONS AS (SELECT * EXCEPT (RN) FROM (SELECT DISTINCT 
SAFE_CAST(TST_BOOKING_ID AS INT64) AS SOURCE_TRANSACTION_ID,
SAFE_CAST(TST_TRIP_ID AS INT64) AS TRIP_TRANSACTION_NUM,
BOOKING_DT AS TRANSACTION_DT,
MDM.CUSTOMER_MDM_KEY MDM_CUSTOMER_KEY,
MEMBERSHIP_NUM AS MEMBER_NUM,
MEMBERSHIP_NUM AS TRAVELER_MEMBERSHIP_NUM,
SUBSTR( MEMBERSHIP_NUM , 15,1)  TRAVELER_ASSOCIATE_ID ,
SUBSTR( MEMBERSHIP_NUM , 4,3)	CLUB_CD,
TRAVEL_TYPE SALE_TYPE_CD,
TRAVEL_TYPE SALE_TYPE_DESC,
SAFE_CAST(TOTAL_GROSS_SALE_AMT AS FLOAT64) SALE_POSTED_AMOUNT,
TRAVEL_TYPE TRAVEL_TYPE_CD,
TRAVEL_TYPE TRAVEL_TYPE_DESC,
TST.FIRST_NM AS TRAVELER_FIRST_NM,
TST.LAST_NM  as TRAVELER_LAST_NM,
TST.ADDRESS_LINE1 AS TRAVELER_ADDRESS_LINE_1,
TST.ADDRESS_LINE2 AS TRAVELER_ADDRESS_LINE_2,
TST.CITY AS TRAVELER_CITY,
SAFE_CAST(NULL AS STRING) AS TRAVELER_STATE,
TST.ZIP AS TRAVELER_ZIP_CD,
TST.PHONE AS TRAVELER_PHONE_NUM, 
TST.EMAIL AS TRAVELER_EMAIL_ADDRESS, 
SAFE_CAST(TST_BOOKING_ID AS INT64) SALE_NUM,
EXTRACT( DATE FROM SAFE_CAST(BOOKING_DT AS DATETIME)) BOOKING_DT,
TRAVEL_TYPE BOOKING_TYPE,
--PARSE_DATE("%Y-%m-%d",SUBSTR(TRAVEL_START_DT,1,10)) DEPARTURE_DT,
SUBSTR(TRAVEL_START_DT,1,10) AS DEPARTURE_DT,
--PARSE_DATE("%Y-%m-%d",SUBSTR(TRAVEL_END_DT,1,10)) RETURN_DT,
SUBSTR(TRAVEL_END_DT,1,10) AS  RETURN_DT,
EXTRACT( DATE FROM SAFE_CAST(MODIFIED_DTTIME AS DATETIME)) CHANGE_DT,
SAFE_CAST (CANCELLATION_DT AS DATE) CANCELLATION_DT,
EXTRACT( DATE FROM SAFE_CAST(BOOKING_DT AS DATETIME)) SALE_POSTING_DT,
TST_AGENT_ID BOOKING_AGENT_ID, 
SAFE_CAST(NULL AS STRING) AS BOOKING_EMPLOYEE_ID, 
SAFE_CAST(NULL AS STRING) AS BOOKING_LOCATION_ID,
TST_AGENT_ID TICKETING_AGENT_ID,
SAFE_CAST(NULL AS STRING)AS TICKETING_EMPLOYEE_ID,
SAFE_CAST(NULL AS STRING) AS TICKETING_LOCATION_ID,
CLUB_AGENT_ID SELLING_AGENT_ID,
SAFE_CAST(NULL AS STRING) AS COMPENSATION_EMPLOYEE_ID, 
SAFE_CAST(NULL AS STRING) AS SELLING_LOCATION_ID,
TRIM(LZD.LOCATION_ID) AS REVENUE_LOCATION_ID, 
TRIM(LZD.LOCATION_ID) TRAVEL_LOCATION_ID,
SAFE_CAST(SUBTOTAL_AMT AS INT64) BASE_FARE,
SAFE_CAST(TOTAL_GROSS_SALE_AMT AS FLOAT64) TOTAL_COST,
CAST(NULL AS FLOAT64)  AS COMMISSION_PERCENT,
CAST(NULL AS FLOAT64)  AS COMMISSION_AMOUNT,
CAST(NULL AS FLOAT64) ANTICIPATED_SALE_AMOUNT,
SAFE_CAST(ESTIMATED_COMMISSION_AGENCY AS FLOAT64) ANTICIPATED_COMMISSION_AMOUNT,
DESTINATION_CITY ,
SAFE_CAST(NULL AS STRING)  AS DESTINATION_REGION,
SAFE_CAST(PASSENGER_COUNT AS INT64) AS PASSENGER_COUNT,
CASE WHEN UPPER(TRAVEL_TYPE) = 'AIR' THEN VENDOR ELSE NULL END AS AIRLINE, 
PROVIDER_REFERENCE_NUM TICKET_NUM,
SAFE_CAST(NULL AS STRING)  AS ITINERARY,
VENDOR PROVIDER,
SAFE_CAST(NULL AS STRING) AS STATUS_CD,
STATUS STATUS_CD_DESC,
PROVIDER_REFERENCE_NUM PNR_LOCATOR,
SAFE_CAST(NULL AS STRING) AS SETTLE_CD,
SAFE_CAST(NULL AS STRING) SETTLE_CD_DESC,
SAFE_CAST(NULL AS STRING) SOJOURN_CD,
SAFE_CAST(NULL AS STRING)  AS CUSTOMER_TYPE_CD,
SAFE_CAST(NULL AS STRING)  AS CUSTOMER_TYPE_DESC,
SAFE_CAST(NULL AS STRING)  AS REVENUE_TYPE_CD,
SAFE_CAST(NULL AS STRING)  AS REVENUE_TYPE_DESC,
SAFE_CAST(NULL AS STRING)  AS INVOICE_POSTED_FLAG,
SAFE_CAST(NULL AS STRING)  AS PROMOTION_CD,
SAFE_CAST(NULL AS STRING)  AS AUTO_TRAVEL_FLAG,
SAFE_CAST(NULL AS STRING)  AS SOURCE_SYSTEM_BRANCH_CD,
SAFE_CAST(NULL AS STRING)  AS STP,
SUPPLIER AS INVENTORY_SUPPLIER,
SAFE_CAST(NULL AS STRING)  AS DOMINT,
SAFE_CAST(NULL AS FLOAT64)  AS TAX1,
SAFE_CAST(NULL AS  FLOAT64)  AS TAX2,
SAFE_CAST(NULL AS  FLOAT64)  AS TAX3,
SAFE_CAST(NULL AS  FLOAT64)  AS TAX4,
SAFE_CAST(NULL AS  FLOAT64)  AS MISC_CHARGE,
SAFE_CAST(NULL AS  FLOAT64)  AS DISCOUNT,
SAFE_CAST(NULL AS  FLOAT64) AS EXCHANGE, 
SAFE_CAST(NULL AS STRING) AS FOP,
"TST" SOURCE_SYSTEM_CD,
CURRENT_DATETIME() CREATE_DTTIME,
ROW_NUMBER() OVER (PARTITION BY  TST_BOOKING_ID ORDER BY BOOKING_DT ASC ) RN 
FROM  LANDING.TRAVEL_TST_LDG TST 
LEFT JOIN (SELECT DISTINCT CUSTOMER_MDM_KEY, SOURCE_KEY1,SOURCE_KEY2 FROM  CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE WHERE SOURCE_SYSTEM_CD='TRAVEL_TST') MDM
ON MDM.SOURCE_KEY1=TST.TST_BOOKING_ID
AND MDM.SOURCE_KEY2=TST.TST_TRIP_ID
LEFT OUTER JOIN CUSTOMER_PRODUCT.LOCATION_ZIP_DIM LZD ON TRIM(TST.ZIP) = LZD.ZIP_CD
WHERE TST_BOOKING_ID  NOT IN ( SELECT SAFE_CAST(SOURCE_TRANSACTION_ID AS STRING) FROM CUSTOMERS.TRAVEL_TRANSACTION_FACT WHERE SOURCE_SYSTEM_CD = 'TST')
)
WHERE RN = 1   )