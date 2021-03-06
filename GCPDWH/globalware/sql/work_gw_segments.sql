SELECT 
TRIM(CONVERT(CHAR,InvPayID)) AS INV_PAY_ID,
TRIM(CONVERT(CHAR,SegmentNum)) AS SEGEMTN_NUM,
TRIM(CONVERT(CHAR,Airline)) AS AIRLINE,
TRIM(CONVERT(CHAR,Flight)) AS FLIGHT,
TRIM(CONVERT(CHAR,Class)) AS CLASS,
TRIM(CONVERT(CHAR,DepartCity)) AS DEPART_CITY,
TRIM(CONVERT(CHAR,ArrivalCity)) AS ARRIVAL_CITY,
TRIM(CONVERT(CHAR,DepartDate,120)) AS DEPART_DT,
TRIM(CONVERT(CHAR,ArrivalDate,120)) AS ARRIVAL_DT,
TRIM(CONVERT(CHAR,DepartTime)) AS DEPART_TM,
TRIM(CONVERT(CHAR,ArrivalTime)) AS ARRIVAL_TM,
TRIM(CONVERT(CHAR,FareBasis)) AS FARE_BASIS,
TRIM(CONVERT(CHAR,Fare   )) AS FARE,
TRIM(CONVERT(CHAR,"Connect")) AS SEGMENTS_CONNECT,
TRIM(CONVERT(CHAR,Status)) AS STATUS,
TRIM(CONVERT(CHAR,AccountId )) AS ACCOUNT_ID,
TRIM(CONVERT(CHAR,DomInt)) AS DOM_INT,
TRIM(CONVERT(CHAR,MealCode)) AS MEAL_CD,
TRIM(CONVERT(CHAR,TicketDesignator)) AS TICKET_DESIGNATOR,
TRIM(CONVERT(CHAR,FreqFlyerNum)) AS FREQ_FLYER_NUM,
TRIM(CONVERT(CHAR,FreqFlyerMiles)) AS FREQ_FLYER_MILES,
TRIM(CONVERT(CHAR,NauticalMiles)) AS NAUTICAL_MILES,
TRIM(CONVERT(CHAR,DestStopover)) AS DEST_STOP_OVER,
TRIM(CONVERT(CHAR,TrueODNum)) AS TRUE_OD_NUM,
TRIM(CONVERT(CHAR,FreqFlyerCarrier)) AS FREQ_FLYER_CARRIER,
TRIM(CONVERT(CHAR,OpAirline)) AS OP_AIRLINE,
TRIM(CONVERT(CHAR,OpAirlineName)) AS OP_AIRLINE_NM
FROM DBA.Segments
WHERE InvPayID IN (SELECT DISTINCT PAYID FROM DBA.INVCHANGELOG WHERE CONVERT(DATE,ChangeDate) >= CONVERT(DATE,'v_startdate')
OR InvPayID > 'max_payid'
