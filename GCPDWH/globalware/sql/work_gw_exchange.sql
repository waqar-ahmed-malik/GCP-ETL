SELECT 
TRIM(CONVERT(CHAR,InvPayID)) AS INV_PAY_ID,
TRIM(CONVERT(CHAR,BaseFare)) AS BASE_FARE,
TRIM(CONVERT(CHAR,Tax1  )) AS TAX_1,
TRIM(CONVERT(CHAR,Tax2  )) AS TAX_2,
TRIM(CONVERT(CHAR,Tax3  )) AS TAX_3,
TRIM(CONVERT(CHAR,Tax4)) AS TAX_4,
TRIM(CONVERT(CHAR,MisCharge)) AS MIS_CHARGE,
TRIM(CONVERT(CHAR,Discount)) AS DISCOUNT,
TRIM(CONVERT(CHAR,TotalCost)) AS TOTAL_COST,
TRIM(CONVERT(CHAR,CommAmount)) AS COMM_AMT,
TRIM(CONVERT(CHAR,Penalty)) AS PENALTY,
TRIM(CONVERT(CHAR,PenaltyComm)) AS PENALTY_COMM,
TRIM(CONVERT(CHAR,LostFare)) AS LOST_FARE,
TRIM(CONVERT(CHAR,TicketNum  )) AS TICKET_NUM,
TRIM(CONVERT(CHAR,Airline)) AS AIRLINE,
TRIM(CONVERT(CHAR,PFC1)) AS PFC1,
TRIM(CONVERT(CHAR,PFC2)) AS PFC2,
TRIM(CONVERT(CHAR,PFC3)) AS PFC3,
TRIM(CONVERT(CHAR,PFC4)) AS PFC4,
TRIM(CONVERT(CHAR,City1)) AS CITY_1,
TRIM(CONVERT(CHAR,City2)) AS CITY_2,
TRIM(CONVERT(CHAR,City3)) AS CITY_3,
TRIM(CONVERT(CHAR,City4)) AS CITY_4,
TRIM(CONVERT(CHAR,Coupon1)) AS COUPON_1,
TRIM(CONVERT(CHAR,Coupon2)) AS COUPON_2,
TRIM(CONVERT(CHAR,Coupon3)) AS COUPON_3,
TRIM(CONVERT(CHAR,NewTkt2   )) AS NEW_TKT2,
TRIM(CONVERT(CHAR,NewTkt3)) AS NEW_TKT3,
TRIM(CONVERT(CHAR,PenaltyTax1)) AS PENALTY_TAX1,
TRIM(CONVERT(CHAR,PenaltyTax2)) AS PENALTY_TAX2,
TRIM(CONVERT(CHAR,PenaltyTax3)) AS PENALTY_TAX3
FROM DBA.Exchange

