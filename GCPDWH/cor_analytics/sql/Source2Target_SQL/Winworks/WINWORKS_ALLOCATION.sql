SELECT 
PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p', PMT_DATE) AS PAYMENT_DATE, 
CAST(PAY_ID AS INT64) AS PAY_ID, 
ORDER_NUMBER, 
CAST(AMOUNT AS FLOAT64) AS AMOUNT, 
CUST_ID, 
EXTRACT(DATE FROM PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p', ALLOC_DATE)) AS ALLOC_DATE,
PARSE_DATETIME('%d-%h-%y %H.%M.%S.000000000 %p', CREATE_DTTIME) AS CREATE_DTTIME, 
CREATE_BY, 
SOURCE_SYSTEM_CD
FROM `aaadata-181822.COR_WINWORKS.STG_ALLOCATIONS` 	
				
				
				
				
				
				
				
				