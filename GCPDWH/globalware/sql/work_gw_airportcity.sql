SELECT 
TRIM(CONVERT(CHAR,Code)) AS CD,
TRIM(CONVERT(CHAR,CityCode)) AS CITY_CD,
TRIM(CONVERT(CHAR,AirportName))  AS AIRPORT_NM,                            
TRIM(CONVERT(CHAR,CityName))   AS CITY_NM,        
TRIM(CONVERT(CHAR,State)) AS STATE,
TRIM(CONVERT(CHAR,Country)) AS COUNTRY,
TRIM(CONVERT(CHAR,DomInt)) AS DOMINT,
TRIM(CONVERT(CHAR,Lattitude)) AS LAT,
TRIM(CONVERT(CHAR,Longitude)) AS LNG,
TRIM(CONVERT(CHAR,Address1,80))  AS ADDRESS_1,                                             
TRIM(CONVERT(CHAR,Address2,80))  AS ADDRESS_2,                                      
TRIM(CONVERT(CHAR,RevenueType)) AS REVENUE_TYPE,
TRIM(CONVERT(CHAR,PostalCode)) AS POSTAL_CD
FROM DBA.AirportCity 
