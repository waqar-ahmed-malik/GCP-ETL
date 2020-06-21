SELECT 
TRIM(CONVERT(CHAR,ArcNum)) AS ARC_NUM,
TRIM(CONVERT(CHAR,BrNum)) AS BR_NUM,
TRIM(CONVERT(CHAR,StpNum))  AS STP_NUM,                            
TRIM(CONVERT(CHAR,Name,80))   AS ARC_TABLE_NM,        
TRIM(CONVERT(CHAR,Addr1,80 )) AS ADDR_1,
TRIM(CONVERT(CHAR,Addr2,80)) AS ADDR_2,
TRIM(CONVERT(CHAR,Addr3,80 )) AS ADDR_3,
TRIM(CONVERT(CHAR,brlogo)) AS BR_LOGO,
TRIM(CONVERT(CHAR,usebrlogo)) AS USE_BR_LOGO,
TRIM(CONVERT(CHAR,Usebronar))  AS USE_BR_ONAR,                                             
TRIM(CONVERT(CHAR,logoname))  AS LOGO_NM                                    

FROM DBA.ArcTable