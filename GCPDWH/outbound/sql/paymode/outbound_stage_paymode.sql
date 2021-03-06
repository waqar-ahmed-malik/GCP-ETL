select PAYMODE_DESC from 
(SELECt 11111111 AS PAYMODE_SEQ,CONCAT(MEMBER_NUM,',',IFNULL(FORMAT("%.*f",2,CAST(BALANCE_DUE AS FLOAT64) + .0001),''),',',FIRST_NAME,',',LAST_NAME,',',TERM_EXPIRATION_DT) AS PAYMODE_DESC
FROM (
SELECT 
DISTINCT CUST_DIM.MEMBER_NUM AS MEMBER_NUM,
CASE WHEN CUST_DIM.ASSOCIATE_ID=1 THEN BALANCE_DUE_AMT ELSE 0 END AS BALANCE_DUE,
IFNULL(UPPER(CUST_DIM.FIRST_NM),'NONE') AS FIRST_NAME,
IFNULL(UPPER(CUST_DIM.LAST_NM) ,'NONE') AS LAST_NAME,
FORMAT_DATE("%m/%d/%Y",TERM_EXPIRATION_DT) AS TERM_EXPIRATION_DT
FROM CUSTOMERS.CONNECTSUITE_MEMBER   AS CUST_DIM
INNER JOIN OPERATIONAL.CONNECTSUITE_MEMBERSHIP AS MEMBERSHIP_DIM
ON CUST_DIM.MEMBERSHIP_NUM         =MEMBERSHIP_DIM.MEMBERSHIP_NUM
WHERE STATUS_CD in ('A','P')
)
UNION DISTINCT
select 99999999  AS PAYMODE_SEQ,CONCAT('Total_number_of_Active_Members=',CAST(count(distinct CUST_DIM.MEMBER_NUM) AS STRING)) AS PAYMODE_DESC
FROM CUSTOMERS.CONNECTSUITE_MEMBER   AS CUST_DIM
INNER JOIN OPERATIONAL.CONNECTSUITE_MEMBERSHIP AS MEMBERSHIP_DIM
ON CUST_DIM.MEMBERSHIP_NUM         =MEMBERSHIP_DIM.MEMBERSHIP_NUM
WHERE STATUS_CD in ('A','P'))
order by PAYMODE_SEQ
