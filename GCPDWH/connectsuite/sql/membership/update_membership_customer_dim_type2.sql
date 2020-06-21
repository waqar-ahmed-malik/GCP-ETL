UPDATE CUSTOMERS.MEMBERSHIP_CUSTOMER_DIM DIM
SET ROW_END_DT = DATE_SUB(PARSE_DATE('%Y-%m-%d',STG.MBRS_UPDATE_DT), INTERVAL 1 DAY),
ACTIVE_FLG = 'N'
FROM `LANDING.WORK_CS_MEMBERSHIP_CUSTOMER_DIM_MD5`  STG
INNER JOIN 
(SELECT MM,ROW_START_DT,ACTIVE_FLG FROM 
(SELECT TRIM(CONCAT(CAST(MEMBERSHIP_NUM AS STRING),CAST(ASSOCIATE_ID AS STRING))) MM,ROW_START_DT,ACTIVE_FLG,ROW_NUMBER() OVER (PARTITION BY TRIM(CONCAT(CAST(MEMBERSHIP_NUM AS STRING),CAST(ASSOCIATE_ID AS STRING))),ACTIVE_FLG ORDER BY ROW_START_DT DESC ) DUP_CHECK FROM CUSTOMERS.MEMBERSHIP_CUSTOMER_DIM WHERE ACTIVE_FLG = 'Y') 
WHERE DUP_CHECK=2) TEMP
ON TEMP.MM =TRIM(CONCAT(CAST(STG.MBRS_ID AS STRING),CAST(STG.ASSOCIATE_ID AS STRING)))
WHERE TEMP.ROW_START_DT = DIM.ROW_START_DT AND TRIM(CONCAT(CAST(DIM.MEMBERSHIP_NUM AS STRING),CAST(DIM.ASSOCIATE_ID AS STRING))) = TRIM(CONCAT(CAST(STG.MBRS_ID AS STRING),CAST(STG.ASSOCIATE_ID AS STRING)))