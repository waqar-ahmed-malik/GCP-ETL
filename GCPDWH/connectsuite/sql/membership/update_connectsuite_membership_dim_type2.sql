UPDATE   `CUSTOMER_PRODUCT.CONNECTSUITE_MEMBERSHIP_DIM` DIM
SET DIM.ROW_END_DT = DATE_SUB(PARSE_DATE('%Y-%m-%d',STG.MBRS_UPDATE_DT), INTERVAL 1 DAY),
ACTIVE_FLG = 'N'
FROM `LANDING.WORK_CS_MEMBERSHIP_DIM_MD5`  STG
INNER JOIN 
(SELECT MEMBERSHIP_KEY,ROW_START_DT,ACTIVE_FLG FROM 
(SELECT MEMBERSHIP_KEY ,ROW_START_DT,ACTIVE_FLG,ROW_NUMBER() OVER (PARTITION BY MEMBERSHIP_KEY,ACTIVE_FLG ORDER BY ROW_START_DT DESC ) DUP_CHECK FROM `CUSTOMER_PRODUCT.CONNECTSUITE_MEMBERSHIP_DIM`WHERE ACTIVE_FLG = 'Y') 
WHERE DUP_CHECK=2) TEMP
ON TEMP.MEMBERSHIP_KEY =SAFE_CAST(STG.MEMBERSHIP_KEY AS INT64)
WHERE TEMP.ROW_START_DT = DIM.ROW_START_DT AND DIM.MEMBERSHIP_KEY = SAFE_CAST(STG.MEMBERSHIP_KEY AS INT64)