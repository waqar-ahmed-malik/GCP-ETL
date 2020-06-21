INSERT INTO CUSTOMER_PRODUCT.GIG_REGISTRATIONS_OVERVIEW (
JOIN_DTTIME ,
CUSTOMER_ID ,
PHONE_NUM ,
FIRST_NM ,
LAST_NM ,
EMAIL ,
MAILING_ADDRESS ,
MAILING_CITY ,
MAILING_STATE ,
LICENSE_ISSUED_STATE ,
BLOCKED_DTTIME ,
MEMBER_NUM ,
BACKGROUND_REPORT_DTTIME ,
BACKGROUND_REPORT_RESULT ,
IS_PHONE_VERIFIED ,
IS_JUMIO_VERIFIED ,
ACCOUNT_STATUS ,
RF_ID ,
CREDITS ,
CREDITS_SOURCE ,
CAN_MEMBER_RESERVE_CAR ,
HAS_VALID_CC ,
JOB_RUN_ID  ,
SOURCE_SYSTEM_CD ,
CREATE_DTTIME ,
CREATE_BY )
(SELECT  
SAFE_CAST (JOIN_DTTIME AS DATETIME) , 
SAFE_CAST (CUSTOMER_ID AS INT64) , 
PHONE_NUM , 
FIRST_NM , 
LAST_NM , 
EMAIL , 
MAILING_ADDRESS , 
MAILING_CITY , 
MAILING_STATE , 
LICENSE_ISSUED_STATE , 
SAFE_CAST (BLOCKED_DTTIME AS DATETIME) , 
MEMBER_NUM , 
SAFE_CAST (BACKGROUND_REPORT_DTTIME AS DATETIME), 
BACKGROUND_REPORT_RESULT , 
IS_PHONE_VERIFIED , 
IS_JUMIO_VERIFIED , 
ACCOUNT_STATUS , 
RF_ID , 
CAST (CREDITS AS FLOAT64) , 
CREDITS_SOURCE , 
CAN_MEMBER_RESERVE_CAR , 
HAS_VALID_CC , 
"8912",
"GIG",
CURRENT_DATETIME() ,
"v_job_name" from LANDING.WORK_GIG_REGISTRATIONS_OVERVIEW);
