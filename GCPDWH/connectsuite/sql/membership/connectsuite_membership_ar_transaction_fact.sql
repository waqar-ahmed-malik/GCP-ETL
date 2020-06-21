MERGE INTO CUSTOMER_PRODUCT.CONNECTSUITE_MEMBERSHIP_AR_TRANSACTION_FACT TGT
USING
  (select 
	distinct MEMBERSHIP_ID AS MEMBERSHIP_NUM,
	MEM.MEMBER_NUM AS MEMBER_NUM,
	CAST(STG.MEMBERSHIP_KEY AS INT64) AS MEMBERSHIP_KEY,
	CAST(STG.MEMBER_KEY AS INT64) AS MEMBER_KEY,
	CAST(SUBSTR(STG.AR_ENROLL_DATE,1,10) AS DATE) AS AR_ENROLL_DT,
	AR_PROCESSED_EMP_ID,
	EMP.LOCATION_ID AS AR_PROCESSED_EMP_LOCATION_ID,
	ENROLLMENT_TYPE,
	CAST(STG.SOURCE_COMMENT_KEY AS INT64) AS SOURCE_COMMENT_KEY,
	CAST(SOURCE_COMMENT_DT AS DATETIME) AS SOURCE_COMMENT_DT,
	'CONNECT SUITE' AS SOURCE_SYSTEM_CD,
	CURRENT_DATETIME() AS UPDATE_DTTIME
		from 
		LANDING.WORK_MEMBERSHIP_AR_ENROLLMENT STG
		LEFT OUTER JOIN
		(SELECT EMPLOYEE_ID,ENT_ID,LOCATION_ID FROM CUSTOMER_PRODUCT.EMPLOYEE_DIM
		where ACTIVE_FLG='Y') EMP
		ON TRIM(AR_PROCESSED_EMP_ID)=TRIM(EMPLOYEE_ID)
		LEFT OUTER JOIN (
			SELECT
			  TRIM(MEMBER_KEY) as MEMBER_KEY,
			  TRIM(MEMBER_NUM) AS MEMBER_NUM
			FROM
			  CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_DIM
			WHERE
			  ACTIVE_FLG='Y') MEM
		ON MEM.MEMBER_KEY=STG.MEMBER_KEY) SRC
ON (TGT.MEMBER_KEY=SRC.MEMBER_KEY and 	TGT.SOURCE_COMMENT_KEY=SRC.SOURCE_COMMENT_KEY)
WHEN MATCHED  THEN  UPDATE   SET TGT.UPDATE_DTTIME=SRC.UPDATE_DTTIME
  WHEN NOT MATCHED THEN
INSERT 
(
MEMBERSHIP_NUM,
MEMBER_NUM,
MEMBERSHIP_KEY,
MEMBER_KEY,
AR_ENROLL_DT,
AR_PROCESSED_EMP_ID,
AR_PROCESSED_EMP_LOCATION_ID,
ENROLLMENT_TYPE,
SOURCE_COMMENT_KEY,
SOURCE_COMMENT_DT,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
UPDATE_DTTIME
) 
VALUES
(
SRC.MEMBERSHIP_NUM,
SRC.MEMBER_NUM,
SRC.MEMBERSHIP_KEY,
SRC.MEMBER_KEY,
SRC.AR_ENROLL_DT,
SRC.AR_PROCESSED_EMP_ID,
SRC.AR_PROCESSED_EMP_LOCATION_ID,
SRC.ENROLLMENT_TYPE,
SRC.SOURCE_COMMENT_KEY,
SRC.SOURCE_COMMENT_DT,
899,
'CONNECT SUITE',
CURRENT_DATETIME()
)