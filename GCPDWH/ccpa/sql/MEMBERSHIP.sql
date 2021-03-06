MERGE COMPLIANCE.CCPA_CUSTOMER_DETAILS AS TGT
USING (
		Select A.*, B.CC_NUMBER, C.BANK_ACCOUNT_NUMBER from
		(SELECT * FROM CUSTOMERS.CONNECTSUITE_MEMBER
		WHERE CUSTOMER_MDM_KEY IS NOT NULL) A
		LEFT OUTER JOIN
		(select MEMBERSHIP_KEY, E_PAYMENT_METHOD, STRING_AGG(distinct ACCOUNT_NUMBER, '|' ) as CC_NUMBER  from OPERATIONAL.CONNECTSUITE_E_PAYMENT_BILLING
		where E_PAYMENT_METHOD = 'CC'
		group by 1,2 ) B
		ON A.MEMBERSHIP_KEY = B.MEMBERSHIP_KEY
		LEFT OUTER JOIN
		(select MEMBERSHIP_KEY, E_PAYMENT_METHOD, STRING_AGG(distinct ACCOUNT_NUMBER, '|' ) as BANK_ACCOUNT_NUMBER  from OPERATIONAL.CONNECTSUITE_E_PAYMENT_BILLING
		where E_PAYMENT_METHOD = 'ECP'
		group by 1,2) C
		ON A.MEMBERSHIP_KEY = C.MEMBERSHIP_KEY
) AS SRC
ON TGT.CUSTOMER_MDM_KEY = SRC.CUSTOMER_MDM_KEY and
TGT.SOURCE_KEY = SRC.MEMBER_NUM and
TGT.SOURCE_SYSTEM_CD = SRC.SOURCE_SYSTEM_CD
WHEN MATCHED THEN UPDATE SET
TGT.TERM_EXPIRATION_DT = SRC.TERM_EXPIRY_DT
,TGT.BANK_ACCOUNT_NUM = SRC.BANK_ACCOUNT_NUMBER
,TGT.CREDIT_CARD_NUM = SRC.CC_NUMBER
,TGT.CANCEL_DT = SRC.CANCEL_DT
,TGT.PRODUCT_STATUS = SRC.STATUS_CD
,TGT.PRODUCT_TYPE = SRC.MEMBERSHIP_LEVEL_DESC
,TGT.RELATIONSHIP_STATUS = CASE WHEN UPPER(SRC.STATUS_CD) = 'C' THEN 'Former' ELSE 'Current' END
,TGT.UPDATE_DTTIME =  CURRENT_DATETIME()
WHEN NOT MATCHED THEN INSERT
(CUSTOMER_MDM_KEY
,SOURCE_KEY
,TERM_EXPIRATION_DT
,CANCEL_DT
,PRODUCT_STATUS
,PRODUCT_TYPE
,RELATIONSHIP_STATUS
,BANK_ACCOUNT_NUM
,CREDIT_CARD_NUM
,COMPANY
,APPLICATION_NM
,SOURCE_SYSTEM_CD
,CREATE_DTTIME
,UPDATE_DTTIME)
VALUES(
CUSTOMER_MDM_KEY ,
MEMBER_NUM,
TERM_EXPIRY_DT,
CANCEL_DT,
STATUS_CD,
MEMBERSHIP_LEVEL_DESC,
CASE WHEN UPPER(STATUS_CD) = 'C' THEN 'Former' ELSE 'Current' END,
SRC.BANK_ACCOUNT_NUMBER,
SRC.CC_NUMBER,
'AAA MWG',
'CONNECT_SUITE',
'MEMBERSHIP',
CURRENT_DATETIME(),
CURRENT_DATETIME())