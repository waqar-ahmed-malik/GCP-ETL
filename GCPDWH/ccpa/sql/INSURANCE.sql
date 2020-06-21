MERGE COMPLIANCE.CCPA_CUSTOMER_DETAILS AS TGT
USING (
select DISTINCT A.CUSTOMER_MDM_KEY, A.AGREEMENT_NUM, B.SOURCE_SYSTEM_CD, B.PRODUCT_TYPE, B.STATUS_CD, B.TERM_EXPIRATION_DT, B.CANCEL_DT, C.CLAIM_NUM, D.AUTO_VIN
from (select DISTINCT CUSTOMER_MDM_KEY, AGREEMENT_NUM FROM CUSTOMER_PRODUCT.INSURANCE_CUSTOMER_DIM WHERE CUSTOMER_MDM_KEY IS NOT NULL) A,
(select * from CUSTOMER_PRODUCT.INSURANCE_POLICY_DIM where ACTIVE_FLG = 'Y') B,
(SELECT  AGREEMENT_NUM, STRING_AGG(distinct CLAIM_NUM, '|' ) as CLAIM_NUM from CUSTOMER_PRODUCT.INSURANCE_CLAIMS_DIM group by AGREEMENT_NUM) C,
(SELECT  AGREEMENT_NUM, STRING_AGG(distinct AUTO_VIN, '|' ) as AUTO_VIN from CUSTOMER_PRODUCT.INSURANCE_UNIT_DETAILS_DIM group by AGREEMENT_NUM) D
where 1=1
and A.AGREEMENT_NUM = B.AGREEMENT_NUM
and A.AGREEMENT_NUM = C.AGREEMENT_NUM
and A.AGREEMENT_NUM = D.AGREEMENT_NUM
) AS SRC
ON TGT.CUSTOMER_MDM_KEY = SRC.CUSTOMER_MDM_KEY and
TGT.SOURCE_KEY = SRC.AGREEMENT_NUM and
TGT.SOURCE_SYSTEM_CD = SRC.SOURCE_SYSTEM_CD
WHEN MATCHED THEN UPDATE SET
TGT.TERM_EXPIRATION_DT = SRC.TERM_EXPIRATION_DT
,TGT.CANCEL_DT = SRC.CANCEL_DT
,TGT.PRODUCT_STATUS = SRC.STATUS_CD
,TGT.PRODUCT_TYPE = SRC.PRODUCT_TYPE
,TGT.CLAIM_HISTORY = SRC.CLAIM_NUM
,TGT.VEHICLE_IDENTIFIERS_NUM = SRC.AUTO_VIN
,TGT.RELATIONSHIP_STATUS = CASE WHEN UPPER(SRC.STATUS_CD) = 'I' THEN 'Former' ELSE 'Current' END
,TGT.UPDATE_DTTIME =  CURRENT_DATETIME()
WHEN NOT MATCHED THEN INSERT
(CUSTOMER_MDM_KEY
,SOURCE_KEY
,TERM_EXPIRATION_DT
,CANCEL_DT
,PRODUCT_STATUS
,PRODUCT_TYPE
,CLAIM_HISTORY
,VEHICLE_IDENTIFIERS_NUM
,RELATIONSHIP_STATUS
,COMPANY
,SOURCE_SYSTEM_CD
,CREATE_DTTIME
,UPDATE_DTTIME)
VALUES(
CUSTOMER_MDM_KEY ,
AGREEMENT_NUM,
TERM_EXPIRATION_DT,
CANCEL_DT,
STATUS_CD,
PRODUCT_TYPE,
CLAIM_NUM,
AUTO_VIN,
CASE WHEN UPPER(STATUS_CD) = 'I' THEN 'Former' ELSE 'Current' END,
'AAA MWG',
SOURCE_SYSTEM_CD,
CURRENT_DATETIME(),
CURRENT_DATETIME())