SELECT   
CAST(CUSTID AS INT64) CUST_ID, 
CUSTNAME CUST_NAME, 
FIRSTNAME FIRST_NAME, 
LASTNAME LAST_NAME, 
TITLE, 
COMPANY, 
ADDRESS1, 
ADDRESS2, 
CITY, 
STATE, 
ZIP, 
TAXEXEMPT TAX_EXEMPT, 
CAST(TAXCATEGORY AS INT64) TAX_CATEGORY, 
TAXCATDESC TAX_CAT_DESC, 
PARSE_DATETIME('%m/%d/%Y %H:%M %p', DATELASTSERVICE) DATE_LAST_SERVICE, 
POREQUIRED PO_REQUIRED, 
CONTACTNAME CONTACT_NAME, 
CONTACTPHONE CONTACT_PHONE,
PARSE_DATETIME('%m/%d/%Y %H:%M %p', CREATEDATE)  CREATE_DATE, 
NOTES, 
ARACCT AR_ACCT, 
CAST(PAYDUEDAYS AS INT64) PAY_DUE_DAYS, 
STMTIFTRXS STMT_IF_TRXS, 
BNAME B_NAME, 
BADDRESS1 B_ADDRESS1, 
BADDRESS2 B_ADDRESS2, 
BCITY B_CITY, 
BSTATE B_STATE, 
BZIP B_ZIP, 
EXEMPTID EXEMPT_ID, 
CPID CP_ID, 
CUSTACTIVE CUST_ACTIVE, 
FULLPAYMENTREQUIRED FULL_PAYMENT_REQUIRED, 
CUSTOMERTYPE CUSTOMER_TYPE, 
CREDITLIMIT CREDIT_LIMIT, 
DAYNUM DAY_NUM, 
EVENINGNUM EVENING_NUM, 
FAXNUM FAX_NUM, 
EMAIL, 
FCYN FC_YN, 
FCGRACEDAYS FC_GRACE_DAYS, 
CUSTGROUP CUST_GROUP, 
MEMBERSHIPNUM MEMBER_NUM, 
CELLNUM CELL_NUM, 
ALTPHONE1 ALT_PHONE1, 
ALTPHONE2 ALT_PHONE2, 
EXTRACT(DATE FROM PARSE_DATETIME('%m/%d/%Y %H:%M %p', MEMNUMEXPDATE)) MEM_NUM_EXP_DATE, 
CHARGESHOPSUPPLIESYN CHARGE_SHOP_SUPPLIES_YN, 
AAAID AAA_ID, 
OKTOCONTACT OKTO_CONTACT
FROM `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_CUSTOMER` 