--Overall flow of these script is
--Step0 Prepare the SAMS Customer data for the MDM process
-- #Step1 update dimension records that have already been tied to records in the bridge table
-- #Step2 Insert records into the bridge table for records that we have a match on the MEMBER_ID
-- #Step3 Insert records into the bridge table for records that we have a match on the name, and address
-- #Step4 Insert records into the bridge table for records that we have a match on the name, and email address
-- #Step5 Insert new records into the dimension table and bridge table for any new members
-- #MDM Process starts by creating the initial work table that contains all customer data currently in the system with fields standardized for MDM
--First step is to update records in the mdm_customer_dim that have previously been linked and then delete those records from the working table

--STEP 1: Find CUSTOMER_MDM_KEY on SAM CUSTOMER_ID and Set it the work table

UPDATE LANDING.WORK_MDM_COMM_LOGB A
SET A.CUSTOMER_MDM_KEY = B.CUSTOMER_MDM_KEY , MDM_MATCH_GROUP = 'SOURCE_KEY1'
FROM CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE B 
WHERE B.SOURCE_SYSTEM_CD = A.SOURCE_SYSTEM_CD
AND A.SOURCE_KEY1 = B.SOURCE_KEY1;

-- STEP 1.1: Find the Duplicates based on name, dob, address match and Move to Exception table.

INSERT INTO
  LANDING.MDM_CUSTOMER_EXCEPTIONS ( SOURCE_KEY1,
    SOURCE_KEY2,
    SOURCE_KEY3,
    SOURCE_KEY1_DESC,
    SOURCE_KEY2_DESC,	
    SOURCE_KEY3_DESC,
    SOURCE_SYSTEM_CD,
    FIRST_NM,
    LAST_NM,
	MIDDLE_NM,
    NAME_SUFFIX_CD,
    SALUTATION_CD,
    BIRTH_DT,
    GENDER_CD,
    PRIMARY_ADDRESS_LINE1,
    PRIMARY_ADDRESS_LINE2,
    PRIMARY_CITY,
    PRIMARY_STATE,
    PRIMARY_ZIP,
    PRIMARY_EMAIL_ADDRESS,
	PRIMARY_PHONE_NUM,
    RESIDENTIAL_ADDRESS_LINE1,
    RESIDENTIAL_ADDRESS_LINE2,
    RESIDENTIAL_CITY,
    RESIDENTIAL_STATE,
    RESIDENTIAL_ZIP,
    HOME_TELEPHONE_NUM,
    SOURCE_CREATE_DATE,
    CREATE_DTTIME)
SELECT
    SOURCE_KEY1,
    SOURCE_KEY2,
    SOURCE_KEY3,
    SOURCE_KEY1_DESC,
    SOURCE_KEY2_DESC,	
    SOURCE_KEY3_DESC,
    SOURCE_SYSTEM_CD,    
    FIRST_NM,
    LAST_NM,
	MIDDLE_NM,
    NAME_SUFFIX_CD,
    SALUTATION_CD,
    BIRTH_DT,
    GENDER_CD,
    PRIMARY_ADDRESS_LINE1,
    PRIMARY_ADDRESS_LINE2,
    PRIMARY_CITY,
    PRIMARY_STATE,
    PRIMARY_ZIP,
    PRIMARY_EMAIL_ADDRESS,
	PRIMARY_PHONE_NUM,
    RESIDENTIAL_ADDRESS_LINE1,
    RESIDENTIAL_ADDRESS_LINE2,
    RESIDENTIAL_CITY,
    RESIDENTIAL_STATE,
    RESIDENTIAL_ZIP,
        HOME_TELEPHONE_NUM,
        CAST(SOURCE_CREATE_DATE AS DATE),
    CURRENT_DATETIME()
FROM LANDING.WORK_MDM_COMM_LOGB
WHERE GOLDEN_MATCH_KEY in 
(select SOURCE_KEY1 FROM LANDING.WORK_MDM_COMM_LOGB C, CUSTOMERS.MDM_CUSTOMER_DIM D
WHERE  C.CUSTOMER_MDM_KEY IS NULL
AND C.MERGE_FLAG='Golden'
AND COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
AND CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is null and D.BIRTH_DT is not null THEN D.BIRTH_DT
		 ELSE C.BIRTH_DT END = CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is NOT null and D.BIRTH_DT is  null THEN C.BIRTH_DT
		 ELSE D.BIRTH_DT END
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'~')=COALESCE(D.ADDRESS_LINE1,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'~')=COALESCE(D.ADDRESS_LINE2,'~') 
AND COALESCE(C.PRIMARY_CITY,'~')=COALESCE(D.CITY,'~') 
AND COALESCE(C.PRIMARY_STATE,'~')=COALESCE(D.STATE,'~')
AND COALESCE(C.PRIMARY_ZIP,'~')=COALESCE(D.ZIP_CD,'~')
GROUP BY SOURCE_KEY1
having count(*)>1);

-- STEP 1.2: Find the Duplicates based on name, dob, address match and delete them from Landing table.

DELETE FROM LANDING.WORK_MDM_COMM_LOGB
WHERE GOLDEN_MATCH_KEY in 
(select SOURCE_KEY1 FROM LANDING.WORK_MDM_COMM_LOGB C, CUSTOMERS.MDM_CUSTOMER_DIM D
WHERE  C.CUSTOMER_MDM_KEY IS NULL
AND C.MERGE_FLAG='Golden'
AND COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
AND CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is null and D.BIRTH_DT is not null THEN D.BIRTH_DT
		 ELSE C.BIRTH_DT END = CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is NOT null and D.BIRTH_DT is  null THEN C.BIRTH_DT
		 ELSE D.BIRTH_DT END
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'~')=COALESCE(D.ADDRESS_LINE1,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'~')=COALESCE(D.ADDRESS_LINE2,'~') 
AND COALESCE(C.PRIMARY_CITY,'~')=COALESCE(D.CITY,'~') 
AND COALESCE(C.PRIMARY_STATE,'~')=COALESCE(D.STATE,'~')
AND COALESCE(C.PRIMARY_ZIP,'~')=COALESCE(D.ZIP_CD,'~')
GROUP BY SOURCE_KEY1
having count(*)>1);

-- STEP 1.3: Find the Duplicates based on name, dob, Email match and Move to Exception table.

INSERT INTO
  LANDING.MDM_CUSTOMER_EXCEPTIONS ( SOURCE_KEY1,
    SOURCE_KEY2,
    SOURCE_KEY3,
    SOURCE_KEY1_DESC,
    SOURCE_KEY2_DESC,	
    SOURCE_KEY3_DESC,
    SOURCE_SYSTEM_CD,
    FIRST_NM,
    LAST_NM,
	MIDDLE_NM,
    NAME_SUFFIX_CD,
    SALUTATION_CD,
    BIRTH_DT,
    GENDER_CD,
     PRIMARY_ADDRESS_LINE1,
    PRIMARY_ADDRESS_LINE2,
    PRIMARY_CITY,
    PRIMARY_STATE,
    PRIMARY_ZIP,
    PRIMARY_EMAIL_ADDRESS,
	PRIMARY_PHONE_NUM,
    RESIDENTIAL_ADDRESS_LINE1,
    RESIDENTIAL_ADDRESS_LINE2,
    RESIDENTIAL_CITY,
    RESIDENTIAL_STATE,
    RESIDENTIAL_ZIP,
    HOME_TELEPHONE_NUM,
    SOURCE_CREATE_DATE,
    CREATE_DTTIME)
SELECT
    SOURCE_KEY1,
    SOURCE_KEY2,
    SOURCE_KEY3,
    SOURCE_KEY1_DESC,
    SOURCE_KEY2_DESC,	
    SOURCE_KEY3_DESC,
    SOURCE_SYSTEM_CD,    
    FIRST_NM,
    LAST_NM,
	MIDDLE_NM,
    NAME_SUFFIX_CD,
    SALUTATION_CD,
    BIRTH_DT,
    GENDER_CD,
    PRIMARY_ADDRESS_LINE1,
    PRIMARY_ADDRESS_LINE2,
    PRIMARY_CITY,
    PRIMARY_STATE,
    PRIMARY_ZIP,
    PRIMARY_EMAIL_ADDRESS,
	PRIMARY_PHONE_NUM,
    RESIDENTIAL_ADDRESS_LINE1,
    RESIDENTIAL_ADDRESS_LINE2,
    RESIDENTIAL_CITY,
    RESIDENTIAL_STATE,
    RESIDENTIAL_ZIP,
        HOME_TELEPHONE_NUM,
        cast(SOURCE_CREATE_DATE as date),
    CURRENT_DATETIME()
FROM LANDING.WORK_MDM_COMM_LOGB
WHERE GOLDEN_MATCH_KEY in  (select SOURCE_KEY1 from
  LANDING.WORK_MDM_COMM_LOGB C, (SELECT   *  FROM CUSTOMERS.MDM_CUSTOMER_DIM
      WHERE NOT (TRIM(EMAIL_ADDRESS) is null or TRIM(EMAIL_ADDRESS)='') ) D
  WHERE  C.CUSTOMER_MDM_KEY IS NULL
AND C.MERGE_FLAG='Golden'
AND COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
    AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
    AND CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is null and D.BIRTH_DT is not null THEN D.BIRTH_DT
		 ELSE C.BIRTH_DT END = CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is NOT null and D.BIRTH_DT is  null THEN C.BIRTH_DT
		 ELSE D.BIRTH_DT END
    AND COALESCE(C.PRIMARY_EMAIL_ADDRESS,'1')=COALESCE(D.EMAIL_ADDRESS,'1')
	AND C.PRIMARY_EMAIL_ADDRESS is not null
  group by SOURCE_KEY1
  having count(*)>1);
  
-- STEP 1.4: Find the Duplicates based on name, dob, Email match and delete them from Landing table.
  
 DELETE FROM LANDING.WORK_MDM_COMM_LOGB
WHERE GOLDEN_MATCH_KEY in (select SOURCE_KEY1 from
  LANDING.WORK_MDM_COMM_LOGB C, (SELECT   *  FROM CUSTOMERS.MDM_CUSTOMER_DIM
      WHERE NOT (TRIM(EMAIL_ADDRESS) is null or TRIM(EMAIL_ADDRESS)='') ) D
  WHERE  C.CUSTOMER_MDM_KEY IS NULL
AND C.MERGE_FLAG='Golden'
AND COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
    AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
    AND CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is null and D.BIRTH_DT is not null THEN D.BIRTH_DT
		 ELSE C.BIRTH_DT END = CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is NOT null and D.BIRTH_DT is  null THEN C.BIRTH_DT
		 ELSE D.BIRTH_DT END
    AND COALESCE(C.PRIMARY_EMAIL_ADDRESS,'1')=COALESCE(D.EMAIL_ADDRESS,'1')
	AND C.PRIMARY_EMAIL_ADDRESS is not null
  group by SOURCE_KEY1
  having count(*)>1);
  





--STEP 2: Find CUSTOMER_MDM_KEY on name, dob, address match and Set it the work table

UPDATE LANDING.WORK_MDM_COMM_LOGB C
SET CUSTOMER_MDM_KEY = D.CUSTOMER_MDM_KEY , MDM_MATCH_GROUP = 'RULES'
FROM CUSTOMERS.MDM_CUSTOMER_DIM D
WHERE  C.CUSTOMER_MDM_KEY IS NULL
AND C.MERGE_FLAG='Golden'
AND COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
AND CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is null and D.BIRTH_DT is not null THEN D.BIRTH_DT
		 ELSE C.BIRTH_DT END = CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is NOT null and D.BIRTH_DT is  null THEN C.BIRTH_DT
		 ELSE D.BIRTH_DT END
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'~')=COALESCE(D.ADDRESS_LINE1,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'~')=COALESCE(D.ADDRESS_LINE2,'~') 
AND COALESCE(C.PRIMARY_CITY,'~')=COALESCE(D.CITY,'~') 
AND COALESCE(C.PRIMARY_STATE,'~')=COALESCE(D.STATE,'~')
AND COALESCE(C.PRIMARY_ZIP,'~')=COALESCE(D.ZIP_CD,'~');

--STEP 3: Find CUSTOMER_MDM_KEY on name, dob, email address match and Set it the work table

UPDATE LANDING.WORK_MDM_COMM_LOGB C
SET CUSTOMER_MDM_KEY = D.CUSTOMER_MDM_KEY , MDM_MATCH_GROUP = 'RULES'
 FROM  ( SELECT   *  FROM CUSTOMERS.MDM_CUSTOMER_DIM
      WHERE NOT (TRIM(EMAIL_ADDRESS) is null or TRIM(EMAIL_ADDRESS)='') ) D
WHERE  C.CUSTOMER_MDM_KEY IS NULL
AND C.MERGE_FLAG='Golden'
AND COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
    AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
    AND CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is null and D.BIRTH_DT is not null THEN D.BIRTH_DT
		 ELSE C.BIRTH_DT END = CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is NOT null and D.BIRTH_DT is  null THEN C.BIRTH_DT
		 ELSE D.BIRTH_DT END
    AND COALESCE(C.PRIMARY_EMAIL_ADDRESS,'1')=COALESCE(D.EMAIL_ADDRESS,'1')
	AND C.PRIMARY_EMAIL_ADDRESS is not null;

--STEP 4: UPDATE CUSTOMER INFORMATION INTO MDM_CUSTOMER_DIM for the exsiting customers.

UPDATE
  CUSTOMERS.MDM_CUSTOMER_DIM A
SET
  A.FIRST_NM=CASE
    WHEN B.FIRST_NM IS NULL THEN A.FIRST_NM
    ELSE B.FIRST_NM END,
  A.LAST_NM=
  CASE
    WHEN B.LAST_NM IS NULL THEN A.LAST_NM
    ELSE B.LAST_NM END,
  A.NAME_SUFFIX_CD=CASE
    WHEN B.NAME_SUFFIX_CD IS NULL THEN A.NAME_SUFFIX_CD
    ELSE B.NAME_SUFFIX_CD END,
  A.MIDDLE_NM = CASE WHEN B.MIDDLE_NM IS NULL THEN A.MIDDLE_NM
    ELSE B.MIDDLE_NM END,
  A.BIRTH_DT=CASE
    WHEN B.BIRTH_DT IS NULL THEN A.BIRTH_DT
    ELSE B.BIRTH_DT END,
  A.GENDER_CD=CASE
    WHEN B.GENDER_CD IS NULL THEN A.GENDER_CD
    ELSE B.GENDER_CD END,
  A.ADDRESS_LINE1=CASE
    WHEN B.PRIMARY_ADDRESS_LINE1 IS NULL THEN A.ADDRESS_LINE1
    ELSE B.PRIMARY_ADDRESS_LINE1 END,
  A.ADDRESS_LINE2=CASE
    WHEN B.PRIMARY_ADDRESS_LINE1 IS NULL THEN A.ADDRESS_LINE2
    ELSE B.PRIMARY_ADDRESS_LINE2 END,
  A.CITY=CASE
    WHEN B.PRIMARY_ADDRESS_LINE1 IS NULL THEN A.CITY
    ELSE B.PRIMARY_CITY END,
  A.STATE=CASE
    WHEN B.PRIMARY_ADDRESS_LINE1 IS NULL THEN A.STATE
    ELSE B.PRIMARY_STATE END,
  A.ZIP_CD=CASE
    WHEN B.PRIMARY_ADDRESS_LINE1 IS NULL THEN A.ZIP_CD
    ELSE B.PRIMARY_ZIP END,
  A.EMAIL_ADDRESS=CASE
    WHEN B.PRIMARY_EMAIL_ADDRESS IS NULL THEN A.EMAIL_ADDRESS
    ELSE B.PRIMARY_EMAIL_ADDRESS END,
  A.PHONE_NUM = B.PRIMARY_PHONE_NUM,
  A.SOURCE_SYSTEM_CD = B.SOURCE_SYSTEM_CD,
     A.ADDRESS_MATCH_KEY1=CASE
    WHEN B.ADDRESS_MATCH_KEY1 IS NULL THEN A.ADDRESS_MATCH_KEY1
    ELSE B.ADDRESS_MATCH_KEY1 END,  
  A.ADDRESS_MATCH_KEY2=CASE
    WHEN B.ADDRESS_MATCH_KEY2 IS NULL THEN A.ADDRESS_MATCH_KEY2
    ELSE B.ADDRESS_MATCH_KEY2 END, 
  A.UPDATE_DTTIME =  CURRENT_DATETIME()
FROM (
  SELECT
    CUSTOMER_MDM_KEY,
    FIRST_NM,
    LAST_NM,
	MIDDLE_NM,
    NAME_SUFFIX_CD,
    SALUTATION_CD,
    BIRTH_DT,
	GENDER_CD,
	PRIMARY_ADDRESS_LINE1,
	PRIMARY_ADDRESS_LINE2,
	PRIMARY_CITY,
	PRIMARY_STATE,
	PRIMARY_ZIP,
	PRIMARY_EMAIL_ADDRESS,
	PRIMARY_PHONE_NUM,
	SOURCE_SYSTEM_CD,
    SOURCE_CREATE_DATE,
	ADDRESS_MATCH_KEY1,
	ADDRESS_MATCH_KEY2,
   ROW_NUMBER() OVER (PARTITION BY CUSTOMER_MDM_KEY ORDER BY SOURCE_CREATE_DATE DESC) AS DUP_CHECK
  FROM (SELECT * FROM LANDING.WORK_MDM_COMM_LOGB WHERE CUSTOMER_MDM_KEY IS NOT NULL AND MERGE_FLAG='Golden')  ) B
WHERE A.CUSTOMER_MDM_KEY=B.CUSTOMER_MDM_KEY
AND A.SOURCE_SYSTEM_CD <> 'MEMBERSHIP'
  AND DUP_CHECK=1;
  
--Overall flow of these script is
--Step0 Prepare the SAMS Customer data for the MDM process
-- #Step1 update dimension records that have already been tied to records in the bridge table
-- #Step2 Insert records into the bridge table for records that we have a match on the MEMBER_ID
-- #Step3 Insert records into the bridge table for records that we have a match on the name, and address
-- #Step4 Insert records into the bridge table for records that we have a match on the name, and email address
-- #Step5 Insert new records into the dimension table and bridge table for any new members
-- #MDM Process starts by creating the initial work table that contains all customer data currently in the system with fields standardized for MDM
--First step is to update records in the mdm_customer_dim that have previously been linked and then delete those records from the working table


-- STEP 5: INSERT NEW CUSTOMER INFORMATION INTO MDM_CUSTOMER_DIM.

INSERT INTO
  CUSTOMERS.MDM_CUSTOMER_DIM ( 
  CUSTOMER_MDM_KEY,
    FIRST_NM,
    LAST_NM,
	NAME_SUFFIX_CD,
    SALUTATION_CD,
	MIDDLE_NM,
    BIRTH_DT,
    GENDER_CD,
		  ADDRESS_LINE1,
    ADDRESS_LINE2,
    CITY,
    STATE,
    ZIP_CD,
    EMAIL_ADDRESS,
    PHONE_NUM,
	SOURCE_SYSTEM_CD,
    CREATE_DTTIME,
	UPDATE_DTTIME,
	ADDRESS_MATCH_KEY1,
    ADDRESS_MATCH_KEY2)
SELECT
  RECORD_NUMBER + (
  SELECT
   CASE WHEN MAX(CUSTOMER_MDM_KEY) IS NULL THEN 0 ELSE MAX(CUSTOMER_MDM_KEY) END AS CUSTOMER_MDM_KEY
  FROM
    CUSTOMERS.MDM_CUSTOMER_DIM) AS CUSTOMER_MDM_KEY,
  FIRST_NM,
  LAST_NM,
  NAME_SUFFIX_CD,
  SALUTATION_CD,
  MIDDLE_NM,
  BIRTH_DT,
  GENDER_CD,
    PRIMARY_ADDRESS_LINE1,
  PRIMARY_ADDRESS_LINE2,
  PRIMARY_CITY,
  PRIMARY_STATE,
  PRIMARY_ZIP,
  PRIMARY_EMAIL_ADDRESS,
  PRIMARY_PHONE_NUM,
  SOURCE_SYSTEM_CD,
  CURRENT_DATETIME(),
  CURRENT_DATETIME(),
  ADDRESS_MATCH_KEY1,
  ADDRESS_MATCH_KEY2
FROM (
  SELECT
    ROW_NUMBER() OVER() RECORD_NUMBER,
    C.FIRST_NM,
    C.LAST_NM,
	C.NAME_SUFFIX_CD,
    C.SALUTATION_CD,
	C.MIDDLE_NM,
    C.BIRTH_DT,
    C.GENDER_CD,
    	  C.PRIMARY_ADDRESS_LINE1,
    C.PRIMARY_ADDRESS_LINE2,
    C.PRIMARY_CITY,
    C.PRIMARY_STATE,
    C.PRIMARY_ZIP,
    C.PRIMARY_EMAIL_ADDRESS,
    C.PRIMARY_PHONE_NUM,
    C.SOURCE_SYSTEM_CD,
	C.SOURCE_CREATE_DATE,
	C.ADDRESS_MATCH_KEY1,
    C.ADDRESS_MATCH_KEY2
  FROM
    LANDING.WORK_MDM_COMM_LOGB C
  WHERE MERGE_FLAG='Golden' AND CUSTOMER_MDM_KEY IS NULL ); 
 	
--STEP6: Find CUSTOMER_MDM_KEY on name, dob, address match and Set it the work table

UPDATE LANDING.WORK_MDM_COMM_LOGB C
SET CUSTOMER_MDM_KEY = D.CUSTOMER_MDM_KEY , MDM_MATCH_GROUP = 'RULES'
FROM CUSTOMERS.MDM_CUSTOMER_DIM D
WHERE  C.CUSTOMER_MDM_KEY IS NULL
AND C.MERGE_FLAG='Golden'
AND COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
AND C.SOURCE_SYSTEM_CD = D.SOURCE_SYSTEM_CD
AND CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is null and D.BIRTH_DT is not null THEN D.BIRTH_DT
		 ELSE C.BIRTH_DT END = CASE WHEN C.BIRTH_DT is null and D.BIRTH_DT is null THEN CURRENT_DATE()
         WHEN C.BIRTH_DT is NOT null and D.BIRTH_DT is  null THEN C.BIRTH_DT
		 ELSE D.BIRTH_DT END
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'~')=COALESCE(D.ADDRESS_LINE1,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'~')=COALESCE(D.ADDRESS_LINE2,'~') 
AND COALESCE(C.PRIMARY_CITY,'~')=COALESCE(D.CITY,'~') 
AND COALESCE(C.PRIMARY_STATE,'~')=COALESCE(D.STATE,'~')
AND COALESCE(C.PRIMARY_ZIP,'~')=COALESCE(D.ZIP_CD,'~')
AND COALESCE(C.PRIMARY_EMAIL_ADDRESS,'~')=COALESCE(D.EMAIL_ADDRESS,'~')	;

--STEP6: UPDATE CUSTOMER_MDM_KEY ON NON GOLDEN CUSTOMERS IN WORK TABLE.
	
UPDATE LANDING.WORK_MDM_COMM_LOGB C
SET C.CUSTOMER_MDM_KEY = D.CUSTOMER_MDM_KEY , MDM_MATCH_GROUP = 'RULES'
 FROM (SELECT * FROM LANDING.WORK_MDM_COMM_LOGB WHERE MERGE_FLAG='Golden') D
 WHERE C.CUSTOMER_MDM_KEY IS NULL
 AND C.MERGE_FLAG <> 'Golden' 
 AND C.GOLDEN_MATCH_KEY = D.SOURCE_KEY1;

--STEP7: INSERT NEW/MATCHED CUSTOMERS DATA INTO BRIDGE TABLE.

INSERT INTO CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE(
SOURCE_SYSTEM_CD,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_DESC,
SOURCE_KEY2,
SOURCE_KEY2_DESC,
BRIDGE_SEQ_ID ,
CREATE_DTTIME, 
UPDATE_DTTIME )
SELECT 
SOURCE_SYSTEM_CD,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_DESC,
SOURCE_KEY2,
SOURCE_KEY2_DESC,
RECORD_NUMBER + (SELECT CASE WHEN MAX(BRIDGE_SEQ_ID) IS NULL THEN 0 ELSE MAX(BRIDGE_SEQ_ID) END FROM CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE) AS BRIDGE_SEQ_ID,
CURRENT_DATETIME(),
CURRENT_DATETIME()
FROM
(SELECT 
SOURCE_SYSTEM_CD,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_DESC,
SOURCE_KEY2,
SOURCE_KEY2_DESC,
ROW_NUMBER() OVER() RECORD_NUMBER
FROM LANDING.WORK_MDM_COMM_LOGB 
WHERE MDM_MATCH_GROUP <> 'SOURCE_KEY1'
);

--STEP 8: UPDATE MD5_KEY's in WORK_MDM_COMM_LOGB.


SET HOME_MD5_KEY=  MD5(CONCAT( COALESCE(CAST(SOURCE_KEY1 AS STRING),' ')
, COALESCE(CAST(SOURCE_KEY1_DESC AS STRING),' ')
, COALESCE(CAST(SOURCE_KEY2 AS STRING),' ')
, COALESCE(CAST(SOURCE_KEY1_DESC AS STRING),' ')
, COALESCE(CAST(SOURCE_SYSTEM_CD AS STRING),' ')
, COALESCE(CAST('HOME' AS STRING),' ')
, COALESCE(CAST(RESIDENTIAL_ADDRESS_LINE1 AS STRING),' ')
, COALESCE(CAST(RESIDENTIAL_ADDRESS_LINE2 AS STRING),' ')
, COALESCE(CAST(RESIDENTIAL_CITY AS STRING),' ')
, COALESCE(CAST(RESIDENTIAL_STATE AS STRING),' ')
, COALESCE(CAST(RESIDENTIAL_ZIP	 AS STRING),' ')
, COALESCE(CAST(PRIMARY_EMAIL_ADDRESS AS STRING),' ')	
, COALESCE(CAST(HOME_TELEPHONE_NUM AS STRING),' ')))
WHERE TRUE;

--STEP 9: INSERT CUSTOMERS CONTACT DATA INTO MDM_CUSTOMER_CONTACT_DIM TABLE.

INSERT INTO  CUSTOMERS.MDM_CUSTOMER_CONTACT_DIM (CUSTOMER_MDM_KEY
,SOURCE_KEY1
,SOURCE_KEY1_DESC
,SOURCE_KEY2
,SOURCE_KEY2_DESC
,TYPE
,ADDRESS_LINE1
,ADDRESS_LINE2
,CITY
,STATE
,ZIP_CD
,EMAIL_ADDRESS
,PHONE_NUM
,ACTIVE_FLG
,ROW_START_DT
,ROW_END_DT
,MD5_KEY
,SOURCE_SYSTEM_CD
,CREATE_DTTIME
,UPDATE_DTTIME)
SELECT DISTINCT CUSTOMER_MDM_KEY
,SOURCE_KEY1
,SOURCE_KEY1_DESC
, SOURCE_KEY2
, SOURCE_KEY2_DESC
,TYPE
, ADDRESS_LINE1
, ADDRESS_LINE2
, CITY
, STATE
, ZIP_CD
, EMAIL_ADDRESS
, PHONE_NUM 
, 'Y' AS ACTIVE_FLG
, CURRENT_DATE() AS ROW_START_DT
, DATE('9999-12-31') AS ROW_END_DT
, MD5_KEY
, SOURCE_SYSTEM_CD
,CURRENT_DATETIME() AS CREATE_DTTIME
,CURRENT_DATETIME() AS UPDATE_DTTIME
FROM (
SELECT 
CUSTOMER_MDM_KEY
,SOURCE_KEY1
,SOURCE_KEY1_DESC
,SOURCE_KEY2
,SOURCE_KEY2_DESC
,SOURCE_SYSTEM_CD
,'HOME' AS TYPE
, PRIMARY_ADDRESS_LINE1 AS  ADDRESS_LINE1
, PRIMARY_ADDRESS_LINE2 AS  ADDRESS_LINE2
, PRIMARY_CITY AS  CITY
, PRIMARY_STATE AS  STATE
, PRIMARY_ZIP AS  ZIP_CD
, PRIMARY_EMAIL_ADDRESS AS EMAIL_ADDRESS
, HOME_TELEPHONE_NUM AS PHONE_NUM
, HOME_MD5_KEY AS MD5_KEY
FROM LANDING.WORK_MDM_COMM_LOGB
WHERE PRIMARY_ADDRESS_LINE1 IS NOT NULL OR HOME_TELEPHONE_NUM IS NULL OR HOME_TELEPHONE_NUM IS NOT NULL
) S
WHERE S.MD5_KEY NOT IN ( SELECT MD5_KEY FROM CUSTOMERS.MDM_CUSTOMER_CONTACT_DIM WHERE ACTIVE_FLG='Y');

--STEP 10: TYPE 2 UPDATE FOR MDM_CUSTOMER_CONTACT_DIM TABLE.

UPDATE CUSTOMERS.MDM_CUSTOMER_CONTACT_DIM DIM
SET DIM.ROW_END_DT = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), 
DIM.UPDATE_DTTIME = CURRENT_DATETIME(),
ACTIVE_FLG = 'N'
FROM (SELECT CUSTOMER_MDM_KEY, TYPE, SOURCE_KEY1, SOURCE_KEY2 ,ROW_START_DT,ACTIVE_FLG FROM 
(SELECT CUSTOMER_MDM_KEY, TYPE, SOURCE_KEY1, SOURCE_KEY2 ,ROW_START_DT,ACTIVE_FLG, 
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_MDM_KEY, TYPE, SOURCE_KEY1 ORDER BY ROW_START_DT DESC ) DUP_CHECK 
FROM CUSTOMERS.MDM_CUSTOMER_CONTACT_DIM WHERE ACTIVE_FLG = 'Y') 
WHERE DUP_CHECK=2) TEMP
WHERE DIM.CUSTOMER_MDM_KEY = TEMP.CUSTOMER_MDM_KEY
AND DIM.TYPE = TEMP.TYPE
AND DIM.SOURCE_KEY1 = TEMP.SOURCE_KEY1
AND TEMP.ROW_START_DT = DIM.ROW_START_DT