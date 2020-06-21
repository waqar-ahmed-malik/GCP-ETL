--Overall flow of these script is
--Step0 Prepare the TRAVEL Customer data for the MDM process
-- #Step1 update dimension records that have already been tied to records in the bridge table
-- #Step2 Insert records into the bridge table for records that we have a match on the MEMBER_ID
-- #Step3 Insert records into the bridge table for records that we have a match on the name, and address
-- #Step4 Insert records into the bridge table for records that we have a match on the name, and email address
-- #Step5 Insert new records into the dimension table and bridge table for any new members
-- #MDM Process starts by creating the initial work table that contains all customer data currently in the system with fields standardized for MDM

--First step is to update records in the mdm_customer_dim that have previously been linked and then delete those records from the working table
UPDATE ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL A
SET
A.FIRST_NM=CASE WHEN A.FIRST_NM IS NULL THEN B.FIRST_NM ELSE A.FIRST_NM END, 
A.LAST_NM= CASE WHEN A.LAST_NM IS NULL THEN B.LAST_NM ELSE A.LAST_NM END,
A.PRIMARY_ADDRESS_LINE1=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_ADDRESS_LINE1 ELSE A.MEM_RESIDENTIAL_ADDRESS_LINE1 END, 
A.PRIMARY_ADDRESS_LINE2=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_ADDRESS_LINE2 ELSE A.MEM_RESIDENTIAL_ADDRESS_LINE2 END, 
A.PRIMARY_CITY=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_CITY ELSE A.MEM_RESIDENTIAL_CITY END,
A.PRIMARY_STATE=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_STATE ELSE A.MEM_RESIDENTIAL_STATE END,
A.PRIMARY_ZIP=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_ZIP ELSE A.MEM_RESIDENTIAL_ZIP END,
A.PRIMARY_EMAIL_ADDRESS=CASE WHEN A.PRIMARY_EMAIL_ADDRESS IS NULL THEN B.PRIMARY_EMAIL_ADDRESS ELSE A.PRIMARY_EMAIL_ADDRESS END,
A.TRAVEL_RESIDENTIAL_ADDRESS_LINE1=RESIDENTIAL_ADDRESS_LINE1,
A.TRAVEL_RESIDENTIAL_ADDRESS_LINE2=RESIDENTIAL_ADDRESS_LINE2,
A.TRAVEL_RESIDENTIAL_CITY=RESIDENTIAL_CITY,
A.TRAVEL_RESIDENTIAL_STATE=RESIDENTIAL_STATE,
A.TRAVEL_RESIDENTIAL_ZIP=RESIDENTIAL_ZIP,
A.TRAVEL_BILLING_ADDRESS_LINE1=BILLING_ADDRESS_LINE1,
A.TRAVEL_BILLING_ADDRESS_LINE2=BILLING_ADDRESS_LINE2,
A.TRAVEL_BILLING_CITY=BILLING_CITY,
A.TRAVEL_BILLING_STATE=BILLING_STATE,
A.TRAVEL_BILLING_ZIP=BILLING_ZIP,
A.TRAVEL_OTHER_ADDRESS_LINE1=OTHER_ADDRESS_LINE1,
A.TRAVEL_OTHER_ADDRESS_LINE2=OTHER_ADDRESS_LINE2,
A.TRAVEL_OTHER_CITY=OTHER_CITY,
A.TRAVEL_OTHER_STATE=OTHER_STATE,
A.TRAVEL_OTHER_ZIP=OTHER_ZIP,	
A.TRAVEL_RESIDENTIAL_PHONE_NUM = B.HOME_TELEPHONE_NUM,
A.TRAVEL_WORK_PHONE_NUM = B.WORK_PHONE_NUM,    
A.TRAVEL_CREATE_DATE = B.SOURCE_CREATE_DATE  
FROM (
SELECT 
CUSTOMER_MDM_KEY,
FIRST_NM,
LAST_NM,
NAME_SUFFIX_CD,
SALUTATION_CD,
GENDER_CD,
MARITAL_STATUS,
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP,
PRIMARY_EMAIL_ADDRESS,
RESIDENTIAL_ADDRESS_LINE1,
RESIDENTIAL_ADDRESS_LINE2,
RESIDENTIAL_CITY,
RESIDENTIAL_STATE,
RESIDENTIAL_ZIP,
BILLING_ADDRESS_LINE1,
BILLING_ADDRESS_LINE2,
BILLING_CITY,
BILLING_STATE,
BILLING_ZIP,
OTHER_ADDRESS_LINE1,
OTHER_ADDRESS_LINE2,
OTHER_CITY,
OTHER_STATE,
OTHER_ZIP,
OTHER_EMAIL_ADDRESS,
HOME_TELEPHONE_NUM,
WORK_PHONE_NUM,
OTHER_PHONE_NUM,
SOURCE_CREATE_DATE,
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_MDM_KEY ORDER BY SOURCE_CREATE_DATE DESC) AS DUP_CHECK
FROM `LANDING.WORK_MDM_TRAVEL` C
INNER JOIN ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL D
ON C.SOURCE_KEY1=D.SOURCE_KEY1
AND D.SOURCE_SYSTEM=C.SOURCE_SYSTEM
) B
WHERE A.CUSTOMER_MDM_KEY=B.CUSTOMER_MDM_KEY AND DUP_CHECK=1;

--STEP1: previously linked customers are updated, removing those customers from the work table

DELETE FROM `LANDING.WORK_MDM_TRAVEL` A
WHERE EXISTS
(SELECT 1 FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL B WHERE A.SOURCE_KEY1=B.SOURCE_KEY1
AND A.SOURCE_SYSTEM=B.SOURCE_SYSTEM);

--STEP1: previously linked records are deleted
-- STEP1: record count in LANDING.WORK_MDM_TRAVEL is
-- STEP1: cleaning up records that have been updated to have the same nk
-- STEP1: updating the bridge table 

UPDATE ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL D
SET D.CUSTOMER_MDM_KEY=C.PRIMARY_CUSTOMER_MDM_KEY
FROM
(SELECT 
B.PRIMARY_CUSTOMER_MDM_KEY,
A.CUSTOMER_MDM_KEY
FROM
(
SELECT
CUSTOMER_MDM_KEY,
FIRST_NM, 
LAST_NM, 
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP,
ROW_NUMBER() OVER (PARTITION BY FIRST_NM, LAST_NM, PRIMARY_ADDRESS_LINE1,PRIMARY_ADDRESS_LINE2,PRIMARY_CITY, PRIMARY_STATE,PRIMARY_ZIP ORDER BY CUSTOMER_MDM_KEY ASC) AS DUP_CHECK
from ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL 
)A 
INNER JOIN
(SELECT 
MIN(CUSTOMER_MDM_KEY) AS PRIMARY_CUSTOMER_MDM_KEY,
FIRST_NM, 
LAST_NM,  
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP
from ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL
GROUP BY 
FIRST_NM, 
LAST_NM, 
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP)
B
ON A.FIRST_NM=B.FIRST_NM 
AND COALESCE(A.LAST_NM,'1')=COALESCE(B.LAST_NM,'1')
AND A.PRIMARY_ADDRESS_LINE1=B.PRIMARY_ADDRESS_LINE1 
AND COALESCE(A.PRIMARY_ADDRESS_LINE2,'1')=COALESCE(B.PRIMARY_ADDRESS_LINE2,'1')
AND A.PRIMARY_CITY=B.PRIMARY_CITY 
AND A.PRIMARY_STATE=B.PRIMARY_STATE
AND A.PRIMARY_ZIP=B.PRIMARY_ZIP
AND A.DUP_CHECK>1) C
WHERE C.CUSTOMER_MDM_KEY=D.CUSTOMER_MDM_KEY;

--STEP1: bridge table updated
--STEP1: deleting records from customer dim that no longer have any matches 

DELETE FROM ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL
WHERE CUSTOMER_MDM_KEY NOT IN (SELECT CUSTOMER_MDM_KEY FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL);

--STEP1: deleting duplicates from  from MDM_CUSTOMER_BRIDGE

--STEP1: finished deleting previous duplicates --case statement change frome membership as above and logci cahnge for address fields
--STEP2:  is to update based on the name and address
UPDATE ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL A
SET
A.FIRST_NM=CASE WHEN A.FIRST_NM IS NULL THEN B.FIRST_NM ELSE A.FIRST_NM END, 
A.LAST_NM= CASE WHEN A.LAST_NM IS NULL THEN B.LAST_NM ELSE A.LAST_NM END,
A.PRIMARY_ADDRESS_LINE1=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_ADDRESS_LINE1 ELSE A.MEM_RESIDENTIAL_ADDRESS_LINE1 END, 
A.PRIMARY_ADDRESS_LINE2=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_ADDRESS_LINE2 ELSE A.MEM_RESIDENTIAL_ADDRESS_LINE2 END, 
A.PRIMARY_CITY=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_CITY ELSE A.MEM_RESIDENTIAL_CITY END,
A.PRIMARY_STATE=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_STATE ELSE A.MEM_RESIDENTIAL_STATE END,
A.PRIMARY_ZIP=CASE WHEN A.MEM_RESIDENTIAL_ADDRESS_LINE1 IS NULL THEN B.PRIMARY_ZIP ELSE A.MEM_RESIDENTIAL_ZIP END,
A.PRIMARY_EMAIL_ADDRESS=CASE WHEN A.PRIMARY_EMAIL_ADDRESS IS NULL THEN B.PRIMARY_EMAIL_ADDRESS ELSE A.PRIMARY_EMAIL_ADDRESS END,
A.TRAVEL_RESIDENTIAL_ADDRESS_LINE1=RESIDENTIAL_ADDRESS_LINE1,
A.TRAVEL_RESIDENTIAL_ADDRESS_LINE2=RESIDENTIAL_ADDRESS_LINE2,
A.TRAVEL_RESIDENTIAL_CITY=RESIDENTIAL_CITY,
A.TRAVEL_RESIDENTIAL_STATE=RESIDENTIAL_STATE,
A.TRAVEL_RESIDENTIAL_ZIP=RESIDENTIAL_ZIP,
A.TRAVEL_BILLING_ADDRESS_LINE1=BILLING_ADDRESS_LINE1,
A.TRAVEL_BILLING_ADDRESS_LINE2=BILLING_ADDRESS_LINE2,
A.TRAVEL_BILLING_CITY=BILLING_CITY,
A.TRAVEL_BILLING_STATE=BILLING_STATE,
A.TRAVEL_BILLING_ZIP=BILLING_ZIP,
A.TRAVEL_OTHER_ADDRESS_LINE1=OTHER_ADDRESS_LINE1,
A.TRAVEL_OTHER_ADDRESS_LINE2=OTHER_ADDRESS_LINE2,
A.TRAVEL_OTHER_CITY=OTHER_CITY,
A.TRAVEL_OTHER_STATE=OTHER_STATE,
A.TRAVEL_OTHER_ZIP=OTHER_ZIP,	
A.TRAVEL_RESIDENTIAL_PHONE_NUM = B.HOME_TELEPHONE_NUM,
A.TRAVEL_WORK_PHONE_NUM = B.WORK_PHONE_NUM,    
A.TRAVEL_CREATE_DATE = B.SOURCE_CREATE_DATE  
FROM (
SELECT 
CUSTOMER_MDM_KEY,
C.FIRST_NM,
C.LAST_NM,
C.PRIMARY_ADDRESS_LINE1,
C.PRIMARY_ADDRESS_LINE2,
C.PRIMARY_CITY,
C.PRIMARY_STATE,
C.PRIMARY_ZIP,
PRIMARY_EMAIL_ADDRESS,
RESIDENTIAL_ADDRESS_LINE1,
RESIDENTIAL_ADDRESS_LINE2,
RESIDENTIAL_CITY,
RESIDENTIAL_STATE,
RESIDENTIAL_ZIP,
BILLING_ADDRESS_LINE1,
BILLING_ADDRESS_LINE2,
BILLING_CITY,
BILLING_STATE,
BILLING_ZIP,
OTHER_ADDRESS_LINE1,
OTHER_ADDRESS_LINE2,
OTHER_CITY,
OTHER_STATE,
OTHER_ZIP,
OTHER_EMAIL_ADDRESS,
HOME_TELEPHONE_NUM,
WORK_PHONE_NUM,
OTHER_PHONE_NUM,
SOURCE_CREATE_DATE,
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_MDM_KEY ORDER BY SOURCE_CREATE_DATE DESC) AS DUP_CHECK
FROM 
(
SELECT 
FIRST_NM,
LAST_NM,
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP,
PRIMARY_EMAIL_ADDRESS,
RESIDENTIAL_ADDRESS_LINE1,
RESIDENTIAL_ADDRESS_LINE2,
RESIDENTIAL_CITY,
RESIDENTIAL_STATE,
RESIDENTIAL_ZIP,
BILLING_ADDRESS_LINE1,
BILLING_ADDRESS_LINE2,
BILLING_CITY,
BILLING_STATE,
BILLING_ZIP,
OTHER_ADDRESS_LINE1,
OTHER_ADDRESS_LINE2,
OTHER_CITY,
OTHER_STATE,
OTHER_ZIP,
OTHER_EMAIL_ADDRESS,
HOME_TELEPHONE_NUM,
WORK_PHONE_NUM,
OTHER_PHONE_NUM,
SOURCE_CREATE_DATE
FROM
LANDING.WORK_MDM_TRAVEL) C
INNER JOIN 
(SELECT
CUSTOMER_MDM_KEY,
FIRST_NM,
LAST_NM,
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP,
ROW_NUMBER() OVER (PARTITION BY FIRST_NM, LAST_NM,PRIMARY_ADDRESS_LINE1,PRIMARY_ADDRESS_LINE2, PRIMARY_CITY, PRIMARY_STATE ,PRIMARY_ZIP ORDER BY TRAVEL_CREATE_DATE DESC) AS DUP_CHECK
FROM ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL
) D
ON C.FIRST_NM=D.FIRST_NM
AND COALESCE(C.LAST_NM,'1')=COALESCE(D.LAST_NM,'1')
AND C.PRIMARY_ADDRESS_LINE1=D.PRIMARY_ADDRESS_LINE1 
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'1')=COALESCE(D.PRIMARY_ADDRESS_LINE2,'1')
AND C.PRIMARY_CITY=D.PRIMARY_CITY 
AND C.PRIMARY_STATE=D.PRIMARY_STATE
AND C.PRIMARY_ZIP = D.PRIMARY_ZIP
) B
WHERE A.CUSTOMER_MDM_KEY=B.CUSTOMER_MDM_KEY AND DUP_CHECK=1;

--STEP3: customers linked by the name, and address
--STEP3: inserting records into bridge table for records linked by name, and address
--add distinct or group by for duplicates

INSERT INTO ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL(
SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
BRIDGE_SEQ_ID)
SELECT 
SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
RECORD_NUMBER + (SELECT CASE WHEN MAX(BRIDGE_SEQ_ID) IS NULL THEN 0 ELSE MAX(BRIDGE_SEQ_ID) END FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL) AS BRIDGE_SEQ_ID
FROM(SELECT 
SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
ROW_NUMBER() OVER() RECORD_NUMBER
FROM (
SELECT 
DISTINCT
SOURCE_SYSTEM AS SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1 AS SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME AS SOURCE_KEY1_COLUMN_NAME
from
(SELECT
CUSTOMER_MDM_KEY,
FIRST_NM,
LAST_NM,
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP,
ROW_NUMBER() OVER (PARTITION BY FIRST_NM, LAST_NM, PRIMARY_ADDRESS_LINE1,PRIMARY_ADDRESS_LINE2, PRIMARY_CITY, PRIMARY_STATE,PRIMARY_ZIP ORDER BY TRAVEL_CREATE_DATE DESC) AS DUP_CHECK
FROM ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL
) C
INNER JOIN 
(SELECT
SOURCE_SYSTEM,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
FIRST_NM,
LAST_NM, 
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP
from LANDING.WORK_MDM_TRAVEL
) D
ON C.FIRST_NM=D.FIRST_NM
AND COALESCE(C.LAST_NM,'1')=COALESCE(D.LAST_NM,'1')
AND C.PRIMARY_ADDRESS_LINE1=D.PRIMARY_ADDRESS_LINE1
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'1')=COALESCE(D.PRIMARY_ADDRESS_LINE2,'1') 
AND C.PRIMARY_CITY=D.PRIMARY_CITY 
AND C.PRIMARY_STATE=D.PRIMARY_STATE
AND C.PRIMARY_ZIP=D.PRIMARY_ZIP
AND C.DUP_CHECK=1));

--DELETE FROM WORK TABLE
DELETE FROM `LANDING.WORK_MDM_TRAVEL` A
WHERE EXISTS
(SELECT 1 FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL B WHERE A.SOURCE_KEY1=B.SOURCE_KEY1 
AND A.SOURCE_SYSTEM=B.SOURCE_SYSTEM);

-- STEP2: previously linked records are deleted
-- STEP2: record count in LANDING.WORK_MDM_TRAVEL is 
-- STEP3: cleaning up records that have been updated to have the same nk
-- STEP3: updating the bridge table 

UPDATE ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL D
SET D.CUSTOMER_MDM_KEY=C.PRIMARY_CUSTOMER_MDM_KEY
FROM
(SELECT 
B.PRIMARY_CUSTOMER_MDM_KEY,
A.CUSTOMER_MDM_KEY
FROM
(
SELECT
CUSTOMER_MDM_KEY,
FIRST_NM, 
LAST_NM,  
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP,
ROW_NUMBER() OVER (PARTITION BY FIRST_NM, LAST_NM, PRIMARY_ADDRESS_LINE1,PRIMARY_ADDRESS_LINE2,PRIMARY_ADDRESS_LINE2, PRIMARY_CITY, PRIMARY_STATE,PRIMARY_ZIP ORDER BY CUSTOMER_MDM_KEY ASC) AS DUP_CHECK
from ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL 
)A 
INNER JOIN
(SELECT 
MIN(CUSTOMER_MDM_KEY) AS PRIMARY_CUSTOMER_MDM_KEY,
FIRST_NM, 
LAST_NM, 
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP
from ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL
GROUP BY 
FIRST_NM, 
LAST_NM,  
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP)
B
ON A.FIRST_NM=B.FIRST_NM 
AND COALESCE(A.LAST_NM,'1')=COALESCE(B.LAST_NM,'1')
AND A.PRIMARY_ADDRESS_LINE1=B.PRIMARY_ADDRESS_LINE1 
AND COALESCE(A.PRIMARY_ADDRESS_LINE2,'1')=COALESCE(B.PRIMARY_ADDRESS_LINE2,'1') 
AND A.PRIMARY_CITY=B.PRIMARY_CITY 
AND A.PRIMARY_STATE=B.PRIMARY_STATE
AND A.PRIMARY_ZIP=B.PRIMARY_ZIP
AND A.DUP_CHECK>1) C
WHERE C.CUSTOMER_MDM_KEY=D.CUSTOMER_MDM_KEY;

-- STEP3: bridge table updated
-- STEP3: deleting records from customer dim that no longer have any matches 

DELETE FROM ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL
WHERE CUSTOMER_MDM_KEY NOT IN (SELECT CUSTOMER_MDM_KEY FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL);


-- STEP5: Inserting new records into the DIM table

INSERT INTO ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL(
CUSTOMER_MDM_KEY,
FIRST_NM, 
LAST_NM, 
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2, 
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP,
TRAVEL_RESIDENTIAL_ADDRESS_LINE1,
TRAVEL_RESIDENTIAL_ADDRESS_LINE2,
TRAVEL_RESIDENTIAL_CITY,
TRAVEL_RESIDENTIAL_STATE,
TRAVEL_RESIDENTIAL_ZIP,
TRAVEL_BILLING_ADDRESS_LINE1,
TRAVEL_BILLING_ADDRESS_LINE2,
TRAVEL_BILLING_CITY,
TRAVEL_BILLING_STATE,
TRAVEL_BILLING_ZIP,
TRAVEL_OTHER_ADDRESS_LINE1,
TRAVEL_OTHER_ADDRESS_LINE2,
TRAVEL_OTHER_CITY,
TRAVEL_OTHER_STATE,
TRAVEL_OTHER_ZIP,
TRAVEL_EMAIL_ADDRESS,
TRAVEL_RESIDENTIAL_PHONE_NUM,
TRAVEL_WORK_PHONE_NUM,
TRAVEL_CREATE_DATE)
SELECT 
RECORD_NUMBER + (SELECT CASE WHEN MAX(CUSTOMER_MDM_KEY) IS NULL THEN 0 ELSE MAX(CUSTOMER_MDM_KEY) END FROM CUSTOMERS.MDM_CUSTOMER_DIM) AS CUSTOMER_MDM_KEY,
FIRST_NM, 
LAST_NM, 
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2, 
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP,
TRAVEL_RESIDENTIAL_ADDRESS_LINE1,
TRAVEL_RESIDENTIAL_ADDRESS_LINE2,
TRAVEL_RESIDENTIAL_CITY,
TRAVEL_RESIDENTIAL_STATE,
TRAVEL_RESIDENTIAL_ZIP,
TRAVEL_BILLING_ADDRESS_LINE1,
TRAVEL_BILLING_ADDRESS_LINE2,
TRAVEL_BILLING_CITY,
TRAVEL_BILLING_STATE,
TRAVEL_BILLING_ZIP,
TRAVEL_OTHER_ADDRESS_LINE1,
TRAVEL_OTHER_ADDRESS_LINE2,
TRAVEL_OTHER_CITY,
TRAVEL_OTHER_STATE,
TRAVEL_OTHER_ZIP,
TRAVEL_EMAIL_ADDRESS,
TRAVEL_RESIDENTIAL_PHONE_NUM,
TRAVEL_WORK_PHONE_NUM,
TRAVEL_CREATE_DATE
FROM 
(
SELECT 
ROW_NUMBER() OVER() RECORD_NUMBER,
FIRST_NM, 
LAST_NM, 
BIRTH_DATE,
PRIMARY_ADDRESS_LINE1, 
PRIMARY_ADDRESS_LINE2, 
PRIMARY_CITY, 
PRIMARY_STATE,
PRIMARY_ZIP,
TRAVEL_RESIDENTIAL_ADDRESS_LINE1,
TRAVEL_RESIDENTIAL_ADDRESS_LINE2,
TRAVEL_RESIDENTIAL_CITY,
TRAVEL_RESIDENTIAL_STATE,
TRAVEL_RESIDENTIAL_ZIP,
TRAVEL_BILLING_ADDRESS_LINE1,
TRAVEL_BILLING_ADDRESS_LINE2,
TRAVEL_BILLING_CITY,
TRAVEL_BILLING_STATE,
TRAVEL_BILLING_ZIP,
TRAVEL_OTHER_ADDRESS_LINE1,
TRAVEL_OTHER_ADDRESS_LINE2,
TRAVEL_OTHER_CITY,
TRAVEL_OTHER_STATE,
TRAVEL_OTHER_ZIP,
TRAVEL_EMAIL_ADDRESS,
TRAVEL_RESIDENTIAL_PHONE_NUM,
TRAVEL_WORK_PHONE_NUM,
TRAVEL_OTHER_PHONE_NUM,
TRAVEL_CREATE_DATE
FROM(
SELECT 
SOURCE_KEY1,
SOURCE_KEY2,
SOURCE_SYSTEM AS SOURCE_SYSTEM,
FIRST_NM, 
LAST_NM, 
BIRTH_DATE,
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP,
RESIDENTIAL_ADDRESS_LINE1 AS TRAVEL_RESIDENTIAL_ADDRESS_LINE1,
RESIDENTIAL_ADDRESS_LINE2 AS TRAVEL_RESIDENTIAL_ADDRESS_LINE2,
RESIDENTIAL_CITY AS TRAVEL_RESIDENTIAL_CITY,
RESIDENTIAL_STATE AS TRAVEL_RESIDENTIAL_STATE,
RESIDENTIAL_ZIP AS TRAVEL_RESIDENTIAL_ZIP,
BILLING_ADDRESS_LINE1 AS TRAVEL_BILLING_ADDRESS_LINE1,
BILLING_ADDRESS_LINE2 AS TRAVEL_BILLING_ADDRESS_LINE2,
BILLING_CITY AS TRAVEL_BILLING_CITY,
BILLING_STATE AS TRAVEL_BILLING_STATE,
BILLING_ZIP AS TRAVEL_BILLING_ZIP,
OTHER_ADDRESS_LINE1 AS TRAVEL_OTHER_ADDRESS_LINE1,
OTHER_ADDRESS_LINE2 AS TRAVEL_OTHER_ADDRESS_LINE2,
OTHER_CITY AS TRAVEL_OTHER_CITY,
OTHER_STATE AS TRAVEL_OTHER_STATE,
OTHER_ZIP AS TRAVEL_OTHER_ZIP,
OTHER_EMAIL_ADDRESS AS TRAVEL_EMAIL_ADDRESS,
HOME_TELEPHONE_NUM AS TRAVEL_RESIDENTIAL_PHONE_NUM,
WORK_PHONE_NUM AS TRAVEL_WORK_PHONE_NUM,
OTHER_PHONE_NUM AS TRAVEL_OTHER_PHONE_NUM,
SOURCE_CREATE_DATE AS TRAVEL_CREATE_DATE,
ROW_NUMBER() OVER (PARTITION BY FIRST_NM, LAST_NM,PRIMARY_ADDRESS_LINE1,PRIMARY_ADDRESS_LINE2,PRIMARY_CITY, PRIMARY_STATE,PRIMARY_ZIP ORDER BY HOME_TELEPHONE_NUM,
SOURCE_CREATE_DATE DESC) AS DUP_CHECK
FROM 
LANDING.WORK_MDM_TRAVEL C
WHERE NOT EXISTS(SELECT 1 FROM ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL D
WHERE C.FIRST_NM=D.FIRST_NM
AND COALESCE(C.LAST_NM,'1')=COALESCE(D.LAST_NM,'1')
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'1')=COALESCE(D.PRIMARY_ADDRESS_LINE1,'1') 
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'1')=COALESCE(D.PRIMARY_ADDRESS_LINE2,'1') 
AND COALESCE(C.PRIMARY_CITY,'1')=COALESCE(D.PRIMARY_CITY,'1')
AND COALESCE(C.PRIMARY_STATE,'1')=COALESCE(D.PRIMARY_STATE,'1')
AND COALESCE(C.PRIMARY_ZIP,'1')=COALESCE(D.PRIMARY_ZIP,'1')
)
) A
WHERE A.DUP_CHECK=1
);


DELETE FROM  ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL WHERE FIRST_NM IS NULL;
-- STEP5: finished load of TRAVEL records into the customer dim
-- STEP5: starting load of new TRAVEL records into the customer bridge

INSERT INTO ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL(
SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
BRIDGE_SEQ_ID)
SELECT 
SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
RECORD_NUMBER + (SELECT CASE WHEN MAX(BRIDGE_SEQ_ID) IS NULL THEN 0 ELSE MAX(BRIDGE_SEQ_ID) END FROM CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE) AS BRIDGE_SEQ_ID
FROM(SELECT 
SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
ROW_NUMBER() OVER() RECORD_NUMBER
FROM (
SELECT 
DISTINCT
SOURCE_SYSTEM AS SOURCE_SYSTEM,
CUSTOMER_MDM_KEY,
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME
from
(SELECT
CUSTOMER_MDM_KEY,
FIRST_NM,
LAST_NM,
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP
FROM ARCHIVE.MDM_CUSTOMER_DIM_TRAVEL) C
INNER JOIN 
(
SELECT 
SOURCE_KEY1,
SOURCE_KEY1_COLUMN_NAME,
SOURCE_SYSTEM,
FIRST_NM, 
LAST_NM, 
PRIMARY_ADDRESS_LINE1,
PRIMARY_ADDRESS_LINE2,
PRIMARY_CITY,
PRIMARY_STATE,
PRIMARY_ZIP
FROM 
LANDING.WORK_MDM_TRAVEL
) D
ON  C.FIRST_NM =D.FIRST_NM 
AND COALESCE(C.LAST_NM,'1')=COALESCE(D.LAST_NM,'1')
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'1')=COALESCE(D.PRIMARY_ADDRESS_LINE1,'1') 
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'1')=COALESCE(D.PRIMARY_ADDRESS_LINE2,'1') 
AND COALESCE(C.PRIMARY_CITY,'1')=COALESCE(D.PRIMARY_CITY,'1')
AND COALESCE(C.PRIMARY_STATE,'1')=COALESCE(D.PRIMARY_STATE,'1')
AND COALESCE(C.PRIMARY_ZIP,'1')=COALESCE(D.PRIMARY_ZIP,'1')
));

--Deleting duplicates from bridge table
DELETE FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL C WHERE EXISTS(SELECT 1 FROM
(SELECT * FROM (
SELECT A.*,ROW_NUMBER() OVER(PARTITION BY A.CUSTOMER_MDM_KEY,A.SOURCE_KEY1,A.SOURCE_SYSTEM) AS RNK FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL A,
(SELECT CUSTOMER_MDM_KEY,SOURCE_KEY1,SOURCE_SYSTEM,COUNT(*)
FROM ARCHIVE.MDM_CUSTOMER_BRIDGE_TRAVEL GROUP BY 
CUSTOMER_MDM_KEY,SOURCE_KEY1,SOURCE_SYSTEM HAVING COUNT(*) >1) TBL
WHERE A.CUSTOMER_MDM_KEY=TBL.CUSTOMER_MDM_KEY AND A.SOURCE_KEY1=TBL.SOURCE_KEY1 AND
A.SOURCE_SYSTEM=TBL.SOURCE_SYSTEM
) WHERE RNK=1) D WHERE C.CUSTOMER_MDM_KEY=D.CUSTOMER_MDM_KEY AND C.SOURCE_KEY1=D.SOURCE_KEY1 AND
C.SOURCE_SYSTEM=D.SOURCE_SYSTEM
AND C.BRIDGE_SEQ_ID=D.BRIDGE_SEQ_ID)
-- STEP5: finished with load of TRAVEL data

