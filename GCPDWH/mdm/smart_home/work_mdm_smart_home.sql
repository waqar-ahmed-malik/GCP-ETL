----------------------------------------------------------------------
--Overall flow of these script is
--Step1     Prepare the SAFE Customer data for the MDM process
--Step2     Update the MERGE_FLAG column using the MERGE_CHECK1_CTL and MERGE_CHECK2_CTL.
--Step2.1   If MERGE_CHECK1_CTL=0 and MERGE_CHECK2_CTL=0 Then Dont Consider those records for MDM Process. All Required Column are having Null Values.
--Step2.2   If MERGE_CHECK1_CTL=1 and MERGE_CHECK2_CTL=1 Then treat that records as Golden record specific to Source System. Adress and Email Id are Unique. It is not merged to any transaction in given source system data.
--Step2.3.0 If MERGE_CHECK1_CTL+MERGE_CHECK2_CTL>=2 Then Identify the Golden and Merge Records from data set. 
--Step2.3.1 To Take the Latest transaction and It is based on Source record creation date and Source record Id (Example: Customer_ID). 
--Step2.3.2 Identify the First record in the Group as "Golden" and the remaining transaction in that group as "Merge".
--Step2.4   If MERGE_CHECK1_CTL=1 and MERGE_CHECK2_CTL=0 Then consider all these transaction as "Golden". It is due to the transaction are unique and not merged with any other records in that group. The Primary Email Address is Null for all these transactions.
--Step2.5   If MERGE_CHECK1_CTL=0 and MERGE_CHECK2_CTL=1 Then consider all these transaction as "Golden". It is due to the transaction are unique and not merged with any other records in that group. The Primary Address Line1 is Null for all these transactions. 
--Step2.6   Delete the records which are not required for MDM Process.[It Could be Source Data Issue and they are not providing all required information for MDM process or Report these transaction as an Issue]
----------------------------------------------------------------------
--STEP1
----------------------------------------------------------------------

CREATE OR REPLACE TABLE LANDING.WORK_MDM_SMART_HOME AS 
(SELECT DISTINCT 
CAST(SOURCE_KEY1 AS STRING) as SOURCE_KEY1,
CAST(SOURCE_KEY2 AS STRING) as SOURCE_KEY2,
CAST(SOURCE_KEY3 AS STRING) as SOURCE_KEY3,
SOURCE_KEY1_DESC,
SOURCE_KEY2_DESC,	
SOURCE_KEY3_DESC,
SOURCE_SYSTEM_CD,
TRIM(FIRST_NM) AS FIRST_NM,
TRIM(LAST_NM) AS LAST_NM,
TRIM(MIDDLE_NM) AS MIDDLE_NM,
NAME_SUFFIX_CD,
SALUTATION_CD,
BIRTH_DT AS BIRTH_DT,
GENDER_CD,
--MARITAL_STATUS,
UPPER(TRIM(PRIMARY_ADDRESS_LINE1)) AS PRIMARY_ADDRESS_LINE1,
UPPER(TRIM(PRIMARY_ADDRESS_LINE2)) AS PRIMARY_ADDRESS_LINE2,
UPPER(TRIM(PRIMARY_CITY)) AS PRIMARY_CITY,
UPPER(TRIM(PRIMARY_STATE)) AS PRIMARY_STATE,
UPPER(TRIM(PRIMARY_ZIP)) AS PRIMARY_ZIP,
UPPER(TRIM(PRIMARY_EMAIL_ADDRESS)) AS PRIMARY_EMAIL_ADDRESS,
COALESCE(HOME_TELEPHONE_NUM, WORK_PHONE_NUM) AS PRIMARY_PHONE_NUM,
UPPER(TRIM(RESIDENTIAL_ADDRESS_LINE1)) AS RESIDENTIAL_ADDRESS_LINE1,
UPPER(TRIM(RESIDENTIAL_ADDRESS_LINE2)) AS RESIDENTIAL_ADDRESS_LINE2,
UPPER(TRIM(RESIDENTIAL_CITY)) AS RESIDENTIAL_CITY,
UPPER(TRIM(RESIDENTIAL_STATE)) AS RESIDENTIAL_STATE,
UPPER(TRIM(RESIDENTIAL_ZIP)) AS RESIDENTIAL_ZIP,
UPPER(TRIM(OTHER_EMAIL_ADDRESS)) AS OTHER_EMAIL_ADDRESS,
HOME_TELEPHONE_NUM ,
WORK_PHONE_NUM ,
OTHER_PHONE_NUM ,
COUNT(CASE
      WHEN (TRIM(PRIMARY_ADDRESS_LINE1) IS NULL OR TRIM(PRIMARY_ADDRESS_LINE1)='') THEN NULL
      ELSE 1 END) OVER(PARTITION BY  UPPER(TRIM(FIRST_NM)), UPPER(TRIM(LAST_NM)),BIRTH_DT,
    UPPER(TRIM(PRIMARY_ADDRESS_LINE1)),
    UPPER(TRIM(PRIMARY_ADDRESS_LINE2)),
    UPPER(TRIM(PRIMARY_CITY)),
    UPPER(TRIM(PRIMARY_STATE)),
    UPPER(TRIM(PRIMARY_ZIP))) AS MERGE_CHECK1_CTL,
  COUNT(CASE
      WHEN (TRIM(PRIMARY_EMAIL_ADDRESS) IS NULL OR TRIM(PRIMARY_EMAIL_ADDRESS)='') THEN NULL
      ELSE 1 END) OVER(PARTITION BY UPPER(FIRST_NM),
    UPPER(LAST_NM),BIRTH_DT,
    UPPER(PRIMARY_EMAIL_ADDRESS)) AS MERGE_CHECK2_CTL,
'      ' as MERGE_FLAG,
CAST(NULL AS STRING) as ADDRESS_MATCH_SOURCE_KEY,  -- RENAMED
CAST(NULL AS INT64) AS CUSTOMER_MDM_KEY,
CAST(NULL AS STRING) AS WORK_MATCH_GROUP,
CAST(NULL AS STRING) AS MDM_MATCH_GROUP,
CAST(NULL AS BYTES) AS HOME_MD5_KEY,
CAST(NULL AS BYTES) AS MAILING_MD5_KEY,
CAST(NULL AS BYTES) AS OTHER_MD5_KEY,
CAST(NULL AS STRING) AS EMAIL_MATCH_SOURCE_KEY, -- New Column
CAST(NULL AS STRING) AS ADDRESS_MATCH_DESC, -- New Column
CAST(NULL AS STRING) AS EMAIL_MATCH_DESC, -- New Column
CAST(NULL AS STRING) AS GOLDEN_MATCH_KEY, -- New Column
SOURCE_CREATE_DATE,
REGEXP_REPLACE(CONCAT( IFNULL(UPPER(TRIM(RESIDENTIAL_ADDRESS_LINE1)),''),
                          IFNULL(UPPER(TRIM(RESIDENTIAL_ADDRESS_LINE2)),''), 
						  IFNULL(UPPER(TRIM(RESIDENTIAL_CITY)),''), 
						  IFNULL(UPPER(TRIM(RESIDENTIAL_STATE)),''), 
						  IFNULL(UPPER(TRIM(RESIDENTIAL_ZIP)),'')),
				  '[^a-zA-Z0-9]','') AS ADDRESS_MATCH_KEY1, -- New Column
    REGEXP_REPLACE(CONCAT( IFNULL(UPPER(TRIM(BILLING_ADDRESS_LINE1)),''),
                          IFNULL(UPPER(TRIM(BILLING_ADDRESS_LINE2)),''), 
						  IFNULL(UPPER(TRIM(BILLING_CITY)),''), 
						  IFNULL(UPPER(TRIM(BILLING_STATE)),''), 
						  IFNULL(UPPER(TRIM(BILLING_ZIP)),'')),
				  '[^a-zA-Z0-9]','') AS ADDRESS_MATCH_KEY2  -- New Column
FROM 
(SELECT
  DISTINCT
  TRIM( SAFE_CUSTOMER_NUM) AS SOURCE_KEY1,
 CAST(NULL AS STRING) AS SOURCE_KEY2,
  CAST(NULL AS STRING) AS SOURCE_KEY3,
  'SAFE_CUSTOMER_NUM' AS SOURCE_KEY1_DESC,
  'MEMBER_NUM' AS SOURCE_KEY2_DESC,	
  CAST(NULL AS STRING) AS SOURCE_KEY3_DESC,
  'SMART_HOME' AS SOURCE_SYSTEM_CD,     
  UPPER(TRIM(FIRST_NM)) AS FIRST_NM ,  
  UPPER(TRIM(LAST_NM )) AS LAST_NM,
  CAST(NULL AS STRING) AS MIDDLE_NM,  
  CAST(NULL AS STRING) AS NAME_SUFFIX_CD,
  CAST(NULL AS STRING) AS SALUTATION_CD,
  SAFE_CAST( BIRTH_DT as DATE) AS BIRTH_DT,
  TRIM(GENDER_CD) AS GENDER_CD,
  --TRIM(COMBINED_MARITAL_STATUS) AS MARITAL_STATUS,
  CASE WHEN ADDRESS_LINE1 ='' THEN NULL ELSE UPPER(TRIM( ADDRESS_LINE1 )) END AS PRIMARY_ADDRESS_LINE1,
  CASE WHEN ADDRESS_LINE2 ='' THEN NULL ELSE UPPER(TRIM( ADDRESS_LINE2 )) END AS PRIMARY_ADDRESS_LINE2,
  CASE WHEN CITY ='' THEN NULL ELSE UPPER(TRIM(CITY )) END AS PRIMARY_CITY,
  CASE WHEN STATE ='' THEN NULL ELSE UPPER(TRIM( STATE )) END AS PRIMARY_STATE,
   CASE WHEN ZIP_CD='' THEN NULL 
    WHEN LENGTH(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]','')) = 6 THEN REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]','')
    WHEN LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]',''),'[^0-9]','')) < 5 
    THEN LPAD(REGEXP_REPLACE(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),5,'0')
  ELSE SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),0,5)
END AS PRIMARY_ZIP,
   TRIM(EMAIL_ADDRESS) AS PRIMARY_EMAIL_ADDRESS,  
  CASE WHEN PHONE_NUM='' THEN NULL ELSE   CASE
    WHEN LENGTH(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]','')) <= 10 THEN REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]','')
    WHEN SUBSTR(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]',''),1,1) = '1' THEN SUBSTR(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]',''),2,11)
  ELSE SUBSTR(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]',''),1,10) END END AS PRIMARY_PHONE_NUM ,
  CASE WHEN ADDRESS_LINE1 ='' THEN NULL ELSE TRIM(ADDRESS_LINE1) END AS RESIDENTIAL_ADDRESS_LINE1,
  CASE WHEN ADDRESS_LINE2='' THEN NULL ELSE TRIM(ADDRESS_LINE2) END AS RESIDENTIAL_ADDRESS_LINE2,
  TRIM(CITY) AS RESIDENTIAL_CITY,
  TRIM(STATE) AS RESIDENTIAL_STATE,
  CASE WHEN ZIP_CD='' THEN NULL 
    WHEN LENGTH(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]','')) = 6 THEN REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]','')
    WHEN LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]',''),'[^0-9]','')) < 5 
    THEN LPAD(REGEXP_REPLACE(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),5,'0')
  ELSE SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(ZIP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),0,5)
END AS RESIDENTIAL_ZIP,
  CAST(NULL AS STRING) AS BILLING_ADDRESS_LINE1,
  CAST(NULL AS STRING) AS BILLING_ADDRESS_LINE2,
  CAST(NULL AS STRING) AS BILLING_CITY,
  CAST(NULL AS STRING) AS BILLING_STATE,
  CAST(NULL AS STRING) AS BILLING_ZIP,
  CAST(NULL AS STRING) AS OTHER_ADDRESS_LINE1,
  CAST(NULL AS STRING) AS OTHER_ADDRESS_LINE2,
  CAST(NULL AS STRING) AS OTHER_CITY,
  CAST(NULL AS STRING) AS OTHER_STATE,
  CAST(NULL AS STRING) AS OTHER_ZIP,
  CAST(NULL AS STRING) AS OTHER_EMAIL_ADDRESS,
  CASE WHEN PHONE_NUM='' THEN NULL ELSE   CASE
    WHEN LENGTH(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]','')) <= 10 THEN REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]','')
    WHEN SUBSTR(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]',''),1,1) = '1' THEN SUBSTR(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]',''),2,11)
  ELSE SUBSTR(REGEXP_REPLACE(PHONE_NUM,'[^a-zA-Z0-9]',''),1,10) END END  AS HOME_TELEPHONE_NUM ,
  CAST(NULL AS STRING) AS WORK_PHONE_NUM,
  CAST(NULL AS STRING) AS OTHER_PHONE_NUM,
  CURRENT_DATE() AS SOURCE_CREATE_DATE
 FROM LANDING.WORK_SMART_HOME_CUSTOMER));


----------------------------------------------------------------------
--STEP 1.2
---------------------------------------------------------------------- 

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
    WORK_PHONE_NUM,
    OTHER_PHONE_NUM,
    SOURCE_CREATE_DATE,
    CREATE_DTTIME
    )
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
    WORK_PHONE_NUM,
    OTHER_PHONE_NUM,
    SOURCE_CREATE_DATE,
    CURRENT_DATETIME() AS CREATE_DTTIME
FROM
  LANDING.WORK_MDM_SMART_HOME
WHERE
  REGEXP_CONTAINS(PRIMARY_CITY, '[0-9]')
  OR LENGTH(PRIMARY_ZIP) < 5
  OR LENGTH(PRIMARY_ZIP) >10
  OR REGEXP_CONTAINS(FIRST_NM, '[0-9]')
  OR REGEXP_CONTAINS(LAST_NM, '[0-9]')
  OR FIRST_NM IS NULL
  OR LAST_NM IS NULL
  OR PRIMARY_ADDRESS_LINE1 IS NULL
  OR TRIM(PRIMARY_ADDRESS_LINE1)='';
----------------------------------------------------------------------
--STEP 1.3
----------------------------------------------------------------------  
DELETE
FROM
  LANDING.WORK_MDM_SMART_HOME
WHERE
  REGEXP_CONTAINS(PRIMARY_CITY, '[0-9]')
  OR LENGTH(PRIMARY_ZIP) < 5
  OR LENGTH(PRIMARY_ZIP) >10
  OR REGEXP_CONTAINS(FIRST_NM, '[0-9]')
  OR REGEXP_CONTAINS(LAST_NM, '[0-9]')
  OR FIRST_NM IS NULL
  OR LAST_NM IS NULL
  OR PRIMARY_ADDRESS_LINE1 IS NULL
  OR TRIM(PRIMARY_ADDRESS_LINE1)='';

----------------------------------------------------------------------
--STEP2.1
----------------------------------------------------------------------
update  LANDING.WORK_MDM_SMART_HOME set MERGE_FLAG='Ignore'
where MERGE_FLAG ='      ' and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)=0
and not (HOME_TELEPHONE_NUM is not null);
----------------------------------------------------------------------
--STEP2.2
----------------------------------------------------------------------
update  LANDING.WORK_MDM_SMART_HOME set MERGE_FLAG='Golden',GOLDEN_MATCH_KEY=SOURCE_KEY1
where MERGE_FLAG ='      ' 
AND MERGE_CHECK2_CTL in (0,1) and MERGE_CHECK1_CTL in (0,1);

----------------------------------------------------------------------
--STEP2.3 -- This Update statement to identify the group where FIRST_NAME,LAST_NAME and BIRTH_DT are same.
----------------------------------------------------------------------
UPDATE LANDING.WORK_MDM_SMART_HOME D
SET D.WORK_MATCH_GROUP=C.MATCH_SOURCE_KEY
FROM (select SOURCE_KEY1,
FIRST_VALUE(SOURCE_KEY1) OVER (PARTITION BY UPPER(FIRST_NM),UPPER(LAST_NM),
BIRTH_DT ORDER BY SOURCE_CREATE_DATE DESC,SOURCE_KEY1 DESC) MATCH_SOURCE_KEY
from LANDING.WORK_MDM_SMART_HOME where MERGE_FLAG ='      '  
and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2) C
WHERE C.SOURCE_KEY1=D.SOURCE_KEY1
and D.MERGE_FLAG ='      ';
----------------------------------------------------------------------
--STEP2.4 -- Below Updates statement to identify the groups where FIRST_NAME,LAST_NAME, BIRTH_DT and ADDRESS are same from STEP2.3.
----------------------------------------------------------------------
UPDATE LANDING.WORK_MDM_SMART_HOME D
SET D.ADDRESS_MATCH_SOURCE_KEY=C.ADDRESS_MATCH_SOURCE_KEY,D.ADDRESS_MATCH_DESC='Merge' -- ADDRESS_MATCH
FROM (select SOURCE_KEY1,
FIRST_VALUE(SOURCE_KEY1) OVER (PARTITION BY WORK_MATCH_GROUP,UPPER(FIRST_NM),UPPER(LAST_NM),
BIRTH_DT,UPPER(PRIMARY_ADDRESS_LINE1),UPPER(PRIMARY_ADDRESS_LINE2),UPPER(PRIMARY_CITY),UPPER(PRIMARY_STATE),UPPER(PRIMARY_ZIP)
ORDER BY SOURCE_CREATE_DATE DESC,SOURCE_KEY1 DESC) ADDRESS_MATCH_SOURCE_KEY
from LANDING.WORK_MDM_SMART_HOME where MERGE_FLAG ='      '  
and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2
) C
WHERE C.SOURCE_KEY1=D.SOURCE_KEY1
and D.MERGE_FLAG ='      ';
-----------
update  LANDING.WORK_MDM_SMART_HOME A set A.ADDRESS_MATCH_DESC ='Golden' -- ADDRESS_MATCH Golden Record
where exists (select 1 from 
(select SOURCE_KEY1,ROW_NUMBER() OVER (PARTITION BY UPPER(FIRST_NM),UPPER(LAST_NM),
BIRTH_DT,UPPER(PRIMARY_ADDRESS_LINE1),UPPER(PRIMARY_ADDRESS_LINE2),UPPER(PRIMARY_CITY),UPPER(PRIMARY_STATE),UPPER(PRIMARY_ZIP)
ORDER BY SOURCE_CREATE_DATE DESC,SOURCE_KEY1 DESC) AS ROW_NUM  from LANDING.WORK_MDM_SMART_HOME where 
MERGE_FLAG ='      '  
and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2) B
where B.SOURCE_KEY1=A.SOURCE_KEY1 and ROW_NUM=1)
and MERGE_FLAG ='      ';

update LANDING.WORK_MDM_SMART_HOME SET EMAIL_MATCH_SOURCE_KEY=ADDRESS_MATCH_SOURCE_KEY, EMAIL_MATCH_DESC=ADDRESS_MATCH_DESC
where MERGE_FLAG ='      ' and MERGE_CHECK2_CTL=0 and MERGE_CHECK1_CTL>=2
and PRIMARY_EMAIL_ADDRESS is null;
 
----------------------------------------------------------------------
--STEP2.5 -- Below Updates statement to identify the groups where FIRST_NAME,LAST_NAME, BIRTH_DT and EMAIL are same from STEP2.3.
----------------------------------------------------------------------
UPDATE LANDING.WORK_MDM_SMART_HOME D
SET D.EMAIL_MATCH_SOURCE_KEY=C.EMAIL_MATCH_SOURCE_KEY,D.EMAIL_MATCH_DESC='Merge' -- EMAIL MATCH
FROM (select SOURCE_KEY1,
FIRST_VALUE(SOURCE_KEY1) OVER (PARTITION BY WORK_MATCH_GROUP,UPPER(FIRST_NM),UPPER(LAST_NM),
BIRTH_DT,UPPER(PRIMARY_EMAIL_ADDRESS)
ORDER BY SOURCE_CREATE_DATE DESC,SOURCE_KEY1 DESC) EMAIL_MATCH_SOURCE_KEY
from LANDING.WORK_MDM_SMART_HOME where MERGE_FLAG ='      '  
and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2
) C
WHERE C.SOURCE_KEY1=D.SOURCE_KEY1
and D.MERGE_FLAG ='      '
and PRIMARY_EMAIL_ADDRESS is not null;

---------------

update  LANDING.WORK_MDM_SMART_HOME A set A.EMAIL_MATCH_DESC ='Golden' -- EMAIL_MATCH Golden Record
where exists (select 1 from 
(select SOURCE_KEY1,ROW_NUMBER() OVER (PARTITION BY UPPER(FIRST_NM),UPPER(LAST_NM),
BIRTH_DT,UPPER(PRIMARY_EMAIL_ADDRESS)
ORDER BY SOURCE_CREATE_DATE DESC,SOURCE_KEY1 DESC) AS ROW_NUM  from LANDING.WORK_MDM_SMART_HOME where 
MERGE_FLAG ='      '  
and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2) B
where B.SOURCE_KEY1=A.SOURCE_KEY1 and ROW_NUM=1)
and MERGE_FLAG ='      '
and PRIMARY_EMAIL_ADDRESS is not null;

----------------------------------------------------------------------
--STEP2.5 -- Below Updates statement to identify the Golden Match Key Based on Address and Email Match Keys Values.
----------------------------------------------------------------------

update  LANDING.WORK_MDM_SMART_HOME A 
SET A.GOLDEN_MATCH_KEY=B.GOLDEN_MATCH_KEY
FROM (select SOURCE_KEY1,ADDRESS_MATCH_SOURCE_KEY,EMAIL_MATCH_SOURCE_KEY as GOLDEN_MATCH_KEY from LANDING.WORK_MDM_SMART_HOME where ADDRESS_MATCH_DESC ='Golden' and EMAIL_MATCH_DESC  in ('Golden','Merge')
and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2) B
where A.SOURCE_KEY1=B.SOURCE_KEY1
and A.MERGE_FLAG ='      ';
---------------
update  LANDING.WORK_MDM_SMART_HOME A 
SET A.GOLDEN_MATCH_KEY=B.GOLDEN_MATCH_KEY
FROM (select M.SOURCE_KEY1,G.GOLDEN_MATCH_KEY from
(select SOURCE_KEY1,ADDRESS_MATCH_SOURCE_KEY,EMAIL_MATCH_SOURCE_KEY as GOLDEN_MATCH_KEY from LANDING.WORK_MDM_SMART_HOME where ADDRESS_MATCH_DESC ='Golden' and EMAIL_MATCH_DESC  in ('Golden','Merge') and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2) G,
(select SOURCE_KEY1,ADDRESS_MATCH_SOURCE_KEY,EMAIL_MATCH_SOURCE_KEY as GOLDEN_MATCH_KEY from LANDING.WORK_MDM_SMART_HOME where ADDRESS_MATCH_DESC ='Merge' and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2) M
WHERE G.ADDRESS_MATCH_SOURCE_KEY=M.ADDRESS_MATCH_SOURCE_KEY
) B
where A.SOURCE_KEY1=B.SOURCE_KEY1
and A.MERGE_FLAG ='      ';

----------------------------------------------------------------------
--STEP2.6 -- Below Updates statement to identify the Golden and Merge Records Based on Golden Match Key Values.
----------------------------------------------------------------------


update  LANDING.WORK_MDM_SMART_HOME A 
SET A.MERGE_FLAG='Golden'
Where ADDRESS_MATCH_DESC ='Golden' and EMAIL_MATCH_DESC  in ('Golden')
and A.MERGE_FLAG ='      ';


update  LANDING.WORK_MDM_SMART_HOME A 
SET A.MERGE_FLAG='Merge'
FROM (select M.SOURCE_KEY1,G.GOLDEN_MATCH_KEY from
(select SOURCE_KEY1,GOLDEN_MATCH_KEY from LANDING.WORK_MDM_SMART_HOME where ADDRESS_MATCH_DESC ='Golden' and EMAIL_MATCH_DESC  in ('Golden')) G,
(select SOURCE_KEY1,GOLDEN_MATCH_KEY from LANDING.WORK_MDM_SMART_HOME where WORK_MATCH_GROUP<>'Golden'  ) M
WHERE G.GOLDEN_MATCH_KEY=M.GOLDEN_MATCH_KEY) B
WHERE A.SOURCE_KEY1=B.SOURCE_KEY1
and A.MERGE_FLAG ='      ';

update LANDING.WORK_MDM_SMART_HOME A  
SET A.MERGE_FLAG='Merge' , A.GOLDEN_MATCH_KEY=A.WORK_MATCH_GROUP
where MERGE_FLAG ='      ';

----------------------------------------------------------------------
--STEP2.7 -- DOB Merge , If Incase any DOB is null
----------------------------------------------------------------------
update  LANDING.WORK_MDM_SMART_HOME A 
SET A.MERGE_FLAG='DOB_Merge',A.GOLDEN_MATCH_KEY=B.GOLDEN_MATCH_KEY
FROM (
select C.SOURCE_KEY1 as SOURCE_KEY1,D.SOURCE_KEY1 as GOLDEN_MATCH_KEY FROM
(select * from LANDING.WORK_MDM_SMART_HOME where BIRTH_DT IS NULL and MERGE_FLAG='Golden' )C
INNER JOIN (select *,
ROW_NUMBER() OVER(PARTITION BY UPPER(FIRST_NM),UPPER(LAST_NM),UPPER(PRIMARY_ADDRESS_LINE1),UPPER(PRIMARY_ADDRESS_LINE2),UPPER(PRIMARY_CITY),
    UPPER(PRIMARY_STATE),UPPER(PRIMARY_ZIP) ORDER BY SOURCE_CREATE_DATE DESC,SOURCE_KEY1 DESC) as ROW_NUM
    FROM LANDING.WORK_MDM_SMART_HOME where MERGE_FLAG='Golden' and BIRTH_DT IS NOT NULL ) D
ON COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'~')=COALESCE(D.PRIMARY_ADDRESS_LINE1,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'~')=COALESCE(D.PRIMARY_ADDRESS_LINE2,'~') 
AND COALESCE(C.PRIMARY_CITY,'~')=COALESCE(D.PRIMARY_CITY,'~') 
AND COALESCE(C.PRIMARY_STATE,'~')=COALESCE(D.PRIMARY_STATE,'~')
AND COALESCE(C.PRIMARY_ZIP,'~')=COALESCE(D.PRIMARY_ZIP,'~')
and D.ROW_NUM=1) B
where A.SOURCE_KEY1=B.SOURCE_KEY1;
----------------------------------------------------------------------
--STEP2.8 DOB Merge
----------------------------------------------------------------------
update  LANDING.WORK_MDM_SMART_HOME A 
SET A.GOLDEN_MATCH_KEY=B.GOLDEN_MATCH_KEY
FROM (select DOB.GOLDEN_MATCH_KEY AS GOLDEN_MATCH_KEY,
M.SOURCE_KEY1 as SOURCE_KEY1 FROM
(select SOURCE_KEY1,GOLDEN_MATCH_KEY from LANDING.WORK_MDM_SMART_HOME  
where MERGE_FLAG='DOB_Merge') DOB,
(select SOURCE_KEY1,GOLDEN_MATCH_KEY from LANDING.WORK_MDM_SMART_HOME 
where MERGE_FLAG='Merge') M
where DOB.SOURCE_KEY1=M.GOLDEN_MATCH_KEY) B
where A.MERGE_FLAG='Merge'
AND A.SOURCE_KEY1=B.SOURCE_KEY1;
----------------------------------------------------------------------
--STEP2.9 DOB Merge
----------------------------------------------------------------------
update LANDING.WORK_MDM_SMART_HOME SET MERGE_FLAG='Merge'
where MERGE_FLAG='DOB_Merge'
