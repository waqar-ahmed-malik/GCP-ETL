CREATE OR REPLACE TABLE LANDING.WORK_MDM_IVANS_IE AS 
SELECT DISTINCT 
CAST(SOURCE_KEY1 AS STRING) as SOURCE_KEY1,
CAST(SOURCE_KEY2 AS STRING) as SOURCE_KEY2,
CONCAT(SOURCE_KEY1, IFNULL(SOURCE_KEY2,'')) as SOURCE_KEY3,
SOURCE_KEY1_DESC,
SOURCE_KEY2_DESC,
TERM_EXPIRATION_DT,	
'FIRST_VALUE_MATCH' AS SOURCE_KEY3_DESC,
SOURCE_SYSTEM_CD,
TRIM(FIRST_NM) AS FIRST_NM,
TRIM(LAST_NM) AS LAST_NM,
TRIM(MIDDLE_NM) AS MIDDLE_NM,
NAME_SUFFIX_CD,
SALUTATION_CD,
BIRTH_DATE AS BIRTH_DT,
GENDER_CD,
MARITAL_STATUS,
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
      ELSE 1 END) OVER(PARTITION BY  UPPER(TRIM(FIRST_NM)), UPPER(TRIM(LAST_NM)),BIRTH_DATE,
    UPPER(TRIM(PRIMARY_ADDRESS_LINE1)),
    UPPER(TRIM(PRIMARY_ADDRESS_LINE2)),
    UPPER(TRIM(PRIMARY_CITY)),
    UPPER(TRIM(PRIMARY_STATE)),
    UPPER(TRIM(PRIMARY_ZIP))) AS MERGE_CHECK1_CTL,
0 AS MERGE_CHECK2_CTL, 
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
FROM (
SELECT
  DISTINCT
  TBL.AGREEMENT_NUM AS SOURCE_KEY1,
  CASE
    WHEN PRODUCT_TYPE='Auto' THEN TBL.DRIVER_SOURCE_ID
    ELSE NULL
  END AS SOURCE_KEY2,
  CAST(NULL AS STRING) AS SOURCE_KEY3,
  'AGREEMENT_NUM' AS SOURCE_KEY1_DESC,
  'DRIVER_SOURCE_ID' AS SOURCE_KEY2_DESC,	
  CAST(NULL AS STRING) AS SOURCE_KEY3_DESC,
  TERM_EXPIRATION_DT,
  CONCAT('IVANS_PAS_', PRODUCT_TYPE ) AS SOURCE_SYSTEM_CD,     
  CASE WHEN 
  PRODUCT_TYPE='Auto' THEN UPPER(TBL.DRVR_FRST_NM)
  ELSE UPPER(B1.FRST_NM) END AS FIRST_NM ,  
  CASE WHEN
  PRODUCT_TYPE='Auto' THEN UPPER(TBL.DRVR_LST_NM)
  ELSE UPPER(B1.LST_NM) END AS LAST_NM,
  CASE WHEN
  PRODUCT_TYPE='Auto' THEN UPPER(TBL.DRVR_MID_NM)
  ELSE UPPER(B1.MID_NM) END AS MIDDLE_NM,
  CAST(NULL AS STRING) AS NAME_SUFFIX_CD,
  CAST(NULL AS STRING) AS SALUTATION_CD,
  CASE WHEN PRODUCT_TYPE='Auto' AND (TBL.BRTH_DT < PARSE_DATE('%Y-%m-%d', '1910-01-01') OR TBL.BRTH_DT > DATE_SUB(CURRENT_DATE(),INTERVAL 10 YEAR)) THEN NULL
  WHEN PRODUCT_TYPE='Auto'  THEN TBL.BRTH_DT
  ELSE NULL END AS BIRTH_DATE,
  CAST(NULL AS STRING) AS GENDER_CD,
  CAST(NULL AS STRING) AS MARITAL_STATUS,
  CASE WHEN ADDR_LN1='' THEN NULL ELSE ADDR_LN1 END AS PRIMARY_ADDRESS_LINE1,
  CASE WHEN ADDR_LN2='' THEN NULL ELSE ADDR_LN2 END AS PRIMARY_ADDRESS_LINE2,
  CASE WHEN CTY_NM='' THEN NULL ELSE CTY_NM END AS PRIMARY_CITY,
  CASE WHEN ST_CD='' THEN NULL ELSE ST_CD END AS PRIMARY_STATE,
  CASE WHEN ZP_CD='' THEN NULL 
    WHEN LENGTH(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]','')) = 6 THEN REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]','')
    WHEN LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]',''),'[^0-9]','')) < 5 
    THEN LPAD(REGEXP_REPLACE(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),5,'0')
  ELSE SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),0,5)
END AS PRIMARY_ZIP,
  CAST(NULL AS STRING) AS PRIMARY_EMAIL_ADDRESS,
  COALESCE(HOME_TELEPHONE_NUM, WORK_PHONE_NUM) AS PRIMARY_PHONE_NUM,
  CASE WHEN ADDR_LN1='' THEN NULL ELSE ADDR_LN1 END AS RESIDENTIAL_ADDRESS_LINE1,
  CASE WHEN ADDR_LN2='' THEN NULL ELSE ADDR_LN2 END AS RESIDENTIAL_ADDRESS_LINE2,
  CTY_NM AS RESIDENTIAL_CITY,
  ST_CD AS RESIDENTIAL_STATE,
   CASE WHEN ZP_CD='' THEN NULL 
    WHEN LENGTH(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]','')) = 6 THEN REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]','')
    WHEN LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]',''),'[^0-9]','')) < 5 
    THEN LPAD(REGEXP_REPLACE(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),5,'0')
  ELSE SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(ZP_CD,'[^a-zA-Z0-9]',''),'[^0-9]',''),0,5)
END AS  RESIDENTIAL_ZIP,
  CAST(NULL AS STRING) AS OTHER_EMAIL_ADDRESS,
  CAST(NULL AS STRING) AS BILLING_ADDRESS_LINE1,
  CAST(NULL AS STRING) AS BILLING_ADDRESS_LINE2,
  CAST(NULL AS STRING) AS BILLING_CITY,
  CAST(NULL AS STRING) AS BILLING_STATE,
  CAST(NULL AS STRING) AS BILLING_ZIP,
  HOME_TELEPHONE_NUM ,
  WORK_PHONE_NUM ,
  CAST(NULL AS STRING) AS OTHER_PHONE_NUM,
  CURRENT_DATE() AS SOURCE_CREATE_DATE
FROM (
  SELECT
    IPD.*,
    E1.*
  FROM
    LANDING.WORK_INSURANCE_POLICY_DIM IPD--Replace with CUSTOMER_PRODUCT.INSURANCE_POLICY_DIM for full load and LANDING.WORK_INSURANCE_POLICY_DIM for incremental load
  LEFT OUTER JOIN (
    SELECT
      CONCAT(UPPER(E1.DRVR_LST_NM), '~',UPPER(E1.DRVR_FRST_NM), '~',CAST(E1.BRTH_DT AS STRING), '~',CAST(E1.DRVR_NUM AS STRING)) AS DRIVER_SOURCE_ID,
      E1.DRVR_TYP_CD AS DRIVER_TYP_CD,
      E1.LIC_ST_CD AS LICENSE_STATE,
      E1.LIC_DT AS LICENSE_DT,
      E1.STU_DISC_CD AS STUDENT_DISCOUNT_IND,
      E1.DRVR_FRST_NM,
      E1.BRTH_DT,
	  E1.DRVR_MID_NM,
      E1.DRVR_LST_NM,
      E1.DRVR_NUM,
      E1.DRVR_TRN_IND DRIVING_TRAINING_IND,
      E1.DFNS_DRVR_DT DEFENSIVE_DRIVER_DT,
      E1.FILE_ID,
      E1.TRANS_NUM
    FROM
      LANDING.IE_E1_LDG E1) E1
  ON
    IPD.SOURCE_ID = CONCAT("IE~",CAST(E1.FILE_ID AS STRING),"~",CAST(E1.TRANS_NUM AS STRING))
   ) TBL
LEFT OUTER JOIN
  (SELECT 
  LB1.*,
    CASE WHEN PHN_NUM1='' THEN NULL ELSE   CASE
    WHEN LENGTH(REGEXP_REPLACE(PHN_NUM1,'[^a-zA-Z0-9]','')) <= 10 THEN REGEXP_REPLACE(PHN_NUM1,'[^a-zA-Z0-9]','')
    WHEN SUBSTR(REGEXP_REPLACE(PHN_NUM1,'[^a-zA-Z0-9]',''),1,1) = '1' THEN SUBSTR(REGEXP_REPLACE(PHN_NUM1,'[^a-zA-Z0-9]',''),2,11)
  ELSE
  SUBSTR(REGEXP_REPLACE(PHN_NUM1,'[^a-zA-Z0-9]',''),1,10)
END END AS HOME_TELEPHONE_NUM ,
CASE WHEN PHN_NUM2='' THEN NULL ELSE   CASE
    WHEN LENGTH(REGEXP_REPLACE(PHN_NUM2,'[^a-zA-Z0-9]','')) <= 10 THEN REGEXP_REPLACE(PHN_NUM2,'[^a-zA-Z0-9]','')
    WHEN SUBSTR(REGEXP_REPLACE(PHN_NUM2,'[^a-zA-Z0-9]',''),1,1) = '1' THEN SUBSTR(REGEXP_REPLACE(PHN_NUM2,'[^a-zA-Z0-9]',''),2,11)
  ELSE
  SUBSTR(REGEXP_REPLACE(PHN_NUM2,'[^a-zA-Z0-9]',''),1,10)
END END AS WORK_PHONE_NUM   
  FROM LANDING.IE_B1_LDG LB1) B1
ON
  CONCAT("IE~",CAST(B1.FILE_ID AS STRING),"~",CAST(B1.TRANS_NUM AS STRING)) = TBL.SOURCE_ID );

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
    MARITAL_STATUS,
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
    OTHER_EMAIL_ADDRESS,
    HOME_TELEPHONE_NUM,
    WORK_PHONE_NUM,
    OTHER_PHONE_NUM,
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
    MARITAL_STATUS,
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
    OTHER_EMAIL_ADDRESS,
    HOME_TELEPHONE_NUM,
    WORK_PHONE_NUM,
    OTHER_PHONE_NUM,
    SOURCE_CREATE_DATE,
    CURRENT_DATETIME()
FROM
  LANDING.WORK_MDM_IVANS_IE
WHERE
  REGEXP_CONTAINS(PRIMARY_CITY, '[0-9]')
  OR LENGTH(PRIMARY_ZIP) < 5
  OR LENGTH(PRIMARY_ZIP) >10
  OR REGEXP_CONTAINS(FIRST_NM, '[0-9]')
  OR REGEXP_CONTAINS(LAST_NM, '[0-9]')
  OR FIRST_NM IS NULL
  OR LAST_NM IS NULL
  OR ((PRIMARY_ADDRESS_LINE1 IS NULL OR PRIMARY_ADDRESS_LINE1 ='') AND PRIMARY_EMAIL_ADDRESS IS NOT NULL);

DELETE
FROM
  LANDING.WORK_MDM_IVANS_IE
WHERE
  REGEXP_CONTAINS(PRIMARY_CITY, '[0-9]')
  OR LENGTH(PRIMARY_ZIP) < 5
  OR LENGTH(PRIMARY_ZIP) >10
  OR REGEXP_CONTAINS(FIRST_NM, '[0-9]')
  OR REGEXP_CONTAINS(LAST_NM, '[0-9]')
  OR FIRST_NM IS NULL
  OR LAST_NM IS NULL
  OR ((PRIMARY_ADDRESS_LINE1 IS NULL OR PRIMARY_ADDRESS_LINE1 ='') AND PRIMARY_EMAIL_ADDRESS IS NOT NULL);
  

----------------------------------------------------------------------
--STEP2.1
----------------------------------------------------------------------

----------------------------------------------------------------------
--STEP2.2
----------------------------------------------------------------------
update  LANDING.WORK_MDM_IVANS_IE set MERGE_FLAG='Golden',GOLDEN_MATCH_KEY=SOURCE_KEY3
where MERGE_FLAG ='      ' 
AND MERGE_CHECK2_CTL in (0,1) and MERGE_CHECK1_CTL in (0,1);
----------------------------------------------------------------------
--STEP2.3 -- This Update statement to identify the group where FIRST_NAME,LAST_NAME and BIRTH_DT are same.
----------------------------------------------------------------------

----------------------------------------------------------------------
--STEP2.4 -- Below Updates statement to identify the groups where FIRST_NAME,LAST_NAME, BIRTH_DT and ADDRESS are same from STEP2.3.
----------------------------------------------------------------------
UPDATE LANDING.WORK_MDM_IVANS_IE D
SET D.ADDRESS_MATCH_SOURCE_KEY=C.ADDRESS_MATCH_SOURCE_KEY,D.ADDRESS_MATCH_DESC='Merge' , D.GOLDEN_MATCH_KEY = C.ADDRESS_MATCH_SOURCE_KEY-- ADDRESS_MATCH
FROM (select SOURCE_KEY3,
FIRST_VALUE(SOURCE_KEY3) OVER (PARTITION BY UPPER(FIRST_NM),UPPER(LAST_NM),
BIRTH_DT,UPPER(PRIMARY_ADDRESS_LINE1),UPPER(PRIMARY_ADDRESS_LINE2),UPPER(PRIMARY_CITY),UPPER(PRIMARY_STATE),UPPER(PRIMARY_ZIP)
ORDER BY TERM_EXPIRATION_DT DESC) ADDRESS_MATCH_SOURCE_KEY
from LANDING.WORK_MDM_IVANS_IE where MERGE_FLAG ='      '  
and (MERGE_CHECK2_CTL+MERGE_CHECK1_CTL)>=2
) C
WHERE C.SOURCE_KEY3=D.SOURCE_KEY3
and D.MERGE_FLAG ='      ';
-----------
update  LANDING.WORK_MDM_IVANS_IE A set A.ADDRESS_MATCH_DESC ='Golden' -- ADDRESS_MATCH Golden Record
where SOURCE_KEY3 = ADDRESS_MATCH_SOURCE_KEY
and MERGE_FLAG ='      ';


-----------
update  LANDING.WORK_MDM_IVANS_IE A set A.MERGE_FLAG=A.ADDRESS_MATCH_DESC,A.GOLDEN_MATCH_KEY=ADDRESS_MATCH_SOURCE_KEY 
where A.MERGE_FLAG ='      ';

----------------------------------------------------------------------
--STEP2.7 -- DOB Merge , If Incase any DOB is null
----------------------------------------------------------------------
update  LANDING.WORK_MDM_IVANS_IE A 
SET A.MERGE_FLAG='DOB_Merge',A.GOLDEN_MATCH_KEY=B.GOLDEN_MATCH_KEY
FROM (
select C.SOURCE_KEY3 as SOURCE_KEY3,D.SOURCE_KEY3 as GOLDEN_MATCH_KEY FROM
(select * from LANDING.WORK_MDM_IVANS_IE where BIRTH_DT IS NULL and MERGE_FLAG='Golden' )C
INNER JOIN (select *,
ROW_NUMBER() OVER(PARTITION BY UPPER(FIRST_NM),UPPER(LAST_NM),UPPER(PRIMARY_ADDRESS_LINE1),UPPER(PRIMARY_ADDRESS_LINE2),UPPER(PRIMARY_CITY),
    UPPER(PRIMARY_STATE),UPPER(PRIMARY_ZIP) ORDER BY TERM_EXPIRATION_DT DESC) as ROW_NUM
    FROM LANDING.WORK_MDM_IVANS_IE where MERGE_FLAG='Golden' and BIRTH_DT IS NOT NULL ) D
ON COALESCE(C.FIRST_NM,'~')=COALESCE(D.FIRST_NM,'~')
AND COALESCE(C.LAST_NM,'~')=COALESCE(D.LAST_NM,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE1,'~')=COALESCE(D.PRIMARY_ADDRESS_LINE1,'~')
AND COALESCE(C.PRIMARY_ADDRESS_LINE2,'~')=COALESCE(D.PRIMARY_ADDRESS_LINE2,'~') 
AND COALESCE(C.PRIMARY_CITY,'~')=COALESCE(D.PRIMARY_CITY,'~') 
AND COALESCE(C.PRIMARY_STATE,'~')=COALESCE(D.PRIMARY_STATE,'~')
AND COALESCE(C.PRIMARY_ZIP,'~')=COALESCE(D.PRIMARY_ZIP,'~')
and D.ROW_NUM=1) B
where A.SOURCE_KEY3=B.SOURCE_KEY3;
----------------------------------------------------------------------
--STEP2.8 DOB Merge
----------------------------------------------------------------------
update  LANDING.WORK_MDM_IVANS_IE A 
SET A.GOLDEN_MATCH_KEY=B.GOLDEN_MATCH_KEY
FROM (select DOB.GOLDEN_MATCH_KEY AS GOLDEN_MATCH_KEY,
M.SOURCE_KEY3 as SOURCE_KEY3 FROM
(select SOURCE_KEY3,GOLDEN_MATCH_KEY from LANDING.WORK_MDM_IVANS_IE  
where MERGE_FLAG='DOB_Merge') DOB,
(select SOURCE_KEY3,GOLDEN_MATCH_KEY from LANDING.WORK_MDM_IVANS_IE 
where MERGE_FLAG='Merge') M
where DOB.SOURCE_KEY3=M.GOLDEN_MATCH_KEY) B
where A.MERGE_FLAG='Merge'
AND A.SOURCE_KEY3=B.SOURCE_KEY3;
----------------------------------------------------------------------
--STEP2.9 DOB Merge
----------------------------------------------------------------------
update LANDING.WORK_MDM_IVANS_IE SET MERGE_FLAG='Merge'
where MERGE_FLAG='DOB_Merge'
