INSERT INTO DIGITAL_MARKETING.DIRECT_FOCUS_INDIVIDUAL_DIM
(
CID,
MEMBER_NUM ,
NCNU_MEMBERSHIP_NUM,
STANDARD_FIRST_NM,
STANDARD_LAST_NM,
STANDARD_MIDDLE_NM,
BIRTH_DTTIME,
STANDARD_ADDRESS_LINE1,
STANDARD_ADDRESS_LINE2,
STANDARD_ADDRESS_NUM,
STANDARD_CITY,
STANDARD_COUNTY_NM,
STANDARD_CR,
STANDARD_LAT,
STANDARD_LATITUDE,
STANDARD_LONG,
STANDARD_LONGITUDE,
STANDARD_STATE,
STANDARD_STATUS_CD,
STANDARD_STREET_NM,
STANDARD_ZIP,
STANDARD_ZIP4,
DMA_PANDER_FLAG,
CENSUS_INCOME_INDEX_2010,
COMBINED_PHONE,
COMBINED_LENGTH_OF_RESIDENCE,
COMBINED_MARITAL_STATUS,
HOME_PURCHASE_PRICE ,
HOME_TOTAL_VALUE,
HOUSEHOLD_COMPOSITION,
LIFE_ATTRITION_MODEL,
LIFE_ATTRITION_MODEL_DECILE,
MOSAIC_HOUSEHOLD_2011,
MOTORCYCLE_OWNERSHIP_CD,
MOVE_UPDATE_CD,
MOVE_UPDATE_DTTIME,
NCNU_LAST_DOWNGRADE_DTTIME,
NCNU_LAST_UPGRADE_DTTIME,
NCNU_MEMBER_ROLE_CD,
NCNU_OK_TO_SOLICIT,
NCNU_PRODUCT_PRICE,
NCNU_PRODUCT_PRICE_CHANGE_DTTIME,
NOASSOC,
NUM_CHILDREN_DRV,
NUM_OF_ADULTS_IN_HOUSEHOLD,
NUM_OF_PERSONS_LIVING_UNIT,
NUM_OF_SOURCES,
NUM_OF_VEHICLES_REGISTERED,
OWNERSHIP_CD_DRV,
PREMIUM_DECILE,
PRESENCE_16_19_YR_OLD,
PRESENCE_19_IN_HH,
PRESENCE_BOAT_MOTORCYCLE_RV,
PRESENCE_OF_BOAT,
PROSPECT_SURVIVAL_DECILE,
PURCHASED_THROUGH_THE_MAIL,
RECREATIONAL_VEHICLE_OWNER_CD,
V1_HH_ESTIMATED_INCOME_RANGE,
V1_PROSPECT_ESTIMA_INCOME_RANGE,
V1_RAAMED_INCOME_RANGE,
WEALTH,
WEALTH_STATE,
ARCHIVE_DTTIME,
ROW_START_DT,
ROW_END_DT,
ACTIVE_FLG,
MD5_VALUE,
UPDATE_DTTIME,
JOB_RUN_ID,
SOURCE_SYSTEM_CD
)
SELECT DISTINCT * EXCEPT(DUP_CHECK) FROM (
SELECT
STG.CID ,
STG.MEM16,
STG.NCNU_MEMBERSHIP_NUM,
STG.STD_FNAME ,
STG.STD_LNAME ,
STG.STD_MNAME ,
CASE 
WHEN STG.BIRTH_DT = 'NULL' OR STG.BIRTH_DT IS NULL THEN NULL
ELSE PARSE_DATETIME('%Y-%m-%d %H:%M:%S',STG.BIRTH_DT)
END AS BIRTH_DTTIME,
STG.STD_ADDRESS ,
STG.STD_ADDRESS2 ,
STG.STD_ADDNUM ,
STG.STD_CITY ,
STG.STD_COUNTY_NAME ,
STG.STD_CR ,
STG.STD_LAT ,
STG.STD_LATITUDE ,
STG.STD_LONG ,
STG.STD_LONGITUDE ,
STG.STD_STATE ,
STG.STD_STATUS_CD ,
STG.STD_STNAME ,
STG.STD_ZIP ,
STG.STD_ZIP4 ,
STG.DMA_PANDER_FLAG,
SAFE_CAST(STG.CENSUS_INCOME_INDEX_2010 AS STRING),
STG.COMBINED_PHONE,
STG.COMBINED_LENGTH_OF_RESIDENCE ,
STG.COMBINED_MARITAL_STATUS ,
STG.HOME_PURCH_PRICE ,
STG.HOME_TOTAL_VALUE ,
STG.HOUSEHOLD_COMPOSITION ,
STG.LIFE_ATTRITION_MODEL ,
STG.LIFE_ATTRITION_MODEL_DECILE ,
STG.MOSAIC_HOUSEHOLD_2011 ,
STG.MOTORCYCLE_OWNERSHIP_CODE ,
STG.MOVE_UPDATE_CODE ,
CASE 
WHEN STG.MOVE_UPDATE_DATE ='NULL' OR STG.MOVE_UPDATE_DATE IS NULL THEN NULL 
ELSE PARSE_DATETIME('%Y-%m-%d %H:%M:%S',STG.MOVE_UPDATE_DATE) 
END AS MOVE_UPDATE_DTTIME,
CASE 
WHEN STG.NCNU_LAST_DOWNGRADE_DT ='NULL' OR STG.NCNU_LAST_DOWNGRADE_DT IS NULL THEN NULL
ELSE PARSE_DATETIME('%Y-%m-%d %H:%M:%S',STG.NCNU_LAST_DOWNGRADE_DT)
END AS NCNU_LAST_DOWNGRADE_DTTIME,
CASE 
WHEN STG.NCNU_LAST_UPGRADE_DT ='NULL' OR NCNU_LAST_UPGRADE_DT IS NULL THEN NULL 
ELSE PARSE_DATETIME('%Y-%m-%d %H:%M:%S',STG.NCNU_LAST_UPGRADE_DT)
END AS NCNU_LAST_UPGRADE_DTTIME,
STG.NCNU_MEMBER_ROLE_CD ,
STG.NCNU_OK_TO_SOLICIT ,
CASE 
WHEN NCNU_PRODUCT_PRICE = 'NULL' OR NCNU_PRODUCT_PRICE IS NULL THEN NULL 
ELSE SAFE_CAST(STG.NCNU_PRODUCT_PRICE AS FLOAT64) 
END AS NCNU_PRODUCT_PRICE,
CASE 
WHEN STG.NCNU_PRODUCT_PRICE_CHANGE_DT = 'NULL' OR STG.NCNU_PRODUCT_PRICE_CHANGE_DT IS NULL THEN NULL
ELSE PARSE_DATETIME('%Y-%m-%d %H:%M:%S',STG.NCNU_PRODUCT_PRICE_CHANGE_DT)
END AS NCNU_PRODUCT_PRICE_CHANGE_DTTIME,
SAFE_CAST(STG.NOASSOC AS INT64) NOASSOC ,
STG.NUMBER_CHILDREN_DRV ,
STG.NUMBER_OF_ADULTS_IN_HOUSEHOLD ,
STG.NUMBER_OF_PERSONS_LIVING_UNIT ,
STG.NUMBER_OF_SOURCES ,
STG.NUMBER_OF_VEHICLES_REGISTERED ,
STG.OWNERSHIP_CODE_DRV ,
STG.PREMIUM_DECILE ,
STG.PRESENCE_16_19_YR_OLD ,
STG.PRESENCE_19_IN_HH ,
STG.PRESENCE_BOAT_MOTORCYCLE_RV ,
STG.PRESENCE_OF_BOAT ,
STG.PROSPECTSURVIVAL_DECILE ,
STG.PURCHASED_THROUGH_THE_MAIL ,
STG.RECREATIONAL_VEHICLE_OWNER_CD ,
STG.V1_HHESTIMATEDINCOMERANGE ,
STG.V1_PROSPECT_ESTIMA_INCOME_RNG AS V1_PROSPECT_ESTIMA_INCOME_RANGE,
STG.V1_RAAMEDINCOMERANGE ,
STG.WEALTH ,
STG.WEALTH_STATE ,
CASE 
WHEN STG. ARCHIVE_DT ='NULL' OR STG.ARCHIVE_DT IS NULL THEN NULL 
ELSE PARSE_DATETIME('%Y-%m-%d %H:%M:%S',STG.ARCHIVE_DT)
END AS ARCHIVE_DTTIME ,
CASE
WHEN STG.ARCHIVE_DT = 'NULL' OR STG.ARCHIVE_DT IS NULL THEN NULL
ELSE EXTRACT(DATE FROM PARSE_DATETIME('%Y-%m-%d %H:%M:%S',STG.ARCHIVE_DT))
END AS ROW_START_DT,
DATE('9999-12-31') AS ROW_END_DT,
'Y' AS ACTIVE_FLG,
STG.MD5_VALUE,
CURRENT_DATETIME() AS UPDATE_DTTIME,
SAFE_CAST('jobrunid' AS INT64) AS JOB_RUN_ID,
'LEXIS NEXIS' AS SOURCE_SYSTEM_CD,
ROW_NUMBER() OVER (PARTITION BY STG.CID  ORDER BY ARCHIVE_DT DESC ) DUP_CHECK
FROM LANDING.WORK_INDIVIDUAL_MD5 STG 
WHERE (( STG.CID NOT IN ( SELECT CID FROM `DIGITAL_MARKETING.DIRECT_FOCUS_INDIVIDUAL_DIM` WHERE ACTIVE_FLG='Y') )  --For New Policies
OR (STG.CID IN ( SELECT CID FROM  `DIGITAL_MARKETING.DIRECT_FOCUS_INDIVIDUAL_DIM`  DIM  WHERE DIM.ACTIVE_FLG='Y') AND STG.MD5_VALUE NOT IN (SELECT DIM.MD5_VALUE FROM `DIGITAL_MARKETING.DIRECT_FOCUS_INDIVIDUAL_DIM`  DIM  )))) WHERE DUP_CHECK = 1-- For Type 2 History Based on MD5 Value Change for Existing Policie
