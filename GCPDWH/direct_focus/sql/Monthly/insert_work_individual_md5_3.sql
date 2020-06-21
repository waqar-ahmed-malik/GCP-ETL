INSERT INTO LANDING.WORK_INDIVIDUAL_MD5(
CID,
MEM16,
NCNU_MEMBERSHIP_NUM,
STD_FNAME,
STD_LNAME,
STD_MNAME,
BIRTH_DT,
STD_ADDRESS,
STD_ADDRESS2,
STD_ADDNUM,
STD_CITY,
STD_COUNTY_NAME,
STD_CR,
STD_LAT,
STD_LATITUDE,
STD_LONG,
STD_LONGITUDE,
STD_STATE,
STD_STATUS_CD,
STD_STNAME,
STD_ZIP,
STD_ZIP4,
DMA_PANDER_FLAG,
CENSUS_INCOME_INDEX_2010,
COMBINED_PHONE,
COMBINED_LENGTH_OF_RESIDENCE,
COMBINED_MARITAL_STATUS,
HOME_PURCH_PRICE,
HOME_TOTAL_VALUE,
HOUSEHOLD_COMPOSITION,
LIFE_ATTRITION_MODEL,
LIFE_ATTRITION_MODEL_DECILE,
MOSAIC_HOUSEHOLD_2011,
MOTORCYCLE_OWNERSHIP_CODE,
MOVE_UPDATE_CODE,
MOVE_UPDATE_DATE,
NCNU_LAST_DOWNGRADE_DT,
NCNU_LAST_UPGRADE_DT,
NCNU_MEMBER_ROLE_CD,
NCNU_OK_TO_SOLICIT,
NCNU_PRODUCT_PRICE,
NCNU_PRODUCT_PRICE_CHANGE_DT,
NOASSOC,
NUMBER_CHILDREN_DRV,
NUMBER_OF_ADULTS_IN_HOUSEHOLD,
NUMBER_OF_PERSONS_LIVING_UNIT,
NUMBER_OF_SOURCES,
NUMBER_OF_VEHICLES_REGISTERED,
OWNERSHIP_CODE_DRV,
PREMIUM_DECILE,
PRESENCE_16_19_YR_OLD,
PRESENCE_19_IN_HH,
PRESENCE_BOAT_MOTORCYCLE_RV,
PRESENCE_OF_BOAT,
PROSPECTSURVIVAL_DECILE,
PURCHASED_THROUGH_THE_MAIL,
RECREATIONAL_VEHICLE_OWNER_CD,
V1_HHESTIMATEDINCOMERANGE,
V1_PROSPECT_ESTIMA_INCOME_RNG,
V1_RAAMEDINCOMERANGE,
WEALTH,
WEALTH_STATE,
ARCHIVE_DT,
MD5_VALUE
)
SELECT
CID AS CID,
MEM16 AS MEM16,
NCNU_MEMBERSHIP_NUM AS NCNU_MEMBERSHIP_NUM,
STD_FNAME AS STD_FNAME,
STD_LNAME AS STD_LNAME,
STD_MNAME AS STD_MNAME,
BIRTH_DT AS BIRTH_DT,
STD_ADDRESS AS STD_ADDRESS,
STD_ADDRESS2 AS STD_ADDRESS2,
STD_ADDNUM AS STD_ADDNUM,
STD_CITY AS STD_CITY,
STD_COUNTY_NAME AS STD_COUNTY_NAME,
STD_CR AS STD_CR,
STD_LAT AS STD_LAT,
STD_LATITUDE AS STD_LATITUDE,
STD_LONG AS STD_LONG,
STD_LONGITUDE AS STD_LONGITUDE,
STD_STATE AS STD_STATE,
STD_STATUS_CD AS STD_STATUS_CD,
STD_STNAME AS STD_STNAME,
STD_ZIP AS STD_ZIP,
STD_ZIP4 AS STD_ZIP4,
DMA_PANDER_FLAG AS DMA_PANDER_FLAG,
_census_income_index AS CENSUS_INCOME_INDEX_2010,
COMBINED_PHONE AS COMBINED_PHONE,
COMBINED_LENGTH_OF_RESIDENCE AS COMBINED_LENGTH_OF_RESIDENCE,
COMBINED_MARITAL_STATUS AS COMBINED_MARITAL_STATUS,
HOME_PURCH_PRICE AS HOME_PURCH_PRICE,
HOME_TOTAL_VALUE AS HOME_TOTAL_VALUE,
HOUSEHOLD_COMPOSITION AS HOUSEHOLD_COMPOSITION,
LIFE_ATTRITION_MODEL AS LIFE_ATTRITION_MODEL,
LIFE_ATTRITION_MODEL_DECILE AS LIFE_ATTRITION_MODEL_DECILE,
MOSAIC_HOUSEHOLD_2011 AS MOSAIC_HOUSEHOLD_2011,
MOTORCYCLE_OWNERSHIP_CODE AS MOTORCYCLE_OWNERSHIP_CODE,
MOVE_UPDATE_CODE AS MOVE_UPDATE_CODE,
MOVE_UPDATE_DATE AS MOVE_UPDATE_DATE,
NCNU_LAST_DOWNGRADE_DT AS NCNU_LAST_DOWNGRADE_DT,
NCNU_LAST_UPGRADE_DT AS NCNU_LAST_UPGRADE_DT,
NCNU_MEMBER_ROLE_CD AS NCNU_MEMBER_ROLE_CD,
NCNU_OK_TO_SOLICIT AS NCNU_OK_TO_SOLICIT,
NCNU_PRODUCT_PRICE AS NCNU_PRODUCT_PRICE,
NCNU_PRODUCT_PRICE_CHANGE_DT AS NCNU_PRODUCT_PRICE_CHANGE_DT,
NOASSOC AS NOASSOC,
NUMBER_CHILDREN_DRV AS NUMBER_CHILDREN_DRV,
NUMBER_OF_ADULTS_IN_HOUSEHOLD AS NUMBER_OF_ADULTS_IN_HOUSEHOLD,
NUMBER_OF_PERSONS_LIVING_UNIT AS NUMBER_OF_PERSONS_LIVING_UNIT,
NUMBER_OF_SOURCES AS NUMBER_OF_SOURCES,
NUMBER_OF_VEHICLES_REGISTERED AS NUMBER_OF_VEHICLES_REGISTERED,
OWNERSHIP_CODE_DRV AS OWNERSHIP_CODE_DRV,
PREMIUM_DECILE AS PREMIUM_DECILE,
PRESENCE_16_19_YR_OLD AS PRESENCE_16_19_YR_OLD,
PRESENCE_19_IN_HH AS PRESENCE_19_IN_HH,
PRESENCE_BOAT_MOTORCYCLE_RV AS PRESENCE_BOAT_MOTORCYCLE_RV,
PRESENCE_OF_BOAT AS PRESENCE_OF_BOAT,
PROSPECTSURVIVAL_DECILE AS PROSPECTSURVIVAL_DECILE,
PURCHASED_THROUGH_THE_MAIL AS PURCHASED_THROUGH_THE_MAIL,
RECREATIONAL_VEHICLE_OWNER_CD AS RECREATIONAL_VEHICLE_OWNER_CD,
V1_HHESTIMATEDINCOMERANGE AS V1_HHESTIMATEDINCOMERANGE,
V1_PROSPECT_ESTIMA_INCOME_RNG AS V1_PROSPECT_ESTIMA_INCOME_RNG,
V1_RAAMEDINCOMERANGE AS V1_RAAMEDINCOMERANGE,
WEALTH AS WEALTH,
WEALTH_STATE AS WEALTH_STATE,
ARCHIVE_DT AS ARCHIVE_DT,
SAFE_CAST(CONCAT(
COALESCE(MEM16, ' ' ),
COALESCE(NCNU_MEMBERSHIP_NUM, ' ' ),
COALESCE(STD_FNAME, ' ' ),
COALESCE(STD_LNAME, ' ' ),
COALESCE(STD_MNAME, ' ' ),
COALESCE(BIRTH_DT, ' ' ),
COALESCE(STD_ADDRESS, ' ' ),
COALESCE(STD_ADDRESS2, ' ' ),
COALESCE(STD_ADDNUM, ' ' ),
COALESCE(STD_CITY, ' ' ),
COALESCE(STD_COUNTY_NAME, ' ' ),
COALESCE(STD_CR, ' ' ),
COALESCE(STD_LAT, ' ' ),
COALESCE(STD_LATITUDE, ' ' ),
COALESCE(STD_LONG, ' ' ),
COALESCE(STD_LONGITUDE, ' ' ),
COALESCE(STD_STATE, ' ' ),
COALESCE(STD_STATUS_CD, ' ' ),
COALESCE(STD_STNAME, ' ' ),
COALESCE(STD_ZIP, ' ' ),
COALESCE(STD_ZIP4, ' ' ),
COALESCE(DMA_PANDER_FLAG, ' ' ),
COALESCE(SAFE_CAST(_census_income_index AS STRING), ' ' ),
COALESCE(COMBINED_PHONE, ' ' ),
COALESCE(COMBINED_LENGTH_OF_RESIDENCE, ' ' ),
COALESCE(COMBINED_MARITAL_STATUS, ' ' ),
COALESCE(HOME_PURCH_PRICE, ' ' ),
COALESCE(HOME_TOTAL_VALUE, ' ' ),
COALESCE(HOUSEHOLD_COMPOSITION, ' ' ),
COALESCE(LIFE_ATTRITION_MODEL, ' ' ),
COALESCE(LIFE_ATTRITION_MODEL_DECILE, ' ' ),
COALESCE(MOSAIC_HOUSEHOLD_2011, ' ' ),
COALESCE(MOTORCYCLE_OWNERSHIP_CODE, ' ' ),
COALESCE(MOVE_UPDATE_CODE, ' ' ),
COALESCE(MOVE_UPDATE_DATE, ' ' ),
COALESCE(NCNU_LAST_DOWNGRADE_DT, ' ' ),
COALESCE(NCNU_LAST_UPGRADE_DT, ' ' ),
COALESCE(NCNU_MEMBER_ROLE_CD, ' ' ),
COALESCE(NCNU_OK_TO_SOLICIT, ' ' ),
COALESCE(NCNU_PRODUCT_PRICE, ' ' ),
COALESCE(NCNU_PRODUCT_PRICE_CHANGE_DT, ' ' ),
COALESCE(NOASSOC, ' ' ),
COALESCE(NUMBER_CHILDREN_DRV, ' ' ),
COALESCE(NUMBER_OF_ADULTS_IN_HOUSEHOLD, ' ' ),
COALESCE(NUMBER_OF_PERSONS_LIVING_UNIT, ' ' ),
COALESCE(NUMBER_OF_SOURCES, ' ' ),
COALESCE(NUMBER_OF_VEHICLES_REGISTERED, ' ' ),
COALESCE(OWNERSHIP_CODE_DRV, ' ' ),
COALESCE(PREMIUM_DECILE, ' ' ),
COALESCE(PRESENCE_16_19_YR_OLD, ' ' ),
COALESCE(PRESENCE_19_IN_HH, ' ' ),
COALESCE(PRESENCE_BOAT_MOTORCYCLE_RV, ' ' ),
COALESCE(PRESENCE_OF_BOAT, ' ' ),
COALESCE(PROSPECTSURVIVAL_DECILE, ' ' ),
COALESCE(PURCHASED_THROUGH_THE_MAIL, ' ' ),
COALESCE(RECREATIONAL_VEHICLE_OWNER_CD, ' ' ),
COALESCE(V1_HHESTIMATEDINCOMERANGE, ' ' ),
COALESCE(V1_PROSPECT_ESTIMA_INCOME_RNG, ' ' ),
COALESCE(V1_RAAMEDINCOMERANGE, ' ' ),
COALESCE(WEALTH, ' ' ),
COALESCE(WEALTH_STATE, ' ' )
) AS BYTES) AS MD5_VALUE
FROM LANDING.WORK_INDIVIDUAL_3