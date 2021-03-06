CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_REGISTRATIONS_OVERVIEW (
JOIN_DTTIME DATETIME ,
CUSTOMER_ID INT64 ,
PHONE_NUM STRING ,
FIRST_NM STRING ,
LAST_NM STRING ,
EMAIL STRING ,
MAILING_ADDRESS STRING ,
MAILING_CITY STRING ,
MAILING_STATE STRING ,
LICENSE_ISSUED_STATE STRING ,
BLOCKED_DTTIME DATETIME ,
MEMBER_NUM STRING ,
BACKGROUND_REPORT_DTTIME DATETIME ,
BACKGROUND_REPORT_RESULT STRING ,
IS_PHONE_VERIFIED STRING ,
IS_JUMIO_VERIFIED STRING ,
ACCOUNT_STATUS STRING ,
RF_ID STRING ,
CREDITS FLOAT64 ,
CREDITS_SOURCE STRING ,
CAN_MEMBER_RESERVE_CAR STRING ,
HAS_VALID_CC STRING ,
JOB_RUN_ID STRING ,
SOURCE_SYSTEM_CD STRING ,
CREATE_DTTIME DATETIME ,
CREATE_BY STRING );

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_DAILY_VEHICLE_HISTORY_REPORT (
TIMESTAMP DATETIME ,
CREATED_DTTIME DATETIME ,
VIN STRING ,
LICENSE_PLATE_NUM STRING ,
VEHICLE_MAKE STRING ,
VEHICLE_COLOR STRING ,
VEHICLE_GROUP STRING ,
CAPACITY INT64 ,
BIKE_RACK_EQUIPPED STRING ,
LIFT_EQUIPPED STRING ,
IN_FLEET_DTTIME DATETIME ,
DE_FLEET_DTTIME DATETIME ,
REGISTRATION_DTTIME DATETIME ,
INSURANCE_DTTIME DATETIME ,
POOL STRING ,
FUEL_PERCENTAGE FLOAT64 ,
CHARGING_STATE STRING ,
DOOR_STATUS STRING ,
RESERVATION_STATUS STRING ,
UPDATED_DTTIME DATETIME ,
GPS_TIMESTAMP DATETIME ,
LAT FLOAT64 ,
LONG FLOAT64 ,
SERVICE_ID INT64 ,
JOB_RUN_ID STRING ,
SOURCE_SYSTEM_CD STRING ,
CREATE_DTTIME DATETIME ,
CREATE_BY STRING );

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_DAILY_RENTAL_REPORT (
EVENT_ID INT64 ,
RESERVATION_MODE STRING ,
RESERVATION_START_DTTIME DATETIME ,
BOOKING_START_DTTIME DATETIME ,
BOOKING_ENDED_DTTIME DATETIME ,
CANCELLED_DTTIME DATETIME ,
CANCELLATION_REASON STRING ,
DRIVE_TM STRING ,
PARKED_TM STRING ,
TOTAL_TO_CHARGE FLOAT64 ,
MILEAGE_DRIVEN FLOAT64 ,
START_LOCATION_DISPLAY STRING ,
START_LOCATION_LAT FLOAT64 ,
START_LOCATION_LONG FLOAT64 ,
START_LOCATION_ADDRESS STRING ,
END_LOCATION_DISPLAY STRING ,
END_LOCATION_LAT FLOAT64 ,
END_LOCATION_LONG FLOAT64 ,
END_LOCATION_ADDRESS STRING ,
FIRST_NM STRING ,
LAST_NM STRING ,
EMAIL STRING ,
RECEIPT_ID INT64 ,
DRIVE_FARE FLOAT64 ,
PARKED_FARE FLOAT64 ,
ADDITIONAL_MILEAGE_CHARGE FLOAT64 ,
SHARED_ASSET_FEE FLOAT64 ,
CREDITS_USED FLOAT64 ,
TRANSACTION_FEE FLOAT64 ,
BLOCK_DISCOUNT_AMT FLOAT64 ,
BLOCKS FLOAT64 ,
DEBITED_DTTIME DATETIME ,
DEBIT_FAILED_DTTIME DATETIME ,
REFUNDED FLOAT64 ,
STRIPE_ID STRING ,
REFUND_REASON STRING ,
EXEMPT STRING ,
MEMBERSHIP_DISCOUNT STRING ,
TOTAL_TAX STRING ,
STATE_TAX_NM STRING ,
STATE_TAX_AMT FLOAT64 ,
COUNTY_TAX_NM STRING ,
COUNTY_TAX_AMT FLOAT64 ,
DISTRICT_TAX_NM STRING ,
DISTRICT_TAX_AMT FLOAT64 ,
VIN STRING ,
VEHICLE_MAKE STRING ,
VEHICLE_NM STRING ,
VEHICLE_LICENSE_PLATE STRING ,
SERVICE_ID INT64 ,
JOB_RUN_ID STRING ,
SOURCE_SYSTEM_CD STRING ,
CREATE_DTTIME DATETIME ,
CREATE_BY STRING );

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_EVENTS (
SRC_CREATED_DTTIME DATETIME ,
SITE_NM STRING ,
SITE_ID INT64 ,
PUBLISHER_NM STRING ,
PUBLISHER_ID INT64 ,
SITE_EVENT_NM STRING ,
SITE_EVENT_ID INT64 ,
STATUS STRING ,
IS_RE_ENGAGEMENT STRING ,
GOOGLE_AID STRING ,
IOS_IFA STRING ,
TEST_PROFILE_NM STRING ,
TEST_PROFILE_ID INT64 ,
INSTALL_PUBLISHER_NM STRING ,
INSTALL_PUBLISHER_ID INT64 ,
ATTRIBUTE_SUB1 STRING ,
ATTRIBUTE_SUB2 STRING ,
ATTRIBUTE_SUB3 STRING ,
ATTRIBUTE_SUB4 STRING ,
ATTRIBUTE_SUB5 STRING ,
LAT FLOAT64 ,
LONG FLOAT64 ,
TRACKING_ID STRING ,
DEVICE_BRAND STRING ,
DEVICE_CARRIER STRING ,
DEVICE_MODEL STRING ,
CAMPAIGN_URL STRING ,
CAMPAIGN_URL_ID INT64 ,
AGENCY_NM STRING ,
AGENCY_ID INT64 ,
MATCH_TYPE STRING ,
CAMPAIGN_NM STRING ,
CAMPAIGN_ID INT64 ,
EVENT_TYPE STRING ,
POSTAL_CD INT64 ,
REGION_NM STRING ,
REGION_ID INT64 ,
TRANSACTION_ID STRING ,
APP_NM STRING ,
APP_VERSION STRING ,
USER_ID STRING ,
PUBLISHER_SUB_AD_NM STRING ,
PUBLISHER_SUB_AD_ID INT64 ,
PUBLISHER_SUB_AD_REF STRING ,
PUBLISHER_SUB_ADGROUP_NM STRING ,
PUBLISHER_SUB_ADGROUP_ID INT64 ,
PUBLISHER_SUB_ADGROUP_REF STRING ,
PUBLISHER_SUB_CAMPAIGN_NM STRING ,
PUBLISHER_SUB_CAMPAIGN_ID INT64 ,
PUBLISHER_SUB_CAMPAIGN_REF STRING ,
PUBLISHER_SUB_SITE_NM STRING ,
PUBLISHER_SUB_SITE_ID INT64 ,
PUBLISHER_SUB_SITE_REF STRING ,
JOB_RUN_ID STRING ,
SOURCE_SYSTEM_CD STRING ,
CREATE_DTTIME DATETIME ,
CREATE_BY STRING );

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_DAILY_RENTAL_RATING (
CREATED_DTTIME DATETIME ,
EVENT_ID INT64 ,
LICENSE_PLATE_NUM STRING ,
REPORTED_BY STRING ,
RATING FLOAT64 ,
RATING_REASON STRING ,
COMMENT STRING ,
SERVICE_ID INT64 ,
JOB_RUN_ID STRING ,
SOURCE_SYSTEM_CD STRING ,
CREATE_DTTIME DATETIME ,
CREATE_BY STRING );

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_DAILY_DAMAGE_REPORT (
ID INT64 ,
CREATED_DTTIME DATETIME ,
UPDATED_DTTIME DATETIME ,
DAMAGE_LOCATION STRING ,
DAMAGE_TYPE STRING ,
VEHICLE_MAKE STRING ,
VEHICLE_COLOR STRING ,
LICENSE_PLATE_NUM STRING ,
VIN STRING ,
REPORTED_BY STRING ,
PHONE_NUM STRING ,
SERVICE_ID INT64 ,
JOB_RUN_ID STRING ,
SOURCE_SYSTEM_CD STRING ,
CREATE_DTTIME DATETIME ,
CREATE_BY STRING );

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.APPOPEN_RESERVATION_BOOKING (
APPOPEN_DT DATE ,
TOTAL_APPOPEN_COUNT INT64 ,
REGISTERED_USER_APPOPEN_COUNT INT64 ,
RESERVATION_COUNT INT64 ,
BOOKING_COUNT INT64 ,
JOB_RUN_ID STRING ,
SOURCE_SYSTEM_CD STRING ,
CREATE_DTTIME DATETIME ,
CREATE_BY STRING );

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_UNSUBSCRIBE (
EMAIL_ADDRESS STRING , 
FIRST_NAME STRING , 
LAST_NAME STRING , 
ADDRESS STRING , 
PHONE_NUMBER STRING , 
RIDE_STATUS STRING , 
TEST_GROUP STRING , 
CODE STRING , 
MEMBER_RATING INT64 , 
OPTIN_TIME DATETIME , 
OPTIN_IP STRING , 
CONFIRM_TIME DATETIME , 
CONFIRM_IP STRING , 
LATITUDE STRING , 
LONGITUDE STRING , 
GMTOFF STRING , 
DSTOFF STRING , 
TIMEZONE STRING , 
CC STRING , 
REGION STRING , 
UNSUB_TIME DATETIME , 
UNSUB_CAMPAIGN_TITLE STRING , 
UNSUB_CAMPAIGN_ID STRING , 
UNSUB_REASON STRING , 
UNSUB_REASON_OTHER STRING , 
LEID INT64 , 
EUID STRING , 
NOTES STRING ,
JOB_RUN_ID INT64,
SOURCE_SYSTEM_CD STRING,
CREATE_DTTIME DATETIME,
CREATE_BY STRING);


CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.GIG_CONTACT_RESTRICTION (
CUST_EMAIL STRING,
RESTRICTION_TYPE STRING,
CREATE_DTTIME DATETIME, 
CREATE_USER STRING );