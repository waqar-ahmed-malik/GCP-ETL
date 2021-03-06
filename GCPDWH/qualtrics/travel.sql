INSERT INTO OPERATIONAL.QUALTRICS_SURVEY_RESPONSE_TRAVEL(
START_DT,
END_DT,
STATUS,
IP_ADDRESS,
PROGRESS,
DURATION,
FINISHED,
RECORDED_DT,
RESPONSE_ID,
RECIPIENT_LAST_NM,
RECIPIENT_FIRST_NM,
RECIPIENT_EMAIL,
EXTERNAL_REFERENCE,
LOCATION_LAT,
LOCATION_LONG,
DISTRIBUTION_CHANNEL,
USER_LANGUAGE,
Q1,
Q2,
Q3,
Q4,
Q5,
Q6,
Q7,
Q8,
Q9_NPS_GROUP,
Q9,
Q10_NPS_GROUP,
Q10,
Q11,
AAA_CLUB_NUM,
INVOICE_DT,
INVOICE_NUM,
TRAVELER,
MEMBER_NBR,
MEMBER_OTH,
MEMBER_ACID,
MEMBER_FLG,
REV_TYPE,
INDEPENDENT_OR_GROUP,
DEPART_DT,
RETURN_DT,
BRANCH,
BKAGT,
MAILING_ADDRESS_LINE1,
MAILIN_GADDRESS_LINE2,
MAILING_CITY,
MAILING_STATE,
MAILING_ZIP,
EMAIL_ADDRESS,
EMAIL_ELIGIBILITY_FLG,
CONSUMER_ONLINE_TRX_FLG,
BRANCH_STATE,
STATE,
OPTIONAL_1,
OPTIONAL_2,
OPTIONAL_3,
OPTIONAL_4,
OPTIONAL_5,
TRAVEL_AGENT_FLG,
PROGRAM,
NATL_CD,
NATL_PRODUCT_TYPE_NM,
PRODUCT_CLASS,
TRIP_PRODUCTS,
ACCREDITATION,
Q_INVOICE_DT,
INVOICE_MONTH,
INVOICE_YEAR,
INVOICE_DAY,
Q_URL,
TOT_CLUBS,
CLUB_TIER,
WEIGHTED_SEGMENT,
MEMBER,
DIRECTIONAL_ERROR,
COMBINED_TEXT_ENTRY)
SELECT 
SAFE_CAST(START_DT AS TIMESTAMP),
SAFE_CAST(END_DT AS TIMESTAMP),
SAFE_CAST(STATUS AS INT64),
IP_ADDRESS,
SAFE_CAST(PROGRESS AS INT64),
SAFE_CAST(DURATION AS INT64),
SAFE_CAST(FINISHED AS INT64),
SAFE_CAST(RECORDED_DT AS TIMESTAMP),
RESPONSE_ID,
RECIPIENT_LAST_NM,
RECIPIENT_FIRST_NM,
RECIPIENT_EMAIL,
EXTERNAL_REFERENCE,
SAFE_CAST(LOCATION_LAT AS FLOAT64),
SAFE_CAST(LOCATION_LONG AS FLOAT64),
DISTRIBUTION_CHANNEL,
USER_LANGUAGE,
SAFE_CAST(Q1 AS INT64),
Q2,
SAFE_CAST(Q3 AS INT64),
SAFE_CAST(Q4 AS INT64),
SAFE_CAST(Q5 AS INT64),
SAFE_CAST(Q6 AS INT64),
SAFE_CAST(Q7 AS INT64),
SAFE_CAST(Q8 AS INT64),
SAFE_CAST(Q9_NPS_GROUP AS INT64),
SAFE_CAST(Q9 AS INT64),
SAFE_CAST(Q10_NPS_GROUP AS INT64),
SAFE_CAST(Q10 AS INT64),
Q11 ,
SAFE_CAST(AAA_CLUB_NUM AS INT64),
SAFE_CAST(INVOICE_DT AS DATE),
SAFE_CAST(INVOICE_NUM AS INT64),
TRAVELER,
SAFE_CAST(MEMBER_NBR AS INT64),
MEMBER_OTH,
MEMBER_ACID,
SAFE_CAST(MEMBER_FLG AS BOOLEAN),
REV_TYPE,
INDEPENDENT_OR_GROUP,
SAFE_CAST(DEPART_DT AS DATE),
SAFE_CAST(RETURN_DT AS DATE),
SAFE_CAST(BRANCH AS INT64),
BKAGT,
MAILING_ADDRESS_LINE1,
MAILIN_GADDRESS_LINE2,
MAILING_CITY,
MAILING_STATE,
SAFE_CAST(MAILING_ZIP AS INT64),
EMAIL_ADDRESS,
EMAIL_ELIGIBILITY_FLG,
CONSUMER_ONLINE_TRX_FLG,
BRANCH_STATE,
STATE,
OPTIONAL_1,
OPTIONAL_2,
OPTIONAL_3,
OPTIONAL_4,
OPTIONAL_5,
SAFE_CAST(TRAVEL_AGENT_FLG AS BOOLEAN),
PROGRAM,
NATL_CD,
NATL_PRODUCT_TYPE_NM,
PRODUCT_CLASS,
TRIP_PRODUCTS,
SAFE_CAST(ACCREDITATION AS BOOLEAN),
SAFE_CAST(Q_INVOICE_DT AS DATE),
SAFE_CAST(INVOICE_MONTH AS INT64),
SAFE_CAST(INVOICE_YEAR AS INT64),
SAFE_CAST(INVOICE_DAY AS INT64),
Q_URL,
SAFE_CAST(TOT_CLUBS AS INT64),
CLUB_TIER,
WEIGHTED_SEGMENT,
SAFE_CAST(MEMBER AS BOOLEAN),
SAFE_CAST(DIRECTIONAL_ERROR  AS BOOLEAN),
COMBINED_TEXT_ENTRY
FROM LANDING.WORK_SURVEY_TRAVEL