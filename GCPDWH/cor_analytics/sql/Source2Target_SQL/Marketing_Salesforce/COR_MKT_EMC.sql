SELECT
    SEGMENT_NAME,
    AC_CUSTOMER_ID,
    SUBSCRIBER_KEY,
    MEMBER_NUMBER_16_DIGITS,
    FIRST_NAME,
    LAST_NAME,
    EMAIL_ADDRESS,
    PARSE_DATE('%F', SOURCE ) AS SOURCE
FROM `COR_ANALYTICS.STG_COR_MKT_EMC_UPDATED`