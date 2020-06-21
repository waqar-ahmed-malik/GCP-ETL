INSERT INTO `LANDING.SAM_QUOTES_LDG`(
QUOTE_ID,
OPPORTUNITY_ID,
CUSTOMER_ID,
SOURCE_CUSTOMER_ID,
SOURCE_QUOTE_ID,
POLICY_STATUS,
PRODUCT_TYPE,
PRODUCT_CD,
POLICY_STATE,
EFFECTIVE_DT,
CREATE_DTTIME,
PREMIUM,
POLICY_TERM_MONTHS,
POLICY_NUM,
SOURCE_SYSTEM,
LAST_MODIFIED_DTTIME,
POLICY_CARRIER
)
SELECT 
QUOTE_ID,
OPPORTUNITY_ID,
CUSTOMER_ID,
SOURCE_CUSTOMER_ID,
SOURCE_QUOTE_ID,
POLICY_STATUS,
PRODUCT_TYPE,
PRODUCT_CD,
POLICY_STATE,
SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',EFFECTIVE_DT),
SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',CREATE_DTTIME),
SAFE_CAST(PREMIUM AS FLOAT64),
SAFE_CAST(POLICY_TERM_MONTHS AS INT64),
POLICY_NUM,
SOURCE_SYSTEM,
SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S',LAST_MODIFIED_DTTIME),
POLICY_CARRIER
FROM `LANDING.WORK_SAM_QUOTES`
WHERE QUOTE_ID  NOT IN ( SELECT QUOTE_ID FROM `LANDING.SAM_QUOTES_LDG`) 