MERGE OPERATIONAL.MWG_STG_INSURANCE_QUOTE AS TGT
USING LANDING.WORK_MWG_QUOTE AS SRC
ON TGT.POLICY_ENTRY_DT = PARSE_DATE('%d %b %y',SRC.POLICY_ENTRY_DT ) and
TGT.QUOTE_POLICY_NUM = SRC.QUOTE_POLICY_NUM 
WHEN MATCHED
THEN UPDATE SET
TGT.QUOTE_NUM = CASE WHEN SUBSTR(SRC.QUOTE_POLICY_NUM, 1,1) != 'Q' THEN CONCAT('Q', SRC.QUOTE_POLICY_NUM) ELSE SRC.QUOTE_POLICY_NUM END,
TGT.STATE = SRC.STATE,
TGT.LINE_OF_BUSINESS = SRC.LINE_OF_BUSINESS,
TGT.PROD_DESC = SRC.PROD_DESC,
TGT.LOCATION_ID_SELLING = SAFE_CAST(SRC.LOCATION_ID_SELLING AS INT64),
TGT.AGENT_CD_SELLING = SAFE_CAST(SRC.AGENT_CD_SELLING AS INT64),
TGT.LOCATION_ID_OWNING = SAFE_CAST(SRC.LOCATION_ID_OWNING AS INT64),
TGT.AGENT_CD_OWNING = SAFE_CAST(SRC.AGENT_CD_OWNING AS INT64),
TGT.POLICY_ENTRY_DT = PARSE_DATE('%d %b %y', SRC.POLICY_ENTRY_DT ),
TGT.TERM_EFFECTIVE_DT = PARSE_DATE('%d %h %y',SRC.TERM_EFFECTIVE_DT ),
TGT.BOUND = SAFE_CAST(SRC.BOUND AS INT64),
TGT.PREMIUM = SAFE_CAST(SRC.PREMIUM AS FLOAT64),
TGT.ONLINE_FLG = SRC.ONLINE_FLG,
TGT.REWRITE_FLG = SRC.REWRITE_FLG,
TGT.TERM = SAFE_CAST(SRC.TERM AS INT64),
TGT.TIER = SRC.TIER,
TGT.TIER_GROUP = SRC.TIER_GROUP,
TGT.QUOTE_POLICY_NUM = SRC.QUOTE_POLICY_NUM,
TGT.QUOTE_UNIQUE_KEY = SAFE_CAST(SRC.QUOTE_UNIQUE_KEY as INT64),
TGT.MEMBER_NUM = SRC.MEMBER_NUM,
TGT.SOURCE_SYSTEM_CD = 'MWGQuote',
TGT.CREATE_DTTIME = CURRENT_DATE(),
TGT.CREATE_BY = 'QUOTES_JOB'
WHEN NOT MATCHED THEN INSERT 
(
QUOTE_NUM,
STATE,
LINE_OF_BUSINESS,
PROD_DESC,
LOCATION_ID_SELLING,
AGENT_CD_SELLING,
LOCATION_ID_OWNING,
AGENT_CD_OWNING,
POLICY_ENTRY_DT,
TERM_EFFECTIVE_DT,
BOUND,
PREMIUM,
ONLINE_FLG,
REWRITE_FLG,
TERM,
TIER,
TIER_GROUP,
QUOTE_POLICY_NUM,
QUOTE_UNIQUE_KEY,
MEMBER_NUM,
SOURCE_SYSTEM_CD,
CREATE_DTTIME,
CREATE_BY
)

VALUES

(
CASE WHEN SUBSTR(SRC.QUOTE_POLICY_NUM, 1,1) != 'Q' THEN CONCAT('Q', SRC.QUOTE_POLICY_NUM) ELSE SRC.QUOTE_POLICY_NUM END,
SRC.STATE,
SRC.LINE_OF_BUSINESS,
SRC.PROD_DESC,
SAFE_CAST(SRC.LOCATION_ID_SELLING AS INT64),
SAFE_CAST(SRC.AGENT_CD_SELLING AS INT64),
SAFE_CAST(SRC.LOCATION_ID_OWNING AS INT64),
SAFE_CAST(SRC.AGENT_CD_OWNING AS INT64),
PARSE_DATE('%d %b %y', SRC.POLICY_ENTRY_DT ),
PARSE_DATE('%d %h %y',SRC.TERM_EFFECTIVE_DT ),
SAFE_CAST(SRC.BOUND AS INT64),
SAFE_CAST(SRC.PREMIUM AS FLOAT64),
SRC.ONLINE_FLG,
SRC.REWRITE_FLG,
SAFE_CAST(SRC.TERM AS INT64),
SRC.TIER,
SRC.TIER_GROUP,
SRC.QUOTE_POLICY_NUM,
SAFE_CAST(SRC.QUOTE_UNIQUE_KEY as INT64),
SRC.MEMBER_NUM,
'MWGQuote',
CURRENT_DATE(),
'QUOTES_JOB'
)