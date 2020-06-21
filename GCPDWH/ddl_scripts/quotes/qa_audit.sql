CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.INSURANCE_QA_AUDIT (POLICY_NUM	STRING		,
POLICY_EFFECTIVE_DT	DATE	,	
CHECK_DT	DATE		,
INITIAL_AGENT_ID	STRING,		
AGMT_NUM	STRING		,
PRODUCT_CD	STRING		,
CUSTOMER_ID	INT64		,
OPPORTUNITY_ID	INT64	,	
BRANCH_CD	STRING		,
RATED_MEMBER_FLG	STRING,		
PREV_QUOTE_AGENT_ID	INT64,		
PREV_QUOTE_DT	DATE		,
ISO_360	FLOAT64		,
PREMIUM_BEARING_ENDORSEMENTS	FLOAT64		,
AUTO_BODILY_INJURY_LIMITS	FLOAT64		,
AUTO_PROPERTY_DAMAGE_LIMITS	FLOAT64		,
AUTO_UNINSURED_AND_UNDERINSURED_LIMITS	FLOAT64		,
AUTO_MEDPAY_OR_PIP_LIMITS	FLOAT64		,
PRIOR_POLICY_NUM	STRING		,
VIOLATION_DT	DATE		,
ANNUAL_MILEAGE_COUNT	INT64		,
COUNT_OF_CLAIMS	INT64		,
HOME_POLICY_DWELLING_LIMIT	INT64		,
POLICY_PREFIX	STRING		,
SELLING_BRANCH_KEY	STRING	,	
SELLING_BRANCH_NM	STRING	,	
SELLING_BRANCH_CITY	STRING	,	
SELLING_BRANCH_STATE	STRING,		
SALES_CHANNEL	STRING		,
TRANSACTION_TYPE	STRING	,	
PRODUCT_TYPE	STRING		,
REGION	STRING		)