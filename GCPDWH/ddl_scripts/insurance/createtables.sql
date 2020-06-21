--WORK_INSURANCE_DIM 
CREATE OR REPLACE TABLE LANDING.WORK_INSURANCE_DIM (
INSURANCE_DIM_KEY  STRING ,
PRODUCT_CD  STRING ,
POLICY_NUM  STRING ,
AGMT_NUM  STRING ,
POLICY_PREFIX  STRING ,
POLICY_STATE  STRING ,
PRIOR_CARRIER  INT64 ,
NUMBER_OF_UNITS  INT64 ,
DATA_SOURCE  STRING ,
RISK_STATE  STRING ,
RISK_CITY  STRING ,
RISK_ZIP_CD  STRING ,
POLICY_INFORCE_IND  STRING ,
PAYMENT_PLAN_CD  STRING ,
TERM_EFFECTIVE_DT  DATE ,
TERM_EXPIRATION_DT  DATE ,
ORIGINAL_EFFECTIVE_DT  DATE ,
LAST_ENDORSEMENT_DT  DATE ,
GROUP_CD  STRING ,
HOME_POLICY_DWELLING_LIMIT  INT64 ,
HOME_POLICY_PROPERTY_LIMIT  INT64 ,
COUNT_OF_AUTO_CLAIMS  INT64 ,
COUNT_OF_PROPERTY_CLAIMS  INT64 ,
AUTO_BODILY_INJURY_LIMITS  STRING ,
AUTO_PROPERTY_DAMAGE_LIMITS  STRING ,
AUTO_UNINSURED_AND_UNDERINSURED_LIMITS  STRING ,
AUTO_MEDPAY_OR_PIP_LIMITS  STRING ,
ANNUAL_MILEAGE_COUNT  FLOAT64 ,
AUTO_COLLISION_DEDUCTIBLE  STRING ,
AUTO_COMPREHENSIVE_DEDUCTIBLE  STRING ,
AUTO_RENTAL  STRING ,
AUTO_GLASS  STRING ,
HOME_OTHER_STRUCTER_LIMIT  INT64 ,
HOME_LOSS_OF_USE_LIMIT  INT64 ,
HOME_PERSONAL_LIABILITY_EACH_OCCURRENCE  INT64 ,
HOME_MEDICAL_PAYMENT_TO_OTHERS  INT64 ,
HOME_DEDUCTIBLE  INT64 ,
REPL_AMT  INT64 ,
VIOLATION_DT  DATE ,
POLICY_STATUS  STRING ,
PRIOR_POLICY_NUM  STRING ,
VIOLATION_PTS  INT64 ,
LIFE_AGENT_EMPLOYEE_ID  STRING ,
LIFE_APPLICATION_DT  DATE ,
LIFE_PAY_UP_DT  DATE ,
LIFE_SETTLE_DT  DATE ,
LIFE_LIFE_POLICY_ISSUE_AGE  STRING ,
LIFE_ACCOUNT_VALUE_DT  DATE ,
LIFE_LOAN_INTEREST_DUE_DT  DATE ,
PRODUCT_TYPE  STRING ,
MD5_VALUE  BYTES )

--Insurance_DIM
CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.INSURANCE_DIM (
INSURANCE_DIM_KEY		STRING	,
PRODUCT_CD		STRING	,
POLICY_NUM		STRING	,
AGMT_NUM		STRING	,
POLICY_PREFIX		STRING	,
POLICY_STATE		STRING	,
PRIOR_CARRIER		INT64	,
NUM_OF_UNITS		INT64	,
DATA_SOURCE		STRING	,
RISK_STATE		STRING	,
RISK_CITY		STRING	,
RISK_ZIP_CD		STRING	,
POLICY_INFORCE_IND		STRING	,
PAYMENT_PLAN_CD		STRING	,
TERM_EFFECTIVE_DT		DATE	,
TERM_EXPIRATION_DT		DATE	,
ORIGINAL_EFFECTIVE_DT		DATE	,
LAST_ENDORSEMENT_DT		DATE	,
GROUP_CD		STRING	,
HOME_POLICY_DWELLING_LIMIT		INT64	,
HOME_POLICY_PROPERTY_LIMIT		INT64	,
COUNT_OF_AUTO_CLAIMS		INT64	,
COUNT_OF_PROPERTY_CLAIMS		INT64	,
AUTO_BODILY_INJURY_LIMITS		STRING	,
AUTO_PROPERTY_DAMAGE_LIMITS		STRING	,
AUTO_UNINSURED_AND_UNDERINSURED_LIMITS		STRING	,
AUTO_MEDPAY_OR_PIP_LIMITS		STRING	,
ANNUAL_MILEAGE_COUNT		FLOAT64	,
AUTO_COLLISION_DEDUCTIBLE		STRING	,
AUTO_COMPREHENSIVE_DEDUCTIBLE		STRING	,
AUTO_RENTAL		STRING	,
AUTO_GLASS		STRING	,
HOME_OTHER_STRUCTER_LIMIT		INT64	,
HOME_LOSS_OF_USE_LIMIT		INT64	,
HOME_PERSONAL_LIABILITY_EACH_OCCURRENCE		INT64	,
HOME_MEDICAL_PAYMENT_TO_OTHERS		INT64	,
HOME_DEDUCTIBLE		INT64	,
REPLACEMENT_AMT		INT64	,
POLICY_STATUS		STRING	,
PRIOR_POLICY_NUM		STRING	,
LIFE_AGENT_EMPLOYEE_ID		STRING	,
LIFE_APPLICATION_DT		DATE	,
LIFE_PAY_UP_DT		DATE	,
LIFE_SETTLE_DT		DATE	,
LIFE_LIFE_POLICY_ISSUE_AGE		STRING	,
LIFE_ACCOUNT_VALUE_DT		DATE	,
LIFE_LOAN_INTEREST_DUE_DT		DATE	,
TERMINATION_DT		DATE	,
VIOLATION_DT		DATE	,
VIOLATION_PTS		INT64	,
PRODUCT_TYPE		STRING	,
EFFECTIVE_FROM_DT		DATE	,
EFFECTIVE_TO_DT		DATE	,
ACTIVE_FLG		STRING	,
MD5_VALUE		BYTES	)

--INSURANCE_TRANSACTION_FACT

CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.INSURANCE_TRANSACTION_FACT(INSURANCE_TRANSACTION_ID	INT64	,	
BATCH_NM	STRING		,
SOURCE_PRODUCT_CD	STRING		,
POLICY_NUM	STRING		,
AGREEMENT_NUM	STRING	,	
MEMBER_NUM	INT64		,
CUSTOMER_MDM_KEY	INT64	,	
POLICY_EFFECTIVE_DT	DATE		,
POLICY_ENDORSEMENT_DT	DATE	,	
POLICY_PREFIX	STRING		,
AGENT_NM	STRING		,
EXPIRATION_DT	DATE	,	
PAS_AGENT_ID	STRING	,	
CEA_POLICY_NUM	STRING	,	
TRANSACTION_EFFECTIVE_DT	DATE,		
MAILING_ZIP_CD	STRING		,
RESIDENTIAL_ZIP_CD	STRING	,	
TRANSACTION_CD	STRING		,
PRODUCT_SUB_CATEGORY	STRING,		
SOURCE_SYSTEM	STRING		,
SOURCE_SYSTEM_TRANSACTION_ID	STRING,		
TRANSACTION_TYPE	STRING		,
TRANSACTION_DT	DATE		,
TRANSACTION_AMT	FLOAT64		,
SALES_REP_ID	STRING		,
SALES_REP_DO	STRING		,
COMPENSATION_EMPLOYEE_ID	STRING	,	
COMPENSATION_EMPLOYEE_FIRST_NM	STRING	,	
COMPENSATION_EMPLOYEE_LAST_NM	STRING	,	
SELLING_BRANCH_KEY	STRING		,
SELLING_BRANCH_NM	STRING		,
SELLING_BRANCH_CITY	STRING		,
SELLING_BRANCH_STATE	STRING	,	
REGION	STRING		,
OWNING_BRANCH_KEY	STRING,		
SALES_CHANNEL	STRING		,
AGENCY_NM	STRING		,
SELLING_AGENCY_NUM	STRING,		
AGENCY_OF_RECORD	STRING	,	
AGENT_OF_TRANSACTION	STRING,		
UNDER_WRITING_COMPANY	STRING	,	
PROCESS_TYPE	STRING		,
CANCEL_TYPE	STRING		,
PRE_STAGE_PROCESS_FLAG	STRING,		
PRE_STAGE_PROCESS_TM	STRING	,	
LINE_NUM	INT64		,
DUPLICATE_ORDER_NUM_FLAG	STRING,		
INCEPTION_DT	DATE		,
TERM_IND	INT64		,
RENEW_COUNT	INT64		,
NUMBER_OF_UNITS	STRING	,	
ERROR_FLAG	STRING		,
GA_CD	STRING		,
PRIOR_CARRIER	STRING,		
CAMPAIGN_SOURCE_CD	STRING,		
POLICY_STATE	STRING		,
RISK_STATE	STRING		,
RISK_CITY	STRING		,
RISK_ZIP_CD	STRING		,
PREMIUM_BEARING_ENDORSEMENTS	FLOAT64	,	
PAYMENT_PLAN_CD	STRING		,
POLICY_AGMT_NUM	STRING		)

--INSURANCE_CUSTOMER_DIM
CREATE OR REPLACE TABLE CUSTOMER_PRODUCT.INSURANCE_CUSTOMER_DIM (
CUSTOMER_MDM_KEY INT64,
AGMT_NUM STRING,
PRODUCT_CD STRING,
MEMBERSHIP_NUM INT64,
ROLE_STATUS_FLG STRING,
OWNER_CUSTOMER_MDM_KEY INT64,
STATUS STRING
);