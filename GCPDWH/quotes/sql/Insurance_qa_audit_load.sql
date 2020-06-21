INSERT INTO `CUSTOMER_PRODUCT.INSURANCE_QA_AUDIT` ( POLICY_NUM,
POLICY_EFFECTIVE_DT		,	
CHECK_DT			,
INITIAL_AGENT_ID	,		
AGMT_NUM			,
PRODUCT_CD			,
CUSTOMER_ID			,
OPPORTUNITY_ID		,	
BRANCH_CD			,
RATED_MEMBER_FLG	,	
DAY_100_VALID_MEMBER_CHECK,
DAY_1_VALID_MEMBER_CHECK,	
PREV_QUOTE_AGENT_ID	,		
PREV_QUOTE_DT			,
ISO_360			,
PREMIUM_BEARING_ENDORSEMENTS			,
AUTO_BODILY_INJURY_LIMITS			,
AUTO_PROPERTY_DAMAGE_LIMITS			,
AUTO_UNINSURED_AND_UNDERINSURED_LIMITS			,
AUTO_MEDPAY_OR_PIP_LIMITS			,
PRIOR_POLICY_NUM			,
VIOLATION_DT			,
ANNUAL_MILEAGE_COUNT			,
COUNT_OF_CLAIMS			,
HOME_POLICY_DWELLING_LIMIT			,
POLICY_PREFIX			,
SELLING_BRANCH_KEY		,	
SELLING_BRANCH_NM		,	
SELLING_BRANCH_CITY		,	
SELLING_BRANCH_STATE	,		
SALES_CHANNEL			,
AGENT_NM,
TRANSACTION_TYPE		,	
PRODUCT_TYPE			,
REGION			,
PREMIUM_BEARING_ENDORSEMENTS_FLG,
TRANSACTION_DT,
POLICY_STATUS,
PREMIUM_BEARING_ENDORSEMENTS_FLG_N)
SELECT 
FACT.POLICY_NUM AS POLICY_NUM,
FACT.TERM_EFFECTIVE_DT AS POLICY_EFFECTIVE_DT,
CURRENT_DATE() AS CHECK_DT,
FACT.COMPENSATION_EMPLOYEE_ID  AS INITIAL_AGENT_ID,
DIM.AGMT_NUM  AS  AGMT_NUM,
DIM.PRODUCT_CD	AS  PRODUCT_CD,
CAST(OPP.CUSTOMER_ID	 AS STRING) AS  CUSTOMER_ID,
CAST(OPP.OPPORTUNITY_ID	AS STRING) AS  OPPORTUNITY_ID,
OPP.BRANCH_CD	AS  BRANCH_CD,
CASE WHEN MEM.STATUS_CD IS NULL THEN 'N'
       WHEN MEM.STATUS_CD IN ('P', 'A') THEN 'Y'
       ELSE 'N'
  END AS RATED_MEMBER_FLG,  
  
CASE WHEN MEM.STATUS_CD IS NULL THEN 'N'
       WHEN MEM.STATUS_CD IN ('A')  AND DATE_ADD(FACT.TRANSACTION_DT,INTERVAL 100 DAY) BETWEEN MEM.EFFECTIVE_START_DT AND MEM.EFFECTIVE_END_DT THEN 'Y'
       ELSE 'N'
  END AS VALID_MEMBER_CHECK_ON_DAY100, 
  
   CASE WHEN MEM.STATUS_CD IS NULL THEN 'N'
       WHEN MEM.STATUS_CD IN ('A') AND  FACT.TRANSACTION_DT  BETWEEN MEM.EFFECTIVE_START_DT AND MEM.EFFECTIVE_END_DT THEN 'Y'
       ELSE 'N'
  END AS VALID_MEMBER_CHECK_ON_DAY1, 
QUOTE.COMPENSATION_EMPLOYEE_ID 	AS  PREV_QUOTE_AGENT_ID,
QUOTE.EFFECTIVE_DT	 AS  PREV_QUOTE_DT,
DIM.REPLACEMENT_AMT		AS  ISO_360,
SAFE_CAST(T1_E11.TRANSACTION_AMT AS INT64) AS  PREMIUM_BEARING_ENDORSEMENTS,
SAFE_CAST(DIM.AUTO_BODILY_INJURY_LIMITS AS INT64) AS AUTO_BODILY_INJURY_LIMITS,
SAFE_CAST(DIM.AUTO_PROPERTY_DAMAGE_LIMITS AS INT64) AS AUTO_PROPERTY_DAMAGE_LIMITS,
SAFE_CAST(DIM.AUTO_UNINSURED_AND_UNDERINSURED_LIMITS AS INT64) AS AUTO_UNINSURED_AND_UNDERINSURED_LIMITS,
SAFE_CAST(DIM.AUTO_MEDPAY_OR_PIP_LIMITS AS INT64) AS AUTO_MEDPAY_OR_PIP_LIMITS,
CASE WHEN (FACT.TRANSACTION_TYPE = 'New' AND FACT.TRANSACTION_SUB_TYPE = 'Issue') 
THEN DIM.PRIOR_POLICY_NUM
ELSE NULL 
END AS PRIOR_POLICY_NUM,
CASE WHEN DATE_DIFF(DIM.VIOLATION_DT,CURRENT_DATE(),YEAR)<7 
THEN DIM.VIOLATION_DT
ELSE NULL 
END AS VIOLATION_DT,
SAFE_CAST(DIM.ANNUAL_MILEAGE_COUNT  AS INT64) AS ANNUAL_MILEAGE_COUNT,
NULL AS COUNT_OF_PROPERTY_CLAIMS,
CASE 
WHEN HOME_POLICY_DWELLING_LIMIT >= DIM.REPLACEMENT_AMT/100 
THEN 'Compliant'
ELSE 'Non-Compliant'
END AS HOME_POLICY_DWELLING_LIMIT,
FACT.POLICY_PREFIX AS POLICY_PREFIX,
LOC.LOCATION_ID SELLING_BRANCH_KEY,
LOC.LOCATION_NM SELLING_BRANCH_NM,
LOC.CITY SELLING_BRANCH_CITY,
LOC.STATE_CD SELLING_BRANCH_STATE,
FACT.SALES_CHANNEL,
COALESCE(AGENT.LEGAL_NAME,PAS_AGENT.LEGAL_NAME,P_AGENT.LEGAL_NAME,FACT.SOURCE_AGENT_NM ) AGENT_NM,
FACT.TRANSACTION_TYPE,
DIM.PRODUCT_TYPE,
LOC.LOCATION_HIERARCHY_NM REGION,
CASE WHEN T1_E11.TRANSACTION_DT BETWEEN FACT.TRANSACTION_DT and DATE_ADD(FACT.TERM_EFFECTIVE_DT ,INTERVAL 30 DAY) AND T1_E11.TRANSACTION_DT IS NOT NULL 
          AND FACT.INSURANCE_TRANSACTION_ID <> T1_E11.INSURANCE_TRANSACTION_ID and FACT.TERM_EFFECTIVE_DT = T1_E11.TERM_EFFECTIVE_DT
THEN 'Y' ELSE  'N' END AS PREMIUM_BEARING_ENDORSEMENTS_FLG,
FACT.TRANSACTION_DT,
DIM.POLICY_STATUS,
CASE WHEN T1_E11.TRANSACTION_DT BETWEEN FACT.TRANSACTION_DT and DATE_ADD(FACT.TERM_EFFECTIVE_DT ,INTERVAL 30 DAY) AND T1_E11.TRANSACTION_DT IS NOT NULL 
          AND FACT.INSURANCE_TRANSACTION_ID <> T1_E11.INSURANCE_TRANSACTION_ID and FACT.TERM_EFFECTIVE_DT = T1_E11.TERM_EFFECTIVE_DT
THEN 'Y' ELSE  'N' END AS PREMIUM_BEARING_ENDORSEMENTS_FLG_N
FROM
(SELECT * FROM 
( SELECT * , ROW_NUMBER() OVER (PARTITION BY POLICY_NUM ORDER BY TRANSACTION_DT DESC) AS DUP_CHECK FROM `CUSTOMER_PRODUCT.INSURANCE_TRANSACTION_FACT` ) FACT
WHERE FACT.DUP_CHECK=1 ) FACT
LEFT OUTER JOIN (SELECT * FROM 
(SELECT INSURANCE_TRANSACTION_ID, PRODUCT_CD, SOURCE_SYSTEM, POLICY_NUM,TRANSACTION_DT,TRANSACTION_AMT,TRANSACTION_EFFECTIVE_DT,TERM_EFFECTIVE_DT,ROW_NUMBER() OVER(PARTITION BY POLICY_NUM ORDER BY TERM_EFFECTIVE_DT DESC) RN 
FROM `aaa-mwg-dwprod.CUSTOMER_PRODUCT.INSURANCE_TRANSACTION_FACT` 
WHERE UPPER(TRANSACTION_TYPE) = 'NEW' AND UPPER(TRANSACTION_SUB_TYPE) = 'ENDORSE' and TRANSACTION_AMT <> 0.0 AND EXTRACT(YEAR FROM  TRANSACTION_DT ) >= 2017 )T1_E1
WHERE T1_E1.RN=1) T1_E11 ON FACT.POLICY_NUM = T1_E11.POLICY_NUM AND FACT.PRODUCT_CD = T1_E11.PRODUCT_CD AND FACT.SOURCE_SYSTEM = T1_E11.SOURCE_SYSTEM  AND FACT.INSURANCE_TRANSACTION_ID <> T1_E11.INSURANCE_TRANSACTION_ID AND FACT.TERM_EFFECTIVE_DT = T1_E11.TERM_EFFECTIVE_DT
LEFT OUTER JOIN `CUSTOMER_PRODUCT.INSURANCE_DIM` DIM
ON FACT.POLICY_NUM = DIM.POLICY_NUM and DATA_SOURCE <> 'AAA_LIFE_INS_AGYPOLE' and ACTIVE_FLG = 'Y'
LEFT OUTER JOIN `CUSTOMER_PRODUCT.QUOTES_DIM` QUOTE
ON FACT.POLICY_NUM  =  SUBSTR(QUOTE.POLICY_NUM,5)   AND QUOTE.EFF_END_DT = '9999-12-31'
LEFT OUTER JOIN `CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_DIM` MEM
ON
MEM.MEMBER_NUM = QUOTE.MEMBERSHIP_NUM AND MEM.ACTIVE_FLG = 'Y'
LEFT OUTER JOIN `CUSTOMER_PRODUCT.SALES_OPPORTUNITIES` OPP
ON QUOTE.OPPORTUNITY_ID  =  OPP.OPPORTUNITY_ID 
LEFT OUTER JOIN
(SELECT * FROM (
SELECT * ,ROW_NUMBER() OVER (PARTITION BY AGENT_ID ORDER BY TERM_DATE DESC) RN FROM REFERENCE.WD_AGENT ) A
WHERE A.RN=1) AGENT
ON  LPAD(CAST(AGENT.AGENT_ID AS STRING),3,'0')= FACT.SALES_REP_ID AND TRIM(AGENT.TYPE)='3 Digit'
LEFT OUTER JOIN
(SELECT * FROM (
SELECT * ,ROW_NUMBER() OVER (PARTITION BY AGENT_ID ORDER BY TERM_DATE DESC) RN FROM REFERENCE.WD_AGENT ) A
WHERE A.RN=1  ) PAS_AGENT
ON TRIM(SAFE_CAST(PAS_AGENT.AGENT_ID AS STRING))= TRIM(FACT.SALES_REP_ID) AND TRIM(PAS_AGENT.TYPE)='PAS ID Primary' 
LEFT OUTER JOIN
(SELECT * FROM (
SELECT * ,ROW_NUMBER() OVER (PARTITION BY AGENT_ID ORDER BY TERM_DATE DESC) RN FROM REFERENCE.WD_AGENT ) A
WHERE A.RN=1  ) P_AGENT
ON TRIM(SAFE_CAST(P_AGENT.AGENT_ID AS STRING))= TRIM(FACT.PAS_AGENT_ID) AND TRIM(P_AGENT.TYPE)='PAS ID Primary' 
LEFT OUTER JOIN 
(SELECT * FROM 
(SELECT *,ROW_NUMBER() OVER (PARTITION BY LOCATION_NM ORDER BY LOCATION_START_DT DESC) RN FROM CUSTOMER_PRODUCT.LOCATION_DIM )LOC
WHERE LOC.RN=1) LOC
ON TRIM(LOC.LOCATION_NM )=TRIM(COALESCE(AGENT.LOCATION_NAME,PAS_AGENT.LOCATION_NAME,P_AGENT.LOCATION_NAME ))
WHERE 
UPPER(FACT.TRANSACTION_TYPE) = 'NEW' AND DIM.PRODUCT_TYPE IN ('Auto','Home' ) 
AND FACT.CEA_POLICY_NUM IS NULL