SELECT 
convert(char, quoteid, 120) as QUOTE_ID,
convert(char, opportunityid, 120) as OPPORTUNITY_ID,
convert(char, customerid, 120) as CUSTOMER_ID,
convert(char, source_customerid, 120) as SOURCE_CUSTOMER_ID,
convert(char, source_quoteid, 120) as SOURCE_QUOTE_ID,
convert(char, policy_status, 120) as POLICY_STATUS,
convert(char, product_type, 120) as PRODUCT_TYPE,
convert(char, product_code, 120) as PRODUCT_CD,
convert(char, policy_state, 120) as POLICY_STATE,
convert(char,effectivedate,120) as EFFECTIVE_DT,
convert(char,createdate, 120) as CREATE_DTTIME,
convert(char, premium, 120) as PREMIUM,
convert(char, policy_term_months, 120) as POLICY_TERM_MONTHS,
convert(char, policy_number, 120) as POLICY_NUM,
convert(char, source_system, 120) as SOURCE_SYSTEM,
convert(char, lastmodified, 120) as LAST_MODIFIED_DTTIME,
convert(char, policy_carrier, 120) as POLICY_CARRIER
from mrm_sam_prd.dbo.quotes
where lastmodified >= DATEADD(day,-v_incr_date,convert(date, 'v_inputdate'))

