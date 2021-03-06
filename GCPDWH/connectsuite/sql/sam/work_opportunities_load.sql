SELECT  
CONVERT(char,opp.OPPORTUNITYID) AS OPPORTUNITY_ID,
CONVERT(char,opp.CAMPAIGNID) AS CAMPAIGN_ID,
CONVERT(char,opp.CUSTOMERID) AS CUSTOMER_ID,
CONVERT(char,opp.ASSIGNEDWORKGROUPID) AS ASSIGNED_WORKGROUP_ID,
CONVERT(char,opp.ADDRESSID) AS ADDRESS_ID,
CONVERT(char(100),opp.NAME) AS  NAME,
CONVERT(char,opp.BRANCHCODE) AS BRANCH_CD,
CONVERT(char,opp.ORIGINCODE) AS ORIGIN_CD,
CONVERT(char,opp.ORIGINATINGCLUBCODE) AS ORIGINATING_CLUB_CD,
CONVERT(char,opp.TYPECODE) AS TYPE_CD,
CONVERT(char,opp.CATEGORYCODE) AS CATEGORY_CD,
CONVERT(char,opp.STATUSCODE) AS STATUS_CD,
CONVERT(char,opp.VENDORCODE) AS VENDOR_CD,
CONVERT(char,opp.OUTCOMECODE) AS OUTCOME_CD,
CONVERT(char,opp.CREATEDBYUSERID) AS CREATED_BY_USER_ID,
CONVERT(char,opp.ASSIGNEDTOUSERID) AS ASSIGNED_TO_USER_ID,
CONVERT(char,opp.CLOSEDBYUSERID) AS CLOSED_BY_USER_ID,
CONVERT (char ,opp.CREATETIME ,120) AS OPPORTUNITY_CREATE_DTTIME,
CONVERT (char ,opp.CLOSETIME,120) AS OPPORTUNITY_CLOSE_DTTIME,
CONVERT(char,opp.PRIORITY) AS PRIORITY,
CONVERT(char,opp.AMOUNT) AS OPPORTUNITY_AMT,
CONVERT (char ,opp.DUEDATE ,120) AS DUE_DT,
CONVERT(char,opp.STATUSDUECODE) AS STATUS_DUE_CD,
CONVERT(char,opp.STATUSDUEMET) AS STATUS_DUE_MET,
CONVERT(char,opp.EXTERNALSYSTEM) AS EXTERNAL_SYSTEM,
CONVERT(char,opp.EXTERNALSYSTEMID) AS EXTERNAL_SYSTEM_ID,
CONVERT(char,opp.MODIFIABLE_BY_EXTERNAL) AS MODIFIABLE_BY_EXTERNAL,
CONVERT(char,opp.ADDITIONALINFO) AS ADDITIONAL_INFO,
CONVERT(char,opp.CREATEMETHOD) AS CREATE_METHOD,
CONVERT(char,opp.REFERRER) AS REFERRER,
CONVERT(char,BUSINESSLINECODE) AS BUSINESSLINE_CD,
CONVERT(char,cust.MEMBERID ) AS MEMBER_ID,
cust.FIRSTNAME AS FIRST_NM,
cust.LASTNAME AS LAST_NM,
CONVERT(char,cust.MIDDLEINITIAL ) AS MIDDLE_INITIAL,
cust.EMAILADDRESS AS EMAIL_ADDRESS,
CONVERT(char,cust.GENDER) AS GENDER,
CONVERT (char ,cust.BIRTHDATE,120) AS BIRTH_DT
--from "v_database_name".dbo.OPPORTUNITIES opp
from mrm_sam_prd.dbo.OPPORTUNITIES opp
LEFT OUTER JOIN
--"v_database_name".dbo.CUSTOMERS cust 
mrm_sam_prd.dbo.CUSTOMERS cust 
ON opp.customerid=cust.customerid
where
opp.CREATETIME >= DATEADD(day,-v_incr_date,convert(date, 'v_inputdate'))
