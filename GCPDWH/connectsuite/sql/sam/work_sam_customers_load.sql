SELECT CUSTOMER_ID,
MEMBER_ID,
FIRST_NM,
LAST_NM,
MIDDLE_INITIAL,
EMAIL_ADDRESS,
GENDER,
MARITAL_STATUS,
TRAVEL_AGENT_USER_ID,
INS_AGENT_USER_ID,
CLUB_CD,
ASSOC_CD,
MEMBERSHIP_STATUS,
COMPANY_NM,
AUTOTRAVEL_AGENT_USER_ID,
DONOT_PHONE,
DONOT_EMAIL,
DONOT_MAIL,
BIRTH_DT,
CREATE_TM,
DUP_OF_CUSTOMER_ID,
CREATE_BY_USER_ID,
CREATE_METHOD,
PRIVATE_BIRTH_DT,
PRIVATE_EMAIL_ADDRESS,
EMAIL_TYPE,
PREFERRED_CONTACT,
NAME_SUFFIX,
SALUTATION_CD,
AUDIT_DT,
HOME_PHONE_NUMBER,
CELL_PHONE_NUMBER,
WORK_PHONE_NUMBER
 FROM (
  SELECT
    CONVERT(char,
      cust.customerid ) AS CUSTOMER_ID,
    CONVERT(char,
      cust.memberid )AS MEMBER_ID,
    cust.firstname AS FIRST_NM,
    cust.lastname AS LAST_NM,
    cust.middleinitial AS MIDDLE_INITIAL,
    cust.emailaddress AS EMAIL_ADDRESS,
    cust.gender AS GENDER,
    CONVERT(char,
      cust.maritalstatus) AS MARITAL_STATUS,
    CONVERT(char,
      cust.travelagentuserid ) AS TRAVEL_AGENT_USER_ID,
    CONVERT(char,
      cust.insagentuserid ) AS INS_AGENT_USER_ID,
    CONVERT(char,
      cust.club_code ) AS CLUB_CD,
    CONVERT(char,
      cust.assoc_code ) AS ASSOC_CD,
    CONVERT(char,
      cust.membershipstatus ) AS MEMBERSHIP_STATUS,
    cust.companyName AS COMPANY_NM,
    CONVERT(char,
      cust.autotravelagentuserid ) AS AUTOTRAVEL_AGENT_USER_ID,
    CONVERT(char,
      cust.donotphone) AS DONOT_PHONE,
    CONVERT(char,
      cust.donotemail) AS DONOT_EMAIL,
    CONVERT(char,
      cust.donotmail) AS DONOT_MAIL,
    CONVERT(char,
      cust.birthdate,
      120) AS BIRTH_DT,
    CONVERT(char,
      cust.createtime,
      120) AS CREATE_TM,
    convert(char,
      cust.dup_of_customerid) AS DUP_OF_CUSTOMER_ID,
    CONVERT(char,
      cust.createdbyuserid ) AS CREATE_BY_USER_ID,
    CONVERT(char,
      cust.createmethod ) AS CREATE_METHOD,
    CONVERT(char,
      cust.private_birthdate ) AS PRIVATE_BIRTH_DT,
    CONVERT(char,
      cust.private_emailaddress) AS PRIVATE_EMAIL_ADDRESS,
    CONVERT(char,
      cust.emailtype ) AS EMAIL_TYPE,
    CONVERT(char,
      cust.preferred_contact ) AS PREFERRED_CONTACT,
    cust.namesuffix AS NAME_SUFFIX,
    CONVERT(char,
      cust.salutationcd ) AS SALUTATION_CD,
    CONVERT(char,
      custaudit.audit_dt,
      120 ) AS AUDIT_DT,
    CONVERT(char,
      HM.PHONE_NUMBER) AS HOME_PHONE_NUMBER,
    CONVERT(char,
      CL.PHONE_NUMBER) AS CELL_PHONE_NUMBER,
    CONVERT(char,
      WK.PHONE_NUMBER) AS WORK_PHONE_NUMBER,
    ROW_NUMBER() OVER (PARTITION BY cust.customerid ORDER BY custaudit.audit_dt DESC ) RN
FROM
--"v_database_name".dbo.customers cust , "v_database_name".dbo.customers_audit custaudit
mrm_sam_prd.dbo.customers cust INNER JOIN
 (select * from mrm_sam_prd.dbo.customers_audit custaudit WHERE 
custaudit.audit_dt >= CONVERT(DATE, GETDATE() - 2)) custaudit
on cust.customerid=custaudit.customerid
LEFT OUTER JOIN
(select customerid,TRIM(phnumber) as PHONE_NUMBER from mrm_sam_prd.dbo.phonenumbers
where TRIM(typecode)='HM') HM
on cust.customerid=HM.customerid
LEFT OUTER JOIN
(select customerid,TRIM(phnumber) as PHONE_NUMBER from mrm_sam_prd.dbo.phonenumbers
where TRIM(typecode)='CL') CL
on cust.customerid=CL.customerid
LEFT OUTER JOIN
(select customerid,TRIM(phnumber) as PHONE_NUMBER from mrm_sam_prd.dbo.phonenumbers
where TRIM(typecode)='WK') WK
on cust.customerid=WK.customerid
) a
WHERE a.RN=1
--and custaudit.audit_dt >= 'v_incr_date';
