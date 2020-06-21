MERGE `OPERATIONAL.AMS_CONTACT_NAME` AS TGT
USING `LANDING.WORK_AMS_CONTACT_NAME` AS SRC
ON TGT.UNIQUE_CONTACT_NAME = SAFE_CAST(SRC.UNIQCONTACTNAME AS INT64)

WHEN MATCHED
THEN UPDATE SET
TGT.UNIQUE_CONTACT_NAME = SAFE_CAST(SRC.UNIQCONTACTNAME AS INT64)
,TGT.UNIQUE_FIXED_CONTACT_NAME = SAFE_CAST(SRC.UNIQFIXEDCONTACTNAME AS INT64)
,TGT.UNIQUE_ENTITY = SAFE_CAST(SRC.UNIQENTITY AS INT64)
,TGT.LK_PREFIX = SAFE_CAST(SRC.LKPREFIX AS STRING)
,TGT.FULL_NAME = SAFE_CAST(SRC.FULLNAME AS STRING)
,TGT.FIRST_NAME = SAFE_CAST(SRC.FIRSTNAME AS STRING)
,TGT.MIDDLE_NAME = SAFE_CAST(SRC.MIDDLENAME AS STRING)
,TGT.LAST_NAME = SAFE_CAST(SRC.LASTNAME AS STRING)
,TGT.LK_SUFFIX = SAFE_CAST(SRC.LKSUFFIX AS STRING)
,TGT.DESC_OF = SAFE_CAST(SRC.DESCRIPTIONOF AS STRING)
,TGT.TITLE = SAFE_CAST(SRC.TITLE AS STRING)
,TGT.DEPARTMENT = SAFE_CAST(SRC.DEPARTMENT AS STRING)
,TGT.UNIQUE_CONTACT_ADDRESS_MAIN = SAFE_CAST(SRC.UNIQCONTACTADDRESSMAIN AS INT64)
,TGT.UNIQUE_CONTACT_ADDRESS_EMPLOYER = SAFE_CAST(SRC.UNIQCONTACTADDRESSEMPLOYER AS INT64)
,TGT.UNIQUE_CONTACT_NUM_MAIN = SAFE_CAST(SRC.UNIQCONTACTNUMBERMAIN AS INT64)
,TGT.UNIQUE_CONTACT_NUM_EMAIL_MAIN = SAFE_CAST(SRC.UNIQCONTACTNUMBEREMAILMAIN AS INT64)
,TGT.CONTACT_METHOD_CD = SAFE_CAST(SRC.CONTACTMETHODCODE AS STRING)
,TGT.INFORMAL_HEADING = SAFE_CAST(SRC.INFORMALHEADING AS STRING)
,TGT.FORMAL_HEADING = SAFE_CAST(SRC.FORMALHEADING AS STRING)
,TGT.BIRTH_DT = SAFE_CAST(SRC.BIRTHDATE AS DATETIME)
,TGT.GENDER_CD = SAFE_CAST(SRC.GENDERCODE AS STRING)
,TGT.SSN = SAFE_CAST(SRC.SSN AS STRING)
,TGT.MARITAL_STATUS_CD = SAFE_CAST(SRC.MARITALSTATUSCODE AS STRING)
,TGT.RELATION_TO_INSURED_CD = SAFE_CAST(SRC.RELATIONTOINSUREDCODE AS STRING)
,TGT.COMMENTS = SAFE_CAST(SRC.COMMENTS AS STRING)
,TGT.BILLING_DELIVERY_CD = SAFE_CAST(SRC.BILLINGDELIVERYCODE AS STRING)
,TGT.SERVICING_DELIVERY_CD = SAFE_CAST(SRC.SERVICINGDELIVERYCODE AS STRING)
,TGT.MARKETING_DELIVERY_CD = SAFE_CAST(SRC.MARKETINGDELIVERYCODE AS STRING)
,TGT.CATEGORY_CD = SAFE_CAST(SRC.CATEGORYCODE AS STRING)
,TGT.EMPLOYER_NAME = SAFE_CAST(SRC.EMPLOYERNAME AS STRING)
,TGT.LK_OCCUPATION = SAFE_CAST(SRC.LKOCCUPATION AS STRING)
,TGT.HIRED_DT = SAFE_CAST(SRC.HIREDDATE AS DATETIME)
,TGT.YEARS_EMPLOYED = SAFE_CAST(SRC.YEARSEMPLOYED AS INT64)
,TGT.YEARS_PRIOR_EMPLOYER = SAFE_CAST(SRC.YEARSPRIOREMPLOYER AS INT64)
,TGT.FEIN = SAFE_CAST(SRC.FEIN AS STRING)
,TGT.DUNS_NUM = SAFE_CAST(SRC.DUNSNUMBER AS STRING)
,TGT.CD_NAICS_CD = SAFE_CAST(SRC.CDNAICSCODE AS STRING)
,TGT.CD_SIC_CD = SAFE_CAST(SRC.CDSICCODE AS STRING)
,TGT.BUSINESS_TYPE_CD = SAFE_CAST(SRC.BUSINESSTYPECODE AS STRING)
,TGT.BUSINESS_TYPE_OTHER_DESC = SAFE_CAST(SRC.BUSINESSTYPEOTHERDESC AS STRING)
,TGT.NUM_MEMBERS_MANAGERS = SAFE_CAST(SRC.NUMBERMEMBERSMANAGERS AS INT64)
,TGT.BUSINESS_STARTED_DT = SAFE_CAST(SRC.BUSINESSSTARTEDDATE AS DATETIME)
,TGT.NATURE_OF_BUSINESS_CD = SAFE_CAST(SRC.NATUREOFBUSINESSCODE AS STRING)
,TGT.NATURE_OF_BUSINESS_OTHER_DESC = SAFE_CAST(SRC.NATUREOFBUSINESSOTHERDESC AS STRING)
,TGT.CREDIT_BUREAU_NAME_CD = SAFE_CAST(SRC.CREDITBUREAUNAMECODE AS STRING)
,TGT.CREDIT_BUREAU_NAME_OTHER_DESC = SAFE_CAST(SRC.CREDITBUREAUNAMEOTHERDESC AS STRING)
,TGT.CREDIT_BUREAU_ID_NUM = SAFE_CAST(SRC.CREDITBUREAUIDNUMBER AS STRING)
,TGT.DRIVER_LICENSE_NUM = SAFE_CAST(SRC.DRIVERLICENSENUMBER AS STRING)
,TGT.LICENSED_STATE = SAFE_CAST(SRC.LICENSEDSTATE AS STRING)
,TGT.LICENSED_DT = SAFE_CAST(SRC.LICENSEDDATE AS DATETIME)
,TGT.LICENSED_MA_DT = SAFE_CAST(SRC.LICENSEDMADATE AS DATETIME)
,TGT.DRIVER_TYPE_CD = SAFE_CAST(SRC.DRIVERTYPECODE AS STRING)
,TGT.GOOD_STUDENT_CD = SAFE_CAST(SRC.GOODSTUDENTCODE AS STRING)
,TGT.DRIVER_TRAINING_CD = SAFE_CAST(SRC.DRIVERTRAININGCODE AS STRING)
,TGT.ACCIDENT_PREVENTION_COURSE_DT = SAFE_CAST(SRC.ACCIDENTPREVENTIONCOURSEDATE AS DATETIME)
,TGT.COMMERCIAL_EXPERIENCE_BEGAN_DT = SAFE_CAST(SRC.COMMERCIALEXPERIENCEBEGANDATE AS DATETIME)
,TGT.MATCH_CLIENT_NAME_OF = SAFE_CAST(SRC.MATCHCLIENTNAMEOF AS INT64)
,TGT.INSERTED_BY_CD = SAFE_CAST(SRC.INSERTEDBYCODE AS STRING)
,TGT.INSERTED_DT = SAFE_CAST(SRC.INSERTEDDATE AS DATETIME)
,TGT.UPDATED_BY_CD = SAFE_CAST(SRC.UPDATEDBYCODE AS STRING)
,TGT.UPDATED_DT = SAFE_CAST(SRC.UPDATEDDATE AS DATETIME)
,TGT.FLGS = SAFE_CAST(SRC.FLAGS AS INT64)
,TGT.TS = SAFE_CAST(SRC.TS AS DATETIME)
,TGT.SIN = SAFE_CAST(SRC.SIN AS STRING)
,TGT.BUSINESS_NUM = SAFE_CAST(SRC.BUSINESSNUMBER AS STRING)
,TGT.BUSINESS_ID_NUM = SAFE_CAST(SRC.BUSINESSIDNUMBER AS STRING)
,TGT.IBC_CD = SAFE_CAST(SRC.IBCCODE AS STRING)
,TGT.UNIQUE_STATIC_LANGUAGE_RESOURCE_LANGUAGE = SAFE_CAST(SRC.UNIQSTATICLANGUAGERESOURCELANGUAGE AS INT64)
,TGT.NATIONAL_INSURANCE_NUM = SAFE_CAST(SRC.NATIONALINSURANCENUMBER AS STRING)
,TGT.ELTO_EXEMPT_STATUS_CD = SAFE_CAST(SRC.ELTOEXEMPTSTATUSCODE AS STRING)
,TGT.ELTO_EMPLOYER_REFERENCE_NUM = SAFE_CAST(SRC.ELTOEMPLOYERREFERENCENUMBER AS STRING)
,TGT.ELTO_EMPLOYER_TYPE_CD = SAFE_CAST(SRC.ELTOEMPLOYERTYPECODE AS STRING)
,TGT.VALUE_ADDED_TAX_NUM = SAFE_CAST(SRC.VALUEADDEDTAXNUMBER AS STRING)
,TGT.COMPANY_REGISTRATION_NUM = SAFE_CAST(SRC.COMPANYREGISTRATIONNUMBER AS STRING)
,TGT.REGISTERED_CHARITY_NUM = SAFE_CAST(SRC.REGISTEREDCHARITYNUMBER AS STRING)
,TGT.CONTACT_ID = SAFE_CAST(SRC.CONTACTID AS STRING)
,TGT.CD_COUNTRY_CD_SIC = SAFE_CAST(SRC.CDCOUNTRYCODESIC AS STRING)
,TGT.PERSON_ID = SAFE_CAST(SRC.PERSONID AS STRING)
,TGT.JOB_RUN_ID = SAFE_CAST(SRC.JOBRUNID AS STRING)
--,TGT.CREATE_DTTIME = CURRENT_DATETIME()
,TGT.UPDATE_DTTIME = CURRENT_DATETIME()
--,TGT.SOURCE_SYSTEM_CD = 'AMS'

WHEN NOT MATCHED
THEN INSERT ( --Target column names
UNIQUE_CONTACT_NAME
,UNIQUE_FIXED_CONTACT_NAME
,UNIQUE_ENTITY
,LK_PREFIX
,FULL_NAME
,FIRST_NAME
,MIDDLE_NAME
,LAST_NAME
,LK_SUFFIX
,DESC_OF
,TITLE
,DEPARTMENT
,UNIQUE_CONTACT_ADDRESS_MAIN
,UNIQUE_CONTACT_ADDRESS_EMPLOYER
,UNIQUE_CONTACT_NUM_MAIN
,UNIQUE_CONTACT_NUM_EMAIL_MAIN
,CONTACT_METHOD_CD
,INFORMAL_HEADING
,FORMAL_HEADING
,BIRTH_DT
,GENDER_CD
,SSN
,MARITAL_STATUS_CD
,RELATION_TO_INSURED_CD
,COMMENTS
,BILLING_DELIVERY_CD
,SERVICING_DELIVERY_CD
,MARKETING_DELIVERY_CD
,CATEGORY_CD
,EMPLOYER_NAME
,LK_OCCUPATION
,HIRED_DT
,YEARS_EMPLOYED
,YEARS_PRIOR_EMPLOYER
,FEIN
,DUNS_NUM
,CD_NAICS_CD
,CD_SIC_CD
,BUSINESS_TYPE_CD
,BUSINESS_TYPE_OTHER_DESC
,NUM_MEMBERS_MANAGERS
,BUSINESS_STARTED_DT
,NATURE_OF_BUSINESS_CD
,NATURE_OF_BUSINESS_OTHER_DESC
,CREDIT_BUREAU_NAME_CD
,CREDIT_BUREAU_NAME_OTHER_DESC
,CREDIT_BUREAU_ID_NUM
,DRIVER_LICENSE_NUM
,LICENSED_STATE
,LICENSED_DT
,LICENSED_MA_DT
,DRIVER_TYPE_CD
,GOOD_STUDENT_CD
,DRIVER_TRAINING_CD
,ACCIDENT_PREVENTION_COURSE_DT
,COMMERCIAL_EXPERIENCE_BEGAN_DT
,MATCH_CLIENT_NAME_OF
,INSERTED_BY_CD
,INSERTED_DT
,UPDATED_BY_CD
,UPDATED_DT
,FLGS
,TS
,SIN
,BUSINESS_NUM
,BUSINESS_ID_NUM
,IBC_CD
,UNIQUE_STATIC_LANGUAGE_RESOURCE_LANGUAGE
,NATIONAL_INSURANCE_NUM
,ELTO_EXEMPT_STATUS_CD
,ELTO_EMPLOYER_REFERENCE_NUM
,ELTO_EMPLOYER_TYPE_CD
,VALUE_ADDED_TAX_NUM
,COMPANY_REGISTRATION_NUM
,REGISTERED_CHARITY_NUM
,CONTACT_ID
,CD_COUNTRY_CD_SIC
,PERSON_ID
,JOB_RUN_ID
,CREATE_DTTIME
,UPDATE_DTTIME
,SOURCE_SYSTEM_CD
)

VALUES ( --Source values
SAFE_CAST(UNIQCONTACTNAME AS INT64)
,SAFE_CAST(UNIQFIXEDCONTACTNAME AS INT64)
,SAFE_CAST(UNIQENTITY AS INT64)
,SAFE_CAST(LKPREFIX AS STRING)
,SAFE_CAST(FULLNAME AS STRING)
,SAFE_CAST(FIRSTNAME AS STRING)
,SAFE_CAST(MIDDLENAME AS STRING)
,SAFE_CAST(LASTNAME AS STRING)
,SAFE_CAST(LKSUFFIX AS STRING)
,SAFE_CAST(DESCRIPTIONOF AS STRING)
,SAFE_CAST(TITLE AS STRING)
,SAFE_CAST(DEPARTMENT AS STRING)
,SAFE_CAST(UNIQCONTACTADDRESSMAIN AS INT64)
,SAFE_CAST(UNIQCONTACTADDRESSEMPLOYER AS INT64)
,SAFE_CAST(UNIQCONTACTNUMBERMAIN AS INT64)
,SAFE_CAST(UNIQCONTACTNUMBEREMAILMAIN AS INT64)
,SAFE_CAST(CONTACTMETHODCODE AS STRING)
,SAFE_CAST(INFORMALHEADING AS STRING)
,SAFE_CAST(FORMALHEADING AS STRING)
,SAFE_CAST(BIRTHDATE AS DATETIME)
,SAFE_CAST(GENDERCODE AS STRING)
,SAFE_CAST(SSN AS STRING)
,SAFE_CAST(MARITALSTATUSCODE AS STRING)
,SAFE_CAST(RELATIONTOINSUREDCODE AS STRING)
,SAFE_CAST(COMMENTS AS STRING)
,SAFE_CAST(BILLINGDELIVERYCODE AS STRING)
,SAFE_CAST(SERVICINGDELIVERYCODE AS STRING)
,SAFE_CAST(MARKETINGDELIVERYCODE AS STRING)
,SAFE_CAST(CATEGORYCODE AS STRING)
,SAFE_CAST(EMPLOYERNAME AS STRING)
,SAFE_CAST(LKOCCUPATION AS STRING)
,SAFE_CAST(HIREDDATE AS DATETIME)
,SAFE_CAST(YEARSEMPLOYED AS INT64)
,SAFE_CAST(YEARSPRIOREMPLOYER AS INT64)
,SAFE_CAST(FEIN AS STRING)
,SAFE_CAST(DUNSNUMBER AS STRING)
,SAFE_CAST(CDNAICSCODE AS STRING)
,SAFE_CAST(CDSICCODE AS STRING)
,SAFE_CAST(BUSINESSTYPECODE AS STRING)
,SAFE_CAST(BUSINESSTYPEOTHERDESC AS STRING)
,SAFE_CAST(NUMBERMEMBERSMANAGERS AS INT64)
,SAFE_CAST(BUSINESSSTARTEDDATE AS DATETIME)
,SAFE_CAST(NATUREOFBUSINESSCODE AS STRING)
,SAFE_CAST(NATUREOFBUSINESSOTHERDESC AS STRING)
,SAFE_CAST(CREDITBUREAUNAMECODE AS STRING)
,SAFE_CAST(CREDITBUREAUNAMEOTHERDESC AS STRING)
,SAFE_CAST(CREDITBUREAUIDNUMBER AS STRING)
,SAFE_CAST(DRIVERLICENSENUMBER AS STRING)
,SAFE_CAST(LICENSEDSTATE AS STRING)
,SAFE_CAST(LICENSEDDATE AS DATETIME)
,SAFE_CAST(LICENSEDMADATE AS DATETIME)
,SAFE_CAST(DRIVERTYPECODE AS STRING)
,SAFE_CAST(GOODSTUDENTCODE AS STRING)
,SAFE_CAST(DRIVERTRAININGCODE AS STRING)
,SAFE_CAST(ACCIDENTPREVENTIONCOURSEDATE AS DATETIME)
,SAFE_CAST(COMMERCIALEXPERIENCEBEGANDATE AS DATETIME)
,SAFE_CAST(MATCHCLIENTNAMEOF AS INT64)
,SAFE_CAST(INSERTEDBYCODE AS STRING)
,SAFE_CAST(INSERTEDDATE AS DATETIME)
,SAFE_CAST(UPDATEDBYCODE AS STRING)
,SAFE_CAST(UPDATEDDATE AS DATETIME)
,SAFE_CAST(FLAGS AS INT64)
,SAFE_CAST(TS AS DATETIME)
,SAFE_CAST(SIN AS STRING)
,SAFE_CAST(BUSINESSNUMBER AS STRING)
,SAFE_CAST(BUSINESSIDNUMBER AS STRING)
,SAFE_CAST(IBCCODE AS STRING)
,SAFE_CAST(UNIQSTATICLANGUAGERESOURCELANGUAGE AS INT64)
,SAFE_CAST(NATIONALINSURANCENUMBER AS STRING)
,SAFE_CAST(ELTOEXEMPTSTATUSCODE AS STRING)
,SAFE_CAST(ELTOEMPLOYERREFERENCENUMBER AS STRING)
,SAFE_CAST(ELTOEMPLOYERTYPECODE AS STRING)
,SAFE_CAST(VALUEADDEDTAXNUMBER AS STRING)
,SAFE_CAST(COMPANYREGISTRATIONNUMBER AS STRING)
,SAFE_CAST(REGISTEREDCHARITYNUMBER AS STRING)
,SAFE_CAST(CONTACTID AS STRING)
,SAFE_CAST(CDCOUNTRYCODESIC AS STRING)
,SAFE_CAST(PERSONID AS STRING)
,SAFE_CAST(JOBRUNID AS STRING)
,CURRENT_DATETIME()
,CURRENT_DATETIME()
,'AMS'
)
