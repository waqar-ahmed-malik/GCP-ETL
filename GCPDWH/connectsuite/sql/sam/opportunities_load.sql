MERGE OPERATIONAL.CONNECTSUITE_SALES_OPPORTUNITIES A  
USING 
(SELECT * FROM 
(SELECT * , ROW_NUMBER() OVER (PARTITION BY TRIM(OPPORTUNITY_ID) ) RN FROM LANDING.WORK_SALES_OPPORTUNITIES ) B
WHERE B.RN=1) B
ON A.OPPORTUNITY_ID = CAST(B.OPPORTUNITY_ID AS INT64)
WHEN MATCHED THEN
UPDATE SET
A.CAMPAIGN_ID= CAST(B.CAMPAIGN_ID AS INT64),
A.CUSTOMER_ID= CAST(B.CUSTOMER_ID AS INT64),
A.ASSIGNED_WORKGROUP_ID= CAST(B.ASSIGNED_WORKGROUP_ID AS INT64),
A.ADDRESS_ID= CAST(B.ADDRESS_ID AS INT64),
A.BRANCH_CD= B.BRANCH_CD,
A.ORIGIN_CD= B.ORIGIN_CD,
A.ORIGINATING_CLUB_CD= B.ORIGINATING_CLUB_CD,
A.TYPE_CD= B.TYPE_CD,
A.CATEGORY_CD= B.CATEGORY_CD,
A.STATUS_CD= TRIM(B.STATUS_CD),
A.VENDOR_CD= B.VENDOR_CD,
A.OUTCOME_CD= B.OUTCOME_CD,
A.CREATED_BY_USER_ID= B.CREATED_BY_USER_ID,
A.ASSIGNED_TO_USER_ID= B.ASSIGNED_TO_USER_ID,
A.CLOSED_BY_USER_ID= B.CLOSED_BY_USER_ID,
A.OPPORTUNITY_CREATE_DT= PARSE_DATETIME('%F%T', trim(B.OPPORTUNITY_CREATE_DTTIME)),
A.OPPORTUNITY_CLOSE_DT= PARSE_DATETIME('%F%T',trim(B.OPPORTUNITY_CLOSE_DTTIME)),
A.PRIORITY= B.PRIORITY,
A.OPPORTUNITY_AMT= CAST(B.OPPORTUNITY_AMT AS FLOAT64),
A.DUE_DT= EXTRACT(DATE FROM PARSE_DATETIME('%F %T',trim(B.DUE_DT))),
A.STATUS_DUE_CD= B.STATUS_DUE_CD,
A.STATUS_DUE_MET= B.STATUS_DUE_MET,
A.EXTERNAL_SYSTEM= B.EXTERNAL_SYSTEM,
A.EXTERNAL_SYSTEM_ID= B.EXTERNAL_SYSTEM_ID,
A.MODIFIABLE_BY_EXTERNAL= CAST(B.MODIFIABLE_BY_EXTERNAL AS STRING),
A.ADDITIONAL_INFO= B.ADDITIONAL_INFO,
A.CREATE_METHOD= B.CREATE_METHOD,
A.REFERRER= B.REFERRER,
A.BUSINESSLINE_CD= B.BUSINESSLINE_CD,
A.MEMBER_ID= B.MEMBER_ID,
A.UPDATE_DTTIME = CURRENT_DATETIME()
WHEN NOT MATCHED THEN
  INSERT (OPPORTUNITY_ID ,
CAMPAIGN_ID,
CUSTOMER_ID,
ASSIGNED_WORKGROUP_ID,
ADDRESS_ID,
BRANCH_CD,
ORIGIN_CD,
ORIGINATING_CLUB_CD,
TYPE_CD,
CATEGORY_CD,
STATUS_CD,
VENDOR_CD,
OUTCOME_CD,
CREATED_BY_USER_ID,
ASSIGNED_TO_USER_ID,
CLOSED_BY_USER_ID,
OPPORTUNITY_CREATE_DT,
OPPORTUNITY_CLOSE_DT,
PRIORITY,
OPPORTUNITY_AMT,
DUE_DT,
STATUS_DUE_CD,
STATUS_DUE_MET,
EXTERNAL_SYSTEM,
EXTERNAL_SYSTEM_ID,
MODIFIABLE_BY_EXTERNAL,
ADDITIONAL_INFO,
CREATE_METHOD,
REFERRER,
BUSINESSLINE_CD,
MEMBER_ID,
JOB_RUN_ID,
SOURCE_SYSTEM_CD,
CREATE_DTTIME,
UPDATE_DTTIME
) VALUES(CAST(OPPORTUNITY_ID AS INT64),
CAST(CAMPAIGN_ID AS INT64),
CAST(CUSTOMER_ID AS INT64),
CAST(ASSIGNED_WORKGROUP_ID AS INT64),
CAST(ADDRESS_ID AS INT64),
BRANCH_CD,
ORIGIN_CD,
ORIGINATING_CLUB_CD,
TYPE_CD,
CATEGORY_CD,
TRIM(STATUS_CD),
VENDOR_CD,
OUTCOME_CD,
CREATED_BY_USER_ID,
ASSIGNED_TO_USER_ID,
CLOSED_BY_USER_ID,
PARSE_DATETIME('%F%T',trim(OPPORTUNITY_CREATE_DTTIME)),
PARSE_DATETIME('%F%T',trim(OPPORTUNITY_CLOSE_DTTIME)),
PRIORITY,
CAST(OPPORTUNITY_AMT AS FLOAT64),
EXTRACT(DATE FROM PARSE_DATETIME('%F %T',DUE_DT)),
STATUS_DUE_CD,
STATUS_DUE_MET,
EXTERNAL_SYSTEM,
EXTERNAL_SYSTEM_ID,
CAST(MODIFIABLE_BY_EXTERNAL AS STRING),
ADDITIONAL_INFO,
CREATE_METHOD,
REFERRER,
BUSINESSLINE_CD,
MEMBER_ID,
CAST('jobrunid'AS INT64),
'SAM',
CURRENT_DATETIME(),
CURRENT_DATETIME()
);