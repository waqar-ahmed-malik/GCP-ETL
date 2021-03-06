INSERT INTO
  LANDING.DATA_LOAD_AUDIT ( 
    AUDIT_ID,
	JOB_ID,
	JOB_PROCESS_DTTIME,
	SOURCE_COUNT
)
SELECT
CASE
    WHEN AU.MAX_ID IS NULL THEN 0+(ROW_NUMBER() OVER ())
    ELSE AU.MAX_ID+(ROW_NUMBER() OVER ())
  END AS AUDIT_ID,   
CAST(JOB_ID AS INT64),
CURRENT_DATETIME(),
CAST(SOURCE_COUNT AS INT64)
FROM
  LANDING.WORK_MEMBERSHIP_AUDIT, 
(SELECT     MAX(AUDIT_ID) MAX_ID   FROM  LANDING.DATA_LOAD_AUDIT) AU