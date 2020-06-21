SELECT 
DISTINCT 
  LTRIM(RTRIM(M.mbrs_id)) AS MEMBERSHIP_ID,
  LTRIM(RTRIM(CONVERT(CHAR,M.MBR_KY)))  AS MEMBER_KEY,
  LTRIM(RTRIM(CONVERT(CHAR,M.MBRS_KY)))  AS MEMBERSHIP_KEY,
  C.mcmt_creat_id         AS AR_PROCESSED_EMP_ID,
  LTRIM(RTRIM(CONVERT(CHAR, E.create_date, 121))) AS AR_ENROLL_DATE,
  LTRIM(RTRIM(CONVERT(CHAR, c.mcmt_creat_dt, 121)))  AS SOURCE_COMMENT_DT,
  LTRIM(RTRIM(CONVERT(CHAR, c.mcmt_ky)))  AS SOURCE_COMMENT_KEY,
  CASE
    WHEN LTRIM(RTRIM(C.mcmt_tx)) = 'Renew method changed to bill - auto renew credit card removed'
    THEN 'UN-ENROLL'
    ELSE 'ENROLL'
  END AS ENROLLMENT_TYPE
FROM
[m_prd].[dbo].[mbrship_e_payment_profile] E,
[m_prd].[dbo].[mbrship_comment] C,
[m_prd].[dbo].[mbr] M 
WHERE C.mbrs_ky     = E.mbrs_ky
AND C.mbrs_ky       = M.mbrs_ky
AND M.MBR_ASSOC_ID  = 1
AND LTRIM(RTRIM(C.mcmt_tx))      IN ('Renewal method changed to Auto-Renew', 'Added member: 1-AUTO RENEW', 'Renew method changed to bill - auto renew credit card removed')
AND CONVERT(DATE,C.mcmt_creat_dt) >= CONVERT(date,'v_inputdate') AND CONVERT(DATE,C.mcmt_creat_dt) < DATEADD(day,1,convert(date, 'v_inputdate'))
