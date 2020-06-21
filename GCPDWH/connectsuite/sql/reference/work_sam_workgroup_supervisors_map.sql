select
LTRIM(RTRIM(CONVERT(CHAR, WORKGROUPID) )) AS WORKGROUPID,
LTRIM(RTRIM( USERID)) AS USERID
FROM [mrm_sam_prd].[dbo].[workgroup_supervisors_map]