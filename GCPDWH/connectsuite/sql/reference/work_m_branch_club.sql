select
LTRIM(RTRIM(CONVERT(CHAR, BRN_KY) )) AS BRN_KY,
LTRIM(RTRIM( CLUB_CD)) AS CLUB_CD
FROM [m_prd].[dbo].[branch_club]