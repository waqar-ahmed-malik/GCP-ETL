select 
LTRIM(RTRIM(CONVERT(CHAR, BRN_KY) )) AS BRN_KY,
LTRIM(RTRIM( BRN_CD)) AS BRN_CD,
LTRIM(RTRIM( BRN_NM)) AS BRN_NM,
LTRIM(RTRIM( BRN_BSC_AD)) AS BRN_BSC_AD,
LTRIM(RTRIM( BRN_SUPL_AD)) AS BRN_SUPL_AD,
LTRIM(RTRIM( BRN_CTY_NM)) AS BRN_CTY_NM,
LTRIM(RTRIM( BRN_ST_PROV_CD)) AS BRN_ST_PROV_CD,
LTRIM(RTRIM( BRN_ZIP_CD)) AS BRN_ZIP_CD,
LTRIM(RTRIM(CONVERT(CHAR, DIV_KY) )) AS DIV_KY,
LTRIM(RTRIM( BRN_TEL_NR)) AS BRN_TEL_NR,
LTRIM(RTRIM( BRN_CO_CD)) AS BRN_CO_CD,
LTRIM(RTRIM( BRN_BUDGT_CTR_CD)) AS BRN_BUDGT_CTR_CD,
TRANSLATE(TRANSLATE(TRANSLATE(LTRIM(RTRIM( BRN_SUBCO_CD)),CHAR(10),' '),CHAR(11),' '),CHAR(13),' ') AS BRN_SUBCO_CD,
LTRIM(RTRIM( BRN_AVATAR_DSN_NM_)) AS BRN_AVATAR_DSN_NM_,
LTRIM(RTRIM( BRN_RT_MRCHT_NB)) AS BRN_RT_MRCHT_NB,
LTRIM(RTRIM( BRN_MRCHT_NB)) AS BRN_MRCHT_NB
FROM [m_prd].[dbo].[branch]