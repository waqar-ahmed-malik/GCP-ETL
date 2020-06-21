select
LTRIM(RTRIM(CONVERT(CHAR, PAY_TYPE_KY) )) AS PAY_TYPE_KY,
LTRIM(RTRIM( PAY_METH_CD)) AS PAY_METH_CD,
LTRIM(RTRIM(CONVERT(CHAR, GLACT_KY) )) AS GLACT_KY,
LTRIM(RTRIM( PAY_TYPE_CD)) AS PAY_TYPE_CD,
LTRIM(RTRIM( PAY_TYPE_DESC)) AS PAY_TYPE_DESC,
LTRIM(RTRIM( INACT_IND)) AS INACT_IND,
LTRIM(RTRIM(CONVERT(CHAR, PAY_TYPE_SORT_NR) )) AS PAY_TYPE_SORT_NR,
LTRIM(RTRIM( NO_JE_IND)) AS NO_JE_IND
FROM [m_prd].[dbo].[payment_type]