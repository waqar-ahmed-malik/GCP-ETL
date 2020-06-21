SELECT  
LTRIM(RTRIM(CONVERT(CHAR, ASSOC_CD ))) AS ASSOC_CD,
LTRIM(RTRIM(CONVERT(CHAR, CLB_CD ))) AS CLB_CD,
LTRIM(RTRIM(CONVERT(CHAR, INV_CAT_CD ))) AS INV_CAT_CD,
LTRIM(RTRIM(CONVERT(CHAR, INV_CAT_DESC ))) AS INV_CAT_DESC,
LTRIM(RTRIM(CONVERT(CHAR, INV_CAT_GL_PREFIX ))) AS INV_CAT_GL_PREFIX,
LTRIM(RTRIM(CONVERT(CHAR, STATUS ))) AS STATUS,
LTRIM(RTRIM(CONVERT(CHAR, PROMO_OVERRIDE_CAT ))) AS PROMO_OVERRIDE_CAT
FROM pos_prd.dbo.inv_cat