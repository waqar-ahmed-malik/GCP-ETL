select
LTRIM(RTRIM( ASSOC_CD)) AS ASSOC_CD,
LTRIM(RTRIM( CLUB_CD)) AS CLUB_CD,
LTRIM(RTRIM( OFC_ID)) AS OFC_ID,
LTRIM(RTRIM(CONVERT(CHAR, CNTY_TAX_PCT) )) AS CNTY_TAX_PCT,
LTRIM(RTRIM( CNTY_TAX_DSC)) AS CNTY_TAX_DSC,
LTRIM(RTRIM(CONVERT(CHAR, LCL_TAX_PCT) )) AS LCL_TAX_PCT,
LTRIM(RTRIM( LCL_TAX_DSC)) AS LCL_TAX_DSC,
LTRIM(RTRIM(CONVERT(CHAR, OTH_TAX_PCT) )) AS OTH_TAX_PCT,
LTRIM(RTRIM( OTH_TAX_DSC)) AS OTH_TAX_DSC,
LTRIM(RTRIM(CONVERT(CHAR, ST_TAX_PCT) )) AS ST_TAX_PCT,
LTRIM(RTRIM( ST_TAX_DSC)) AS ST_TAX_DSC,
LTRIM(RTRIM( TAX_GL)) AS TAX_GL
FROM [pos_prd].[dbo].[clb_ofc_sls_tax]