select
LTRIM(RTRIM( ASSOC_CD)) AS ASSOC_CD,
LTRIM(RTRIM( CLB_CD)) AS CLB_CD,
LTRIM(RTRIM(CONVERT(CHAR, INV_SKU_ID) )) AS INV_SKU_ID,
LTRIM(RTRIM(CONVERT(CHAR, INV_ITM_ID) )) AS INV_ITM_ID,
LTRIM(RTRIM( INV_SKU)) AS INV_SKU,
LTRIM(RTRIM( STATUS)) AS STATUS,
LTRIM(RTRIM(CONVERT(CHAR, INV_SKU_EFF_DT, 121))) AS INV_SKU_EFF_DT,
LTRIM(RTRIM(CONVERT(CHAR, INV_SKU_EXP_DT, 121))) AS INV_SKU_EXP_DT,
LTRIM(RTRIM( INV_SKU_DESC)) AS INV_SKU_DESC,
LTRIM(RTRIM( INV_SKU_UOM)) AS INV_SKU_UOM,
LTRIM(RTRIM(CONVERT(CHAR, INV_SKU_WEIGHT) )) AS INV_SKU_WEIGHT,
LTRIM(RTRIM(CONVERT(CHAR, INV_SKU_SHP_CST) )) AS INV_SKU_SHP_CST,
LTRIM(RTRIM(CONVERT(CHAR, INV_SKU_HNDL_CST) )) AS INV_SKU_HNDL_CST,
LTRIM(RTRIM(CONVERT(CHAR, INV_SKU_SHP_CT) )) AS INV_SKU_SHP_CT,
LTRIM(RTRIM(CONVERT(CHAR, MEMBER_PRC) )) AS MEMBER_PRC,
LTRIM(RTRIM(CONVERT(CHAR, NONMEMBER_PRC) )) AS NONMEMBER_PRC,
LTRIM(RTRIM(CONVERT(CHAR, EMPLOYEE_PRC) )) AS EMPLOYEE_PRC,
LTRIM(RTRIM(CONVERT(CHAR, LIST_PRC) )) AS LIST_PRC,
LTRIM(RTRIM(CONVERT(CHAR, MEMBER_SAVINGS_AMT) )) AS MEMBER_SAVINGS_AMT,
LTRIM(RTRIM(CONVERT(CHAR, UNIT_COST) )) AS UNIT_COST,
LTRIM(RTRIM(CONVERT(CHAR, PRE_PUB_PRC) )) AS PRE_PUB_PRC,
LTRIM(RTRIM(CONVERT(CHAR, AFTER_PUB_PRC) )) AS AFTER_PUB_PRC,
LTRIM(RTRIM(CONVERT(CHAR, EDITION_YEAR) )) AS EDITION_YEAR,
LTRIM(RTRIM(CONVERT(CHAR, REVISION_MONTH) )) AS REVISION_MONTH,
LTRIM(RTRIM( ZERO_PRICE_IND)) AS ZERO_PRICE_IND,
LTRIM(RTRIM(CONVERT(CHAR, LAST_UPDATE, 121))) AS LAST_UPDATE,
LTRIM(RTRIM( USER_DATA_FLAG)) AS USER_DATA_FLAG,
LTRIM(RTRIM( USER_DATA_DESC)) AS USER_DATA_DESC,
LTRIM(RTRIM( DEFAULT_TO_CASH_ADV)) AS DEFAULT_TO_CASH_ADV,
LTRIM(RTRIM( FOREIGN_CURRENCY)) AS FOREIGN_CURRENCY,
LTRIM(RTRIM( PROMO_OVERRIDE_SKU)) AS PROMO_OVERRIDE_SKU,
LTRIM(RTRIM( TRAVEL_MONEY)) AS TRAVEL_MONEY,
LTRIM(RTRIM( WARRANTY)) AS WARRANTY,
LTRIM(RTRIM( OFAC_COMP_REQ)) AS OFAC_COMP_REQ,
LTRIM(RTRIM( LTV_CALC_FUNCT)) AS LTV_CALC_FUNCT,
LTRIM(RTRIM(CONVERT(CHAR, LTV_CALC_AMOUNT) )) AS LTV_CALC_AMOUNT
FROM [pos_prd].[dbo].[inv_sku]
