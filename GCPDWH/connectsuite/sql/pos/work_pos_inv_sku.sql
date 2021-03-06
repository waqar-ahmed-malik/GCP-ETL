SELECT
  CASE
    WHEN LTRIM(RTRIM(S.INV_SKU))<>LTRIM(RTRIM(I.INV_ITM_NR))
    THEN LTRIM(RTRIM(I.INV_ITM_NR))
    ELSE LTRIM(RTRIM(S.INV_SKU))
  END AS INV_SKU,
  S.STATUS,
  S.INV_SKU_DESC,
  S.MEMBER_PRC,
  S.NONMEMBER_PRC,
  S.LIST_PRC,
  S.LAST_UPDATE,
  LTRIM(RTRIM(S.INV_SKU))    AS ORG_INV_SKU,
  LTRIM(RTRIM(I.INV_ITM_NR)) AS INV_ITM_NR,
  INV_ITM_TAXABLE
FROM pos_prd.dbo.INV_SKU S,
  pos_prd.dbo.INV_ITM I
WHERE S.INV_ITM_ID=I.INV_ITM_ID
AND CONVERT(CHAR,S.LAST_UPDATE,127) >= 'max_date'
