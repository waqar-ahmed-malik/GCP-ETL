select
LTRIM(RTRIM(CONVERT(CHAR, SAGT_KY) )) AS SAGT_KY,
LTRIM(RTRIM( SAGT_ID)) AS SAGT_ID,
LTRIM(RTRIM( SAGT_FST_NM)) AS SAGT_FST_NM,
LTRIM(RTRIM( SAGT_LST_NM)) AS SAGT_LST_NM,
LTRIM(RTRIM( SAGT_COMP_METH_CD)) AS SAGT_COMP_METH_CD,
LTRIM(RTRIM(CONVERT(CHAR, SAGT_MTD_COMP_AM) )) AS SAGT_MTD_COMP_AM,
LTRIM(RTRIM(CONVERT(CHAR, SAGT_PTD_COMP_AM) )) AS SAGT_PTD_COMP_AM,
LTRIM(RTRIM(CONVERT(CHAR, SAGT_YTD_COMP_AM) )) AS SAGT_YTD_COMP_AM,
LTRIM(RTRIM( SAGT_SSN_NR)) AS SAGT_SSN_NR,
LTRIM(RTRIM( SAGT_COMM_IN)) AS SAGT_COMM_IN,
LTRIM(RTRIM(CONVERT(CHAR, SAGT_HIRE_DT, 121))) AS SAGT_HIRE_DT,
LTRIM(RTRIM(CONVERT(CHAR, SAGT_DRAW_AMT) )) AS SAGT_DRAW_AMT,
LTRIM(RTRIM( SAGT_COST_CNTR)) AS SAGT_COST_CNTR,
LTRIM(RTRIM(CONVERT(CHAR, BRN_KY) )) AS BRN_KY,
LTRIM(RTRIM( SAGT_TYP_CD)) AS SAGT_TYP_CD,
LTRIM(RTRIM( INACTIVE_IND)) AS INACTIVE_IND,
LTRIM(RTRIM( EMPLOYEE_ID)) AS EMPLOYEE_ID,
LTRIM(RTRIM( SAGT_UPDT_ID)) AS SAGT_UPDT_ID,
LTRIM(RTRIM(CONVERT(CHAR, SAGT_UPDT_DT, 121))) AS SAGT_UPDT_DT
FROM [m_prd].[dbo].[sales_agent]