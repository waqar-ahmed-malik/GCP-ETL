SELECT 
TRIM(CONVERT(CHAR,AP_KY))  AS AP_KEY,
TRIM(CONVERT(CHAR,AP_RECPNT_IN)) AS AP_RECIPIENT_IND,
TRIM(CONVERT(CHAR,AP_RECPNT_KY))  AS AP_RECIPIENT_KEY, 
TRIM(CONVERT(CHAR,AP_PROC_DT,121)) AS AP_PROCESSED_DTTIME,
TRIM(CONVERT(CHAR,AP_CRT_DT,121)) AS AP_CREDIT_DTTIME,
TRIM(CONVERT(CHAR,AP_STUB_DSC_TX)) AS AP_STUB_DESC_TX, 
TRIM(CONVERT(CHAR,GLACT_KY))AS GL_ACCOUNT_KEY,
TRIM(CONVERT(CHAR,AP_AT)) AS AP_AMT,
TRIM(CONVERT(CHAR,MPMT_KY)) AS MEMBERSHIP_PAYMENT_KEY,
TRIM(CONVERT(CHAR,AP_ATTN_AD)) AS AP_ALTERNATE_ADDRESS,
TRIM(CONVERT(CHAR(200),AP_BSC_AD)) AS AP_BASIC_ADDRESS,
TRIM(CONVERT(CHAR(200),AP_SUPL_AD)) AS AP_SUPPLEMENTARY_ADDRESS,
TRIM(CONVERT(CHAR,AP_CTY_NM)) AS AP_CITY_NM, 
TRIM(CONVERT(CHAR,AP_ST_PROV_CD)) AS AP_STATE_PROV_CD,
TRIM(CONVERT(CHAR,AP_ZIP_CD)) AS AP_ZIP_CD,
TRIM(CONVERT(CHAR(200),AP_PAYEE)) AS AP_PAYEE,
TRIM(CONVERT(CHAR,AP_FST_NM)) AS AP_FIRST_NM,
TRIM(CONVERT(CHAR,AP_LST_NM)) AS AP_LAST_NM,
TRIM(CONVERT(CHAR,AP_MID_INIT_NM)) AS AP_MIDDLE_INIT_NM, 
TRIM(CONVERT(CHAR(200),AP_NOTES)) AS AP_NOTES,
TRIM(CONVERT(CHAR,AP_CREATE_ID)) AS AP_CREATE_ID
FROM
 "v_database_name".dbo.ACCTS_PAYABLE
WHERE CONVERT(date,AP_CRT_DT) >= DATEADD(day,-1,convert(date, 'v_inputdate'))
AND CONVERT(date,AP_CRT_DT) < CONVERT(date,'v_inputdate');