SELECT 
TRIM(CONVERT(CHAR,noteid,120)) AS NOTE_ID,
TRIM(CONVERT(CHAR,tablename,120)) AS TABLE_NM,
TRIM(CONVERT(CHAR,primarykey,120)) AS PRIMARY_KEY,
TRIM(CONVERT(CHAR,userid,120)) AS USER_ID,
TRIM(CONVERT(CHAR,notetime,121)) AS NOTE_DTTIME,
notetext AS NOTE_TEXT 
FROM mrm_sam_prd.dbo.notes
WHERE CONVERT(date,notetime ) >=DATEADD(day,-1,convert(date, 'v_inputdate'))
AND CONVERT(date,notetime) < CONVERT(date,'v_inputdate');