SELECT
TO_CHAR(SC_DT,'YYYY-MM-DD HH24:MI:SS') as SC_DT ,
TO_CHAR(translate(SC_ID,chr(10)||chr(11)||chr(13),' ')) as SC_ID,
TO_CHAR(translate(EMPLE_ID,chr(10)||chr(11)||chr(13),' ')) as EMPLE_ID,
TO_CHAR(translate(EMPLE_ROLE,chr(10)||chr(11)||chr(13),' ')) as EMPLE_ROLE,
TO_CHAR(translate(SC_COMM_ID,chr(10)||chr(11)||chr(13),' ')) as SC_COMM_ID,
TO_CHAR(SC_COMM_ADD_TM,'YYYY-MM-DD') as SC_COMM_ADD_TM ,
TO_CHAR(translate(SC_COMM_TX,chr(10)||chr(11)||chr(13),' ')) as SC_COMM_TX,
TO_CHAR(translate(EMPLE_LOCATION,chr(10)||chr(11)||chr(13),' ')) as EMPLE_LOCATION,
TO_CHAR(translate(SC_COMM_STATUS,chr(10)||chr(11)||chr(13),' ')) as SC_COMM_STATUS,
TO_CHAR(translate(SC_COMM_TYPE,chr(10)||chr(11)||chr(13),' ')) as SC_COMM_TYPE,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD	
FROM NCA_CAD.SC_CALL_COMMENT
WHERE SC_COMM_ADD_TM  >=  TO_DATE('incr_date_sc_call','yyyy-mm-dd hh24:mi:ss')


