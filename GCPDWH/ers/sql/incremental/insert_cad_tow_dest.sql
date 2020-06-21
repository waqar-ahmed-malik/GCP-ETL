SELECT
TO_CHAR(SC_DT,'YYYY-MM-DD HH24:MI:SS') as SC_DT ,
TO_CHAR(translate(SC_ID,chr(10)||chr(11)||chr(13),' ')) as SC_ID,
TO_CHAR(translate(TOW_DEST_SEQ_ID,chr(10)||chr(11)||chr(13),' ')) as TOW_DEST_SEQ_ID,
TO_CHAR(translate(TOW_DST_UPRS_TX,chr(10)||chr(11)||chr(13),' ')) as TOW_DST_UPRS_TX,
TO_CHAR(LAST_UPDATE,'YYYY-MM-DD') as LAST_UPDATE ,
TO_CHAR(translate(TOW_UNSEEN_CHNG,chr(10)||chr(11)||chr(13),' ')) as TOW_UNSEEN_CHNG,
TO_CHAR(translate(EMPLE_ID,chr(10)||chr(11)||chr(13),' ')) as EMPLE_ID,
TO_CHAR(translate(TOW_DEST_LATITUDE,chr(10)||chr(11)||chr(13),' ')) as TOW_DEST_LATITUDE,
TO_CHAR(translate(TOW_DEST_LONGITUDE,chr(10)||chr(11)||chr(13),' ')) as TOW_DEST_LONGITUDE,
TO_CHAR(translate(TOW_DEST_ID,chr(10)||chr(11)||chr(13),' ')) as TOW_DEST_ID,
TO_CHAR(translate(TOW_DEST_GRID,chr(10)||chr(11)||chr(13),' ')) as TOW_DEST_GRID,
TO_CHAR(translate(TD_VERIFIED_LOC,chr(10)||chr(11)||chr(13),' ')) as TD_VERIFIED_LOC,
TO_CHAR(translate(TOW_DEST_ADDITIONAL_INFO,chr(10)||chr(11)||chr(13),' ')) as TOW_DEST_ADDITIONAL_INFO,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_CAD.TOW_DEST
WHERE LAST_UPDATE  >=  TO_DATE('incr_date_tow_dest','yyyy-mm-dd hh24:mi:ss')