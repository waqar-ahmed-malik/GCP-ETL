SELECT
TO_CHAR(translate(SVC_FACL_ID,chr(10)||chr(11)||chr(13),' ')) as SVC_FACL_ID,
TO_CHAR(translate(TRK_DRIVR_ID,chr(10)||chr(11)||chr(13),' ')) as TRK_DRIVR_ID,
TO_CHAR(translate(SC_PROB_CD,chr(10)||chr(11)||chr(13),' ')) as SC_PROB_CD,
TO_CHAR(CAPABLE_DT,'YYYY-MM-DD HH24:MI:SS') as CAPABLE_DT,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_CAD.svc_trk_driver_cap