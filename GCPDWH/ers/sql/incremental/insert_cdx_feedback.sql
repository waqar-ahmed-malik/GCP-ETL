SELECT
TO_CHAR(translate(PROXY_ID,chr(10)||chr(11)||chr(13),' ')) as PROXY_ID,
TO_CHAR(translate(TRACKING_NBR,chr(10)||chr(11)||chr(13),' ')) as TRACKING_NBR,
TO_CHAR(translate(SOURCE_CLB,chr(10)||chr(11)||chr(13),' ')) as SOURCE_CLB,
TO_CHAR(translate(CLB_CD,chr(10)||chr(11)||chr(13),' ')) as CLB_CD,
TO_CHAR(translate(MBR_ID,chr(10)||chr(11)||chr(13),' ')) as MBR_ID,
TO_CHAR(SC_DT,'YYYY-MM-DD HH24:MI:SS') as SC_DT ,
TO_CHAR(translate(TROUBLE_CD,chr(10)||chr(11)||chr(13),' ')) as TROUBLE_CD,
TO_CHAR(translate(AUTH_CD,chr(10)||chr(11)||chr(13),' ')) as AUTH_CD,
TO_CHAR(FDBK_DATETIME,'YYYY-MM-DD HH24:MI:SS') as FDBK_DATETIME ,
TO_CHAR(translate(ASC_ID,chr(10)||chr(11)||chr(13),' ')) as ASC_ID,
TO_CHAR(translate(CHG_ENTITLEMENT,chr(10)||chr(11)||chr(13),' ')) as CHG_ENTITLEMENT,
TO_CHAR(translate(LONG_TOW_MILES,chr(10)||chr(11)||chr(13),' ')) as LONG_TOW_MILES,
TO_CHAR(translate(LONG_TOW_KILOS,chr(10)||chr(11)||chr(13),' ')) as LONG_TOW_KILOS,
TO_CHAR(translate(RENTAL_DAYS,chr(10)||chr(11)||chr(13),' ')) as RENTAL_DAYS,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_ERS_HIST.CDX_FEEDBACK