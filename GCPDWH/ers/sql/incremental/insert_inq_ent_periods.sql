SELECT
TO_CHAR(translate(CLB_CD,chr(10)||chr(11)||chr(13),' ')) as CLB_CD,
TO_CHAR(translate(MBR_ID,chr(10)||chr(11)||chr(13),' ')) as MBR_ID,
TO_CHAR(translate(ASSOC_MBR_ID,chr(10)||chr(11)||chr(13),' ')) as ASSOC_MBR_ID,
TO_CHAR(translate(SEQ_NUM,chr(10)||chr(11)||chr(13),' ')) as SEQ_NUM,
TO_CHAR(ENT_START_DATE,'YYYY-MM-DD') as ENT_START_DATE ,
TO_CHAR(ENT_END_DATE,'YYYY-MM-DD') as ENT_END_DATE ,
TO_CHAR(translate(PERIOD_STATUS,chr(10)||chr(11)||chr(13),' ')) as PERIOD_STATUS,
TO_CHAR(translate(MBRSHP_TYPE,chr(10)||chr(11)||chr(13),' ')) as MBRSHP_TYPE,
TO_CHAR(translate(YTD,chr(10)||chr(11)||chr(13),' ')) as YTD,
TO_CHAR(translate(ENTITLEMENTS,chr(10)||chr(11)||chr(13),' ')) as ENTITLEMENTS,
TO_CHAR(translate(ENT_DELTA,chr(10)||chr(11)||chr(13),' ')) as ENT_DELTA,
TO_CHAR(translate(GRACE_USAGE,chr(10)||chr(11)||chr(13),' ')) as GRACE_USAGE,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_INQ.ENTITLEMENT_PERIODS 
WHERE ENT_START_DATE >= TO_DATE('incr_date_ent_periods','yyyy-mm-dd') 