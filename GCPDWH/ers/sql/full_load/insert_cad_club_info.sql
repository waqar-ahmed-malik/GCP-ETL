SELECT 
TO_CHAR(translate(CLB_CD,chr(10)||chr(11)||chr(13),' ')) as CLB_CD,
TO_CHAR(translate(CL_NM,chr(10)||chr(11)||chr(13),' ')) as CL_NM,
TO_CHAR(translate(CL_PLS_MBR_IN,chr(10)||chr(11)||chr(13),' ')) as CL_PLS_MBR_IN,
TO_CHAR(translate(MBR_VERF_TEL_NR,chr(10)||chr(11)||chr(13),' ')) as MBR_VERF_TEL_NR,
TO_CHAR(translate(MBR_VERF_TEL_X_NR,chr(10)||chr(11)||chr(13),' ')) as MBR_VERF_TEL_X_NR,
TO_CHAR(translate(DIVISION_FLG,chr(10)||chr(11)||chr(13),' ')) as DIVISION_FLG,
TO_CHAR(translate(PARSE_TYPE,chr(10)||chr(11)||chr(13),' ')) as PARSE_TYPE,
TO_CHAR(translate(CDX_SUPPORT,chr(10)||chr(11)||chr(13),' ')) as CDX_SUPPORT,
TO_CHAR(translate(ENTITLEMENT_TYPE,chr(10)||chr(11)||chr(13),' ')) as ENTITLEMENT_TYPE,
TO_CHAR(translate(CLUB_TYPE,chr(10)||chr(11)||chr(13),' ')) as CLUB_TYPE,
TO_CHAR(translate(LOCALE,chr(10)||chr(11)||chr(13),' ')) as LOCALE,
TO_CHAR(translate(DEFAULT_PTA,chr(10)||chr(11)||chr(13),' ')) as DEFAULT_PTA,
TO_CHAR(LAST_DEF_PTA_OVERRIDE,'YYYY-MM-DD') as LAST_DEF_PTA_OVERRIDE ,
TO_CHAR(translate(CALLMOVER_FLAG,chr(10)||chr(11)||chr(13),' ')) as CALLMOVER_FLAG,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_CAD.CLUB_INFO