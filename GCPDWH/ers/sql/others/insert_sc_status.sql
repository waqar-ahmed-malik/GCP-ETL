SELECT TO_CHAR(translate(SC_STATUS.COMM_CTR_ID, chr(10)||chr(11)||chr(13),' ')) COMM_CTR_ID, 
TO_CHAR(SC_STATUS.SC_DT,'YYYY-MM-DD HH24:MI:SS') SC_DT, 
TO_CHAR(translate(SC_STATUS.SC_ID, chr(10)||chr(11)||chr(13),' ')) SC_ID,
TO_CHAR(SC_STATUS.SC_STS_TM , 'YYYY-MM-DD HH24:MI:SS') SC_STS_TM, 
TO_CHAR(translate(SC_STATUS.SC_STS_CD, chr(10)||chr(11)||chr(13),' ')) SC_STS_CD,
TO_CHAR(translate(SC_STATUS.SC_STS_UPD_CD, chr(10)||chr(11)||chr(13),' ')) SC_STS_UPD_CD,
TO_CHAR(translate(SC_STATUS.SC_STS_RSN_CD, chr(10)||chr(11)||chr(13),' ')) SC_STS_RSN_CD,
TO_CHAR(translate(SC_STATUS.SC_FACL_ID, chr(10)||chr(11)||chr(13),' ')) SC_FACL_ID,
TO_CHAR(translate(SC_STATUS.SC_TRK_ID, chr(10)||chr(11)||chr(13),' ')) SC_TRK_ID,
TO_CHAR(translate(SC_STATUS.SC_STS_SRC_CD, chr(10)||chr(11)||chr(13),' ')) SC_STS_SRC_CD, 
TO_CHAR(translate(SC_STATUS.SC_STS_TRK_NEW_ETA, chr(10)||chr(11)||chr(13),' ')) SC_STS_TRK_NEW_ETA, 
TO_CHAR(translate(SC_STATUS.EMPLE_ID, chr(10)||chr(11)||chr(13),' ')) EMPLE_ID,
TO_CHAR(translate(SC_STATUS.AVL_ETA, chr(10)||chr(11)||chr(13),' ')) AVL_ETA,
TO_CHAR(translate(SC_STATUS.EMPLE_LOCATION, chr(10)||chr(11)||chr(13),' ')) EMPLE_LOCATION,
TO_CHAR(translate(SC_STATUS.EMPLE_ROLE, chr(10)||chr(11)||chr(13),' ')) EMPLE_ROLE,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM
 NCA_ERS_HIST.SC_STATUS SC_STATUS
INNER JOIN
(
SELECT SC_ID, SC_DT , COMM_CTR_ID FROM NCA_ERS_HIST.ARCH_CALL  
WHERE  ARCH_DATE >=  TO_DATE('incr_date_arch','yyyy-mm-dd hh24:mi:ss')
UNION
SELECT  SC_ID, SC_DT , COMM_CTR_ID FROM NCA_ERS_HIST.SC_CALL_COMMENT SC 
WHERE  SC_COMM_ADD_TM >=  TO_DATE('incr_date_arch','yyyy-mm-dd hh24:mi:ss')
UNION 
SELECT  cast(KEY3_DA AS number)   AS SC_ID,TO_DATE(TRANSLATE(TRIM(KEY2_DA),chr(47),'-'),'mm-dd-yyyy hh24:mi:ss' ) AS SC_DT , KEY1_DA AS COMM_CTR_ID 
FROM NCA_APD.D2K_AUDIT 
WHERE   KEY1_DA =  '005' AND  TIMESTAMP_DT >=   TO_DATE('incr_date_arch','yyyy-mm-dd hh24:mi:ss') 
AND KEY3_DA IS NOT NULL AND trim(KEY3_DA) NOT IN ('0','1','2','3','4','5','6','7','8','9')
AND trim(KEY3_DA) NOT IN (0,1,2,3,4,5,6,7,8,9)
) CDC
ON SC_STATUS.SC_ID=CDC.SC_ID AND SC_STATUS.COMM_CTR_ID=CDC.COMM_CTR_ID AND SC_STATUS.SC_DT=CDC.SC_DT