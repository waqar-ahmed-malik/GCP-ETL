SELECT TO_CHAR(translate(MH.COMM_CTR_ID,chr(10)||chr(11)||chr(13),' ')) as COMM_CTR_ID,
TO_CHAR(translate(MH.SC_ID,chr(10)||chr(11)||chr(13),' ')) as SC_ID,
TO_CHAR(MH.SC_DT,'YYYY-MM-DD HH24:MI:SS') as SC_DT,
TO_CHAR(translate(MILES_TW_DISP,chr(10)||chr(11)||chr(13),' ')) as MILES_TW_DISP,
TO_CHAR(translate(MILES_ER_CALC,chr(10)||chr(11)||chr(13),' ')) as MILES_ER_CALC,
TO_CHAR(translate(MILES_TW_CALC,chr(10)||chr(11)||chr(13),' ')) as MILES_TW_CALC,
TO_CHAR(translate(MILES_ER_CPMS,chr(10)||chr(11)||chr(13),' ')) as MILES_ER_CPMS,
TO_CHAR(translate(MILES_TW_CPMS,chr(10)||chr(11)||chr(13),' ')) as MILES_TW_CPMS,
TO_CHAR(translate(ADJUSTED_ER,chr(10)||chr(11)||chr(13),' ')) as ADJUSTED_ER,
TO_CHAR(translate(ADJUSTED_TW,chr(10)||chr(11)||chr(13),' ')) as ADJUSTED_TW,
TO_CHAR(translate(ER_TOLERANCE,chr(10)||chr(11)||chr(13),' ')) as ER_TOLERANCE,
TO_CHAR(translate(TW_TOLERANCE,chr(10)||chr(11)||chr(13),' ')) as TW_TOLERANCE,
TO_CHAR(translate(MILEAGE_COMMENT,chr(10)||chr(11)||chr(13),' ')) as MILEAGE_COMMENT,
TO_CHAR(translate(ER_LAT,chr(10)||chr(11)||chr(13),' ')) as ER_LAT,
TO_CHAR(translate(ER_LONG,chr(10)||chr(11)||chr(13),' ')) as ER_LONG,
TO_CHAR(translate(MILEAGE_CALC_TYPE,chr(10)||chr(11)||chr(13),' ')) as MILEAGE_CALC_TYPE,
TO_CHAR(translate(MILES_ER_DISP,chr(10)||chr(11)||chr(13),' ')) as MILES_ER_DISP
FROM NCA_ERS_HIST.MILEAGE_HIST MH 
INNER JOIN
(
SELECT SC_ID, SC_DT , COMM_CTR_ID FROM NCA_ERS_HIST.ARCH_CALL  
WHERE  ARCH_DATE >= TO_DATE('incr_date_arch','yyyy-mm-dd hh24:mi:ss')
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
ON MH.SC_ID=CDC.SC_ID AND MH.COMM_CTR_ID=CDC.COMM_CTR_ID AND MH.SC_DT=CDC.SC_DT