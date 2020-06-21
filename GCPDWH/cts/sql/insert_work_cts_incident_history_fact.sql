SELECT TO_CHAR(NCA_CTS.INCD_STATUS_HIST.BRANCH_CD) BRANCH_CD,
TO_CHAR(NCA_CTS.INCD_STATUS_HIST.RECEIVED_DT,'YYYY-MM-DD HH:MM:SS') AS RECEIVED_DT,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.CMPLNT_ID) CMPLNT_ID,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.INCD_ID) INCD_ID,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.STATUS_CD) STATUS_CD,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.STATUS_DTIME_DT,'YYYY-MM-DD HH:MM:SS') AS STATUS_DTIME_DT,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.STATUS_EMPLE_ID) STATUS_EMPLE_ID,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.STATUS_RSN_CD) STATUS_RSN_CD,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.STATUS_TX) STATUS_TX,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.STATUS_SEQ_NBR) STATUS_SEQ_NBR,
 TO_CHAR(NCA_CTS.INCD_STATUS_HIST.SUSPEND_UNTIL_DATE ,'YYYY-MM-DD HH:MM:SS') AS SUSPEND_UNTIL_DATE
FROM
 NCA_CTS.INCD_STATUS_HIST
WHERE TRUNC(INCD_STATUS_HIST.RECEIVED_DT)>=to_date('v_input','yyyy-mm-dd')