UPDATE OPERATIONAL.CONNECTSUITE_MBRSHIP_BILL  MB 
     SET BILL_PRNT_DT=current_datetime()
	  WHERE MB.BILL_PRNT_DT IS NULL
	 	  AND TRIM(MB.BILL_TYP_CD) = 'R'
      AND MB.BILL_STMT_NR IN (1)
      AND EXISTS (select '1' from LANDING.STAGE_MEMBERSHIP_BILL SMB
WHERE EVENT_TYPE='BILL1'
    AND cast(SMB.UPDATE_DTTIME as date)=CURRENT_DATE()
    AND SMB.FILE_GENERATED='Y'
    AND SMB.BILL_KEY=MB.BILL_KY);
