  --Audit sql to be put here
UPDATE LANDING.STAGE_MEMBERSHIP_BILL SET EXCEPTION_FLAG='Y',FILE_GENERATED = 'E',
EXCEPTION_REASON=case when EXCEPTION_REASON is null then 'Amount greater than $999' ELSE CONCAT(EXCEPTION_REASON,',','Amount greater than $999') END
WHERE (TOTAL_AMT_PAID+TOTAL_AMT_DUE)>999 and CAST(CREATE_DTTIME AS DATE)=CURRENT_DATE() AND EVENT_TYPE='BILL2';