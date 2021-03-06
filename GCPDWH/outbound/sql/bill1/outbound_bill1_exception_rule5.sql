  --Audit sql to be put here
UPDATE LANDING.STAGE_MEMBERSHIP_BILL SET EXCEPTION_FLAG='Y',FILE_GENERATED = 'E',
EXCEPTION_REASON=case when EXCEPTION_REASON is null then 'Address is incomplete' ELSE CONCAT(EXCEPTION_REASON,',','Address is incomplete') END
WHERE (ADDRESS1 is null or CITY is null or STATE is null or ZIP is null) and CAST(CREATE_DTTIME AS DATE)=CURRENT_DATE() AND EVENT_TYPE='BILL1';