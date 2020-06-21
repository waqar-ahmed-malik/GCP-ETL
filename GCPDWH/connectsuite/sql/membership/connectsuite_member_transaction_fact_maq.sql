CREATE OR REPLACE TABLE LANDING.MAQ_INDICATOR
AS
WITH sorted_transactions as
(
	  SELECT 
	    CAST(CAST(TRIM(TRANSACTION_DTTIME) as DATETIME) as DATE) as TRANSACTION_DTTIME, --truncate timestamp to date
	    TRIM(SOURCE_TRANSACTION_ID) SOURCE_TRANSACTION_ID, 
	    TRIM(MEMBERSHIP_NUM) MEMBERSHIP_NUM, 
	    TRIM(TRANSACTION_TYPE_CD) TRANSACTION_TYPE_CD, 
	    TRIM(COMPONENT_CD) COMPONENT_CD, 
	    CAST(ASSOCIATE_ID as INT64) ASSOCIATE_ID,
		CASE WHEN TRIM(UPPER(PREVIOUS_TRANSFER_IN_IND)) IN ('T','T1') THEN 'Y' ELSE  TRIM(UPPER(TRANSFER_IN_IND)) END AS TRANSFER_IN_IND,
	    CAST(CAST(TRIM(TERM_EXPIRATION_DT) as DATETIME) as DATE) TERM_EXPIRATION_DT, --truncate timestamp to date
	    RANK() OVER empwindow as RANK,
	    SUM(IF(TRANSACTION_TYPE_CD IN ('ADD','REINSTATE','CREATE'),1,0)) OVER empwindow as ADD,
	    SUM(IF(TRANSACTION_TYPE_CD IN ('UPGRADE'),1,0)) OVER empwindow as UPGRADE,
	    SUM(IF(TRANSACTION_TYPE_CD IN ('RENEW'),1,0)) OVER empwindow as RENEW
	  FROM `LANDING.WORK_CS_MEMBER_TRANSACTIONS_FACT` 
	  WINDOW empwindow AS (PARTITION BY TRANSACTION_DTTIME, MEMBERSHIP_NUM, ASSOCIATE_ID, COMPONENT_CD ORDER BY SOURCE_TRANSACTION_ID DESC)
),
daily_transactions_windowed as
(
	  select 
	  *, 
	  COALESCE
	    (IF(ADD=1 and TRANSACTION_TYPE_CD IN ('ADD','REINSTATE','CREATE'),'New',NULL),  -- change to New, Renew, Upgrade, Service
		    IF(UPGRADE=1 and TRANSACTION_TYPE_CD IN ('UPGRADE'),'Upgrade',NULL),
			  IF(RENEW=1 and TRANSFER_IN_IND = 'Y' and TRANSACTION_TYPE_CD IN ('RENEW'),'New',NULL),
			    IF(RENEW=1 and TRANSACTION_TYPE_CD IN ('RENEW'),'Renew',NULL),
				    'Service') as trans_disposition
				  from sorted_transactions
			),
			daily_transactions_special_cases as
			(
				  select 
				  MEMBERSHIP_NUM, ASSOCIATE_ID, COMPONENT_CD, trans_disposition,
				  MIN(TRANSACTION_DTTIME) TRANSACTION_DTTIME, MAX(TERM_EXPIRATION_DT) TERM_EXPIRATION_DT
				  from daily_transactions_windowed 
				  where trans_disposition in ('New','Renew') 
				  group by 1,2,3,4 
				    having count(*) > 0
			),
			fact_check_for_new as
			(
				  select fcfn.MEMBERSHIP_NUM, fcfn.ASSOCIATE_ID, fcfn.MEMBERSHIP_LEVEL, fcfn.service_disposition,
				  MAX(fcfn.mf_TRANSACTION_DTTIME) mf_TRANSACTION_DTTIME,
				  MIN(fcfn.dtsc_TRANSACTION_DTTIME) dtsc_TRANSACTION_DTTIME  
				  from
				    (
					    select 
					    TRIM(mf.MEMBERSHIP_NUM) MEMBERSHIP_NUM, mf.ASSOCIATE_ID, TRIM(mf.MEMBERSHIP_LEVEL) MEMBERSHIP_LEVEL, 'Service' as service_disposition,
					    mf.TRANSACTION_DTTIME mf_TRANSACTION_DTTIME, 
					    dtsc.TRANSACTION_DTTIME dtsc_TRANSACTION_DTTIME
					    from daily_transactions_special_cases dtsc
					    join `CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_TRANSACTIONS_FACT` mf 
					      on TRIM(dtsc.MEMBERSHIP_NUM) = TRIM(mf.MEMBERSHIP_NUM) 
					         and dtsc.ASSOCIATE_ID = mf.ASSOCIATE_ID
						     and TRIM(dtsc.COMPONENT_CD) = TRIM(mf.MEMBERSHIP_LEVEL)
							 and dtsc.trans_disposition = 'New'
							 --and mf.TRANSACTION_TYPE_CD in ('ADD','CREATE','RENEW','REINSTATE')
							 and UPPER(mf.MAQ_IND) in ('NEW','RENEW')
    -- we subtract the difference between the fact table transaction date and we want that difference to be less than 1 year + 90 days
    -- may need to look at how many rows can return in this join
    where DATE_DIFF(CAST(dtsc.TRANSACTION_DTTIME as DATE),CAST(mf.TERM_EXPIRATION_DT AS DATE),day) <= (90)
    and dtsc.TRANSACTION_DTTIME > CAST(mf.TRANSACTION_DTTIME AS DATE)
    ) fcfn
  group by 1,2,3,4
),
fact_check_for_renew as
(
	  select distinct
	    cast(mf.TRANSACTION_DTTIME as DATE) TRANSACTION_DTTIME, mf.MEMBERSHIP_NUM, mf.ASSOCIATE_ID, mf.MEMBERSHIP_LEVEL, 
	    'Service' as service_disposition, mf.TERM_EXPIRATION_DT
	  from daily_transactions_special_cases dtsc
	  join `CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_TRANSACTIONS_FACT` mf 
	    on dtsc.MEMBERSHIP_NUM = mf.MEMBERSHIP_NUM 
	      and dtsc.ASSOCIATE_ID = mf.ASSOCIATE_ID 
	      and dtsc.COMPONENT_CD = mf.MEMBERSHIP_LEVEL
	      and dtsc.trans_disposition = 'Renew'
	      and mf.TRANSACTION_TYPE_CD = 'RENEW'
	      and dtsc.TERM_EXPIRATION_DT = mf.TERM_EXPIRATION_DT
	      and dtsc.TRANSACTION_DTTIME > cast(mf.TRANSACTION_DTTIME as DATE)
)
select 
    dtw.*, 
    fcfn.service_disposition as new_service_disposition, 
    fcfn.mf_TRANSACTION_DTTIME as new_service_disposition_trans_date,
    fcfr.service_disposition as renew_service_disposition,
    fcfr.TERM_EXPIRATION_DT as renew_service_disposition_exp,
    fcfr.TRANSACTION_DTTIME as renew_service_disposition_trans_date,
    COALESCE(fcfn.service_disposition, fcfr.service_disposition, trans_disposition) as MAQ_INDICATOR_FINAL
  from daily_transactions_windowed dtw
  left outer join fact_check_for_new fcfn 
    on dtw.MEMBERSHIP_NUM = fcfn.MEMBERSHIP_NUM 
      and dtw.ASSOCIATE_ID = fcfn.ASSOCIATE_ID 
      and dtw.COMPONENT_CD = fcfn.MEMBERSHIP_LEVEL
      and dtw.trans_disposition = 'New'
  left outer join fact_check_for_renew fcfr 
    on dtw.MEMBERSHIP_NUM = fcfr.MEMBERSHIP_NUM 
      and dtw.ASSOCIATE_ID = fcfr.ASSOCIATE_ID 
      and dtw.COMPONENT_CD = fcfr.MEMBERSHIP_LEVEL
      and dtw.trans_disposition = 'Renew'
      and dtw.TERM_EXPIRATION_DT = fcfr.TERM_EXPIRATION_DT
;
