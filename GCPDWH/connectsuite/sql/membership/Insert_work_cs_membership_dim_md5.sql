--DELETE FROM LANDING.WORK_CS_MEMBERSHIP_DIM_MD5 WHERE TRUE;
INSERT INTO LANDING.WORK_CS_MEMBERSHIP_DIM_MD5
(MEMBERSHIP_KEY           ,    
MEMBERSHIP_ID            ,
BILL_PAY_METHOD          ,
MEMBERSHIP_STATUS_CD     ,
MEMBERSHIP_STATUS_DESC   ,
DUES_COST_AMT            ,
PAYMENT_APPLY_AMT        ,
PAYMENT_PENDING_AMT      ,
PAYMENT_REFUND_AMT       ,
DUES_ADJUSTMENT_AMT      ,
CREDIT_APPLY_AMT         ,
CREDIT_PENDING_AMT       ,
RETURNED_CHECK_FEE_AMT   ,
OTHER_FEE_AMT            ,
PLUS_MEMBERSHIP_IND      ,
FUTURE_EFFECTIVE_IND     ,
KIT_ISSUED_IND           ,
BRANCH_CD                ,
BRANCH_NAME              ,
INCENTIVE_TYPE_CD        ,
ENTRANCE_FEE_AMT         ,
PREVIOUS_STAUS_DESC_CD   ,
DONOT_RENEW_REASON_CD    ,
CANCEL_DT                ,
MEMBERSHIP_ACTIVE_DT     ,
TERM_EFFECTIVE_DT        ,
TERM_EXPIRATION_DT       ,
REINSTATE_DT             ,
INCEPTION_DT             ,
TRANSFER_IN_IND          ,
MEMBERSHIP_LEVEL         ,
BALANCE_DUE_AMT          ,
GIFTED_IND               ,
LAST_UPGRADE_DT          ,
LAST_DOWNGRADE_DT        ,
COMPENSATION_EMPLOYEE_ID ,
AR_DISCOUNT_APPLIED_DT   ,
AR_DECLINE_FLAG          ,
TOTAL_CHARGES            ,
TOTAL_PAYMENTS           ,
TOTAL_CREDITS            ,
SAFETY_OWED              ,
BRN_KY                   ,
PAYMENT_DT               ,
STATE                    ,
MBRS_DNR_KY              ,
MBRS_UPDATE_DT,
DONOR_FIRST_NM,
DONOR_MIDDLE_INITIAL_NM,
DONOR_LAST_NM,
MD5_VALUE
)
SELECT * EXCEPT(RN),MD5(CONCAT(
--COALESCE(CAST(MEMBERSHIP_KEY AS STRING),' '),
COALESCE(CAST(MEMBERSHIP_ID AS STRING),' '),
COALESCE(CAST(BILL_PAY_METHOD AS STRING),' '),
COALESCE(CAST(MEMBERSHIP_STATUS_CD AS STRING),' '),
COALESCE(CAST(MEMBERSHIP_STATUS_DESC AS STRING),' '),
COALESCE(CAST(DUES_COST_AMT AS STRING),' '),
COALESCE(CAST(PAYMENT_APPLY_AMT AS STRING),' '),
COALESCE(CAST(PAYMENT_PENDING_AMT AS STRING),' '),
COALESCE(CAST(PAYMENT_REFUND_AMT AS STRING),' '),
COALESCE(CAST(DUES_ADJUSTMENT_AMT AS STRING),' '),
COALESCE(CAST(CREDIT_APPLY_AMT AS STRING),' '),
COALESCE(CAST(CREDIT_PENDING_AMT AS STRING),' '),
COALESCE(CAST(RETURNED_CHECK_FEE_AMT AS STRING),' '),
COALESCE(CAST(OTHER_FEE_AMT AS STRING),' '),
COALESCE(CAST(PLUS_MEMBERSHIP_IND AS STRING),' '),
COALESCE(CAST(FUTURE_EFFECTIVE_IND AS STRING),' '),
COALESCE(CAST(KIT_ISSUED_IND AS STRING),' '),
COALESCE(CAST(BRANCH_CD AS STRING),' '),
COALESCE(CAST(BRANCH_NAME AS STRING),' '),
COALESCE(CAST(INCENTIVE_TYPE_CD AS STRING),' '),
COALESCE(CAST(ENTRANCE_FEE_AMT AS STRING),' '),
COALESCE(CAST(PREVIOUS_STAUS_DESC_CD AS STRING),' '),
COALESCE(CAST(DONOT_RENEW_REASON_CD AS STRING),' '),
COALESCE(CAST(CANCEL_DT AS STRING),' '),
COALESCE(CAST(MEMBERSHIP_ACTIVE_DT AS STRING),' '),
COALESCE(CAST(TERM_EFFECTIVE_DT AS STRING),' '),
COALESCE(CAST(TERM_EXPIRATION_DT AS STRING),' '),
COALESCE(CAST(REINSTATE_DT AS STRING),' '),
COALESCE(CAST(INCEPTION_DT AS STRING),' '),
COALESCE(CAST(TRANSFER_IN_IND AS STRING),' '),
COALESCE(CAST(MEMBERSHIP_LEVEL AS STRING),' '),
COALESCE(CAST(BALANCE_DUE_AMT AS STRING),' '),
COALESCE(CAST(GIFTED_IND AS STRING),' '),
COALESCE(CAST(LAST_UPGRADE_DT AS STRING),' '),
COALESCE(CAST(LAST_DOWNGRADE_DT AS STRING),' '),
COALESCE(CAST(COMPENSATION_EMPLOYEE_ID AS STRING),' '),
COALESCE(CAST(AR_DISCOUNT_APPLIED_DT AS STRING),' '),
COALESCE(CAST(AR_DECLINE_FLAG AS STRING),' '),
COALESCE(CAST(TOTAL_CHARGES AS STRING),' '),
COALESCE(CAST(TOTAL_PAYMENTS AS STRING),' '),
COALESCE(CAST(TOTAL_CREDITS AS STRING),' '),
COALESCE(CAST(SAFETY_OWED AS STRING),' '),
COALESCE(CAST(BRN_KY AS STRING),' '),
COALESCE(CAST(PAYMENT_DT AS STRING),' '),
COALESCE(CAST(STATE AS STRING),' '),
COALESCE(CAST(DONOR_FIRST_NM AS STRING),' '),
COALESCE(CAST(DONOR_MIDDLE_INITIAL_NM AS STRING),' '),
COALESCE(CAST(DONOR_LAST_NM AS STRING),' ')
)) FROM (
select DISTINCT 
MEMBERSHIP_KEY           ,    
MEMBERSHIP_ID            ,
BILL_PAY_METHOD          ,
MEMBERSHIP_STATUS_CD     ,
MEMBERSHIP_STATUS_DESC   ,
DUES_COST_AMT            ,
PAYMENT_APPLY_AMT        ,
PAYMENT_PENDING_AMT      ,
PAYMENT_REFUND_AMT       ,
DUES_ADJUSTMENT_AMT      ,
CREDIT_APPLY_AMT         ,
CREDIT_PENDING_AMT       ,
RETURNED_CHECK_FEE_AMT   ,
OTHER_FEE_AMT            ,
PLUS_MEMBERSHIP_IND      ,
FUTURE_EFFECTIVE_IND     ,
KIT_ISSUED_IND           ,
BRANCH_CD                ,
BRANCH_NAME              ,
INCENTIVE_TYPE_CD        ,
ENTRANCE_FEE_AMT         ,
PREVIOUS_STAUS_DESC_CD   ,
DONOT_RENEW_REASON_CD    ,
CANCEL_DT                ,
MEMBERSHIP_ACTIVE_DT     ,
TERM_EFFECTIVE_DT        ,
TERM_EXPIRATION_DT       ,
REINSTATE_DT             ,
INCEPTION_DT             ,
TRANSFER_IN_IND          ,
MEMBERSHIP_LEVEL         ,
BALANCE_DUE_AMT          ,
GIFTED_IND               ,
LAST_UPGRADE_DT          ,
LAST_DOWNGRADE_DT        ,
COMPENSATION_EMPLOYEE_ID ,
AR_DISCOUNT_APPLIED_DT   ,
AR_DECLINE_FLAG          ,
TOTAL_CHARGES            ,
TOTAL_PAYMENTS           ,
TOTAL_CREDITS            ,
SAFETY_OWED              ,
BRN_KY                   ,
PAYMENT_DT               ,
STATE                    ,
MBRS_DNR_KY              ,
MBRS_UPDATE_DT  ,
DONOR_FIRST_NM,
DONOR_MIDDLE_INITIAL_NM,
DONOR_LAST_NM,
ROW_NUMBER() OVER(PARTITION BY MEMBERSHIP_KEY ORDER BY MBRS_UPDATE_DT DESC) RN
from 
LANDING.WORK_CS_MEMBERSHIP_DIM)WHERE RN = 1