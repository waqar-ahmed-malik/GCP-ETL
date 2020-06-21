select DESCRIPTION from 
(SELECT 1 as ROW_NUM,
'TRANSACTION_EFFECTIVE_DATE|TRANSACTION_CATEGORY|ACCOUNTING_ENTRY|TERM_EFFECTIVE_DATE|TERM_EXPIRATION|PAYMENT_AMOUNT|FEE_TYPE|PAYMENT_CHANNEL|MERCHANT_ID|FEES_EXPIRATION|ACCOUNTING_PERIOD_EFFECTIVE|ACCOUNTING_PERIOD_EXPIRATION|BATCH_ID|BATCH_DATE' as DESCRIPTION
union all
select 2 as ROW_NUM,CONCAT(IFNULL(CAST(TRANSACTION_EFFECTIVE_DATE as STRING),''),'|',IFNULL(TRANSACTION_CATEGORY,''),'|','','|','','|','','|',CAST(ROUND(IFNULL(PAYMENT_AMOUNT,0),2) AS STRING),'|','','|',IFNULL(PAYMENT_CHANNEL,''),'|','','|',IFNULL(CAST(FEES_EXPIRATION AS STRING),''),'|',IFNULL(CAST(ACCOUNTING_PERIOD_EFFECTIVE AS STRING),''),'|',IFNULL(CAST(ACCOUNTING_PERIOD_EXPIRATION AS STRING),''),'|',IFNULL(CAST(BATCH_ID AS STRING),''),'|',IFNULL(CAST(BATCH_DATE AS STRING),'')) as DESCRIPTION
FROM (
SELECT 
  CAST(A.PAYMENT_DT AS DATE) AS TRANSACTION_EFFECTIVE_DATE,
  A.TRANSACTION_CATEGORY,
  '' as ACCOUNTING_ENTRY,
  '' as TERM_EFFECTIVE_DATE,
  '' as TERM_EXPIRATION,
  SUM(A.PAYMENT_AMOUNT) AS PAYMENT_AMOUNT,
  A.FEE_TYPE as FEE_TYPE,
  A.PAYMENT_CHANNEL as PAYMENT_CHANNEL,
  '' as MERCHANT_ID,
  CAST(A.FEES_EXPIRATION_DT AS DATE) AS FEES_EXPIRATION,
  CAST(A.ACC_PERIOD_EFFECTIVE_DT AS DATE) AS ACCOUNTING_PERIOD_EFFECTIVE,
  CAST(A.ACC_PERIOD_EXPIRATION_DT AS DATE) AS  ACCOUNTING_PERIOD_EXPIRATION,
  A.JOB_RUN_ID AS BATCH_ID,
  CAST(A.CREATE_DTTIME AS DATE) as BATCH_DATE
FROM OPERATIONAL.REVREC_MEMBERSHIP_PAYMENTS A
where A.RECORD_TYPE in  ('VOID PAYMENT')  and A.FEE_TYPE='DUES' AND  A.JOB_RUN_ID >=
  (SELECT MAX(JOB_RUN_ID)
  FROM OPERATIONAL.REVREC_MEMBERSHIP_PAYMENTS
   )
GROUP BY
 CAST(A.PAYMENT_DT AS DATE),
  A.TRANSACTION_CATEGORY,
  A.FEE_TYPE,
  A.PAYMENT_CHANNEL,
  CAST(A.FEES_EXPIRATION_DT AS DATE),
  CAST(A.ACC_PERIOD_EFFECTIVE_DT AS DATE),
  CAST(A.ACC_PERIOD_EXPIRATION_DT AS DATE),
  A.JOB_RUN_ID,
  CAST(A.CREATE_DTTIME AS DATE))
union all
SELECT 3 as ROW_NUM,CONCAT('CONTROL','|',CAST(COUNT(*) as STRING),'|',CAST(ROUND(SUM(PAYMENT_AMOUNT),2) AS STRING),'|',FORMAT_DATETIME("%m/%d/%Y %H:%M:%S",CURRENT_DATETIME())) AS DESCRIPTION FROM
(SELECT 
  CAST(A.PAYMENT_DT AS DATE) AS TRANSACTION_EFFECTIVE_DATE,
  A.TRANSACTION_CATEGORY,
  SUM(A.PAYMENT_AMOUNT) AS PAYMENT_AMOUNT,
  A.FEE_TYPE,
  A.PAYMENT_CHANNEL,
  CAST(A.FEES_EXPIRATION_DT AS DATE) AS FEES_EXPIRATION_DT,
  CAST(A.ACC_PERIOD_EFFECTIVE_DT AS DATE) AS ACC_PERIOD_EFFECTIVE_DT,
  CAST(A.ACC_PERIOD_EXPIRATION_DT AS DATE) AS  ACC_PERIOD_EXPIRATION_DT,
  A.JOB_RUN_ID AS BATCH_ID,
  CAST(A.CREATE_DTTIME AS DATE)
FROM OPERATIONAL.REVREC_MEMBERSHIP_PAYMENTS A
where A.RECORD_TYPE in  ('VOID PAYMENT')  and A.FEE_TYPE='DUES' AND  A.JOB_RUN_ID >=
  (SELECT MAX(JOB_RUN_ID)
  FROM OPERATIONAL.REVREC_MEMBERSHIP_PAYMENTS
   )
GROUP BY
 CAST(A.PAYMENT_DT AS DATE),
  A.TRANSACTION_CATEGORY,
  A.FEE_TYPE,
  A.PAYMENT_CHANNEL,
  CAST(A.FEES_EXPIRATION_DT AS DATE),
  CAST(A.ACC_PERIOD_EFFECTIVE_DT AS DATE),
  CAST(A.ACC_PERIOD_EXPIRATION_DT AS DATE),
  A.JOB_RUN_ID,
  CAST(A.CREATE_DTTIME AS DATE)))
  order by ROW_NUM
