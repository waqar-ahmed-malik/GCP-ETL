SELECT
  result
FROM (
------------------------HEADER------------------------ 
  SELECT
    1 AS sequence,
    CONCAT('HDR',LPAD('PROD',
        6),LPAD('CS',
        9),LPAD('CS_EXTERNAL_TRANSACTIONS',
        25),LPAD((
        SELECT
          DISTINCT
          CASE
            WHEN catalog_name='aaa-mwg-dwprod' THEN 'Prod'
          ELSE
          ' Test'
        END
        FROM
          INFORMATION_SCHEMA.SCHEMATA),
        25), 'ext_payments_', FORMAT_DATE("%Y%m%d",CURRENT_DATE()),'.csv', LPAD(' ',
        25),FORMAT_DATETIME("%Y%m%d%H%M%S",
        current_datetime()),'01') AS result
  UNION ALL
------------------------DETAIL------------------------
  SELECT
    2 AS sequence,
    CONCAT(CAST(FORMAT_DATE("%m/%d/%Y",TRANSACTION_DT) AS string), ' ', '00:00:00', '    ', 
    rpad(case when TRANSACTION_AMT<0.9 
              then LPAD(REPLACE(CAST(TRANSACTION_AMT* 100 AS string),'0',NULL),2,'0')
              else cast(TRANSACTION_AMT*100 as string) 
              end,
              7,' '),
        rpad(MEMBERSHIP_NUM,
        8,
        ' '), CONCAT(IFNULL(PAYMENT_CHANNEL,
          ''),'|', IFNULL(CREDIT_CARD_NUM_LAST_4,
          ''),'|', IFNULL(CREDIT_CARD_TYPE,
          ''),'|', IFNULL(CHECK_ROUTING_NUM_LAST_4,
          ''),'|', IFNULL(CHECKING_ACCOUNT_NUM_LAST_4,
          ''),'|',IFNULL(CHECK_NUM,
          '')) ) AS result
  FROM
    OPERATIONAL.EXTERNAL_SOURCE_PAYMENT_TRANSACTIONS
  WHERE
    MEMBERSHIP_APPLICATION_SENT_IND = 'Y'
    AND MEMBERSHIP_APPLICATION_SENT_DT IS NULL
    AND UPDATE_DTTIME> '{{ prev_execution_date }}'
  UNION ALL
------------------------TRAILER------------------------
  SELECT
    3 AS sequence,
    CONCAT('TRL',LPAD(CAST(COUNT(*) AS string),
        6,
        ' '),
        lpad(CAST(round(SUM(TRANSACTION_AMT),2)*100 AS string), 
        12,
        ' ')) AS result
  FROM (
    SELECT
      TRANSACTION_DT,
      TRANSACTION_AMT,
      MEMBERSHIP_NUM
    FROM
      OPERATIONAL.EXTERNAL_SOURCE_PAYMENT_TRANSACTIONS
    WHERE
      MEMBERSHIP_APPLICATION_SENT_IND = 'Y'
      AND MEMBERSHIP_APPLICATION_SENT_DT IS NULL
      AND UPDATE_DTTIME> '{{ prev_execution_date }}'
      ) 
      )
ORDER BY
  sequence asc
