SELECT
  ROUND(CASE
      WHEN B.countB=0 THEN 0
    ELSE
    (A.countA / B.countB * 100)
  END
    ,2) AS PERCENT
FROM (
  SELECT
    COUNT(1) AS countA
  FROM
    LANDING.STAGE_MEMBERSHIP_BILL
  WHERE
    EVENT_TYPE = 'BILL1'
    AND CAST(CREATE_DTTIME AS date) = CURRENT_DATE()
    AND (TOTAL_AMT_DUE + TOTAL_AMT_PAID = 0)) A,
  (
  SELECT
    COUNT(1) AS countB
  FROM
    LANDING.STAGE_MEMBERSHIP_BILL
  WHERE
    EVENT_TYPE = 'BILL1'
    AND CAST(CREATE_DTTIME AS date) = CURRENT_DATE() ) B
WHERE
  1=1