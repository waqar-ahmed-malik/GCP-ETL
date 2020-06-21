--average year of a the cars
 
SELECT
    AVG(CAR_AGE) AVG_CAR_AGE
FROM (
    SELECT
        *,
        (CUR_YEAR-UNIT_YEAR) CAR_AGE
    FROM (
        SELECT
            AGMT_NUM,
            UNIT_YEAR,
            EXTRACT(YEAR FROM CURRENT_DATE()) AS CUR_YEAR,
            VIN
        FROM `COR_ANALYTICS.INSURANCE_UNIT_DETAILS_DIM`
      -- HAD TO USE THE WHERE CAUSE LOT OF AGREEMENTS NOS DOESN'T HAVE A VIN CORRESPONDING TO IT
        WHERE UPPER(PRODUCT_TYPE) ='AUTO'
        GROUP BY
            AGMT_NUM,
            UNIT_YEAR,
            VIN
        )
    )