--Odometer reading by UNIT YEAR

SELECT
    UNIT_YEAR,
    AVG(MAX_MIL)AVG_MIL
FROM (
    SELECT
        VIN,
        UNIT_YEAR,
        MAX(VEHICLE_ODOMETER_READING) AS MAX_MIL
    FROM `COR_ANALYTICS.INSURANCE_UNIT_DETAILS_DIM`
    WHERE
        UPPER(PRODUCT_TYPE) = "AUTO"
        AND VEHICLE_ODOMETER_READING != 0
        AND VEHICLE_ODOMETER_READING <= 400000
        AND UNIT_YEAR >= 1929
    GROUP BY
        VIN,
        UNIT_YEAR
    )
GROUP BY
    UNIT_YEAR
ORDER BY
    UNIT_YEAR