WITH A AS (
    SELECT
        LOC_NO,
        SERVICES,
        MONTH,
        COUNT(SERVICES) SERVICE_COUNTS
    FROM (
        SELECT
            LOCATIONNO AS LOC_NO,
            LABOR.DESC_LINES AS SERVICES,
            EXTRACT(MONTH FROM RO_DATE) MONTH
        FROM `COR_ANALYTICS.RO_WRITER_HRO`
        INNER JOIN (
            SELECT
                RO_NO AS R_NO,
                TRIM(DESC_LINES)DESC_LINES,
                LOCATION_NO
            FROM `COR_ANALYTICS.RO_WRITER_HLABOR`
            WHERE DESC_LINES IS NOT NULL
        ) LABOR
        ON RO_NO = R_NO
        AND LOCATIONNO = LOCATION_NO
    )
    GROUP BY
        LOC_NO,
        SERVICES,
        MONTH
)
SELECT
    LOC_NO,
    SERVICES,
    MONTH,
    SERVICE_COUNTS,
    STD,
    AVG,
    OUTLIER_CLASS
FROM (
    SELECT
        LOC_NO,
        SERVICES,
        MONTH,
        SERVICE_COUNTS,
        STD,
        AVG,
        CASE
            WHEN SERVICE_COUNTS >(AVG+STD+STD) THEN 'OUTLIER'
            ELSE 'NOT_OUTLIER'
            END AS OUTLIER_CLASS
    FROM (
        SELECT
            A.LOC_NO,
            A.SERVICES,
            A.MONTH,
            A.SERVICE_COUNTS,
            STD_DEV.STD,
            STD_DEV.AVG
        FROM (
            SELECT *
            FROM (
                SELECT
                    DESC_LINES,
                    MONTH_RO,
                    STDDEV_SAMP(SERVICE_COUNTS)STD,
                    AVG(SERVICE_COUNTS)AVG
                FROM (
                    SELECT
                        *,
                        COUNT(DESC_LINES) SERVICE_COUNTS
                    FROM (
                        SELECT
                            LOCATIONNO AS LOC_NO,
                            LABOR.DESC_LINES,
                            EXTRACT(MONTH FROM RO_DATE) MONTH_RO
                        FROM `COR_ANALYTICS.RO_WRITER_HRO`
                        INNER JOIN (
                            SELECT
                                RO_NO AS R_NO,
                                TRIM(DESC_LINES)DESC_LINES,
                                LOCATION_NO
                            FROM `COR_ANALYTICS.RO_WRITER_HLABOR`
                            WHERE DESC_LINES IS NOT NULL
                        ) LABOR
                        ON RO_NO = R_NO
                        AND LOCATIONNO = LOCATION_NO
                    )
                    GROUP BY
                        LOC_NO,
                        DESC_LINES,
                        MONTH_RO
                )
                GROUP BY
                    DESC_LINES,
                    MONTH_RO
            )
            WHERE STD IS NOT NULL
        ) STD_DEV
        INNER JOIN A
        ON SERVICES=DESC_LINES
        AND MONTH=MONTH_RO
    )
)
WHERE OUTLIER_CLASS ='OUTLIER'
