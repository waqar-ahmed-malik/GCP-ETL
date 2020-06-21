SELECT SOURCE_COUNT AS SOURCE_COUNT, 
JOB_ID AS JOB_ID
FROM 
(select 
COUNT(*) AS SOURCE_COUNT, 1 AS JOB_ID
FROM M_PRD.DBO.MBR MBR
INNER JOIN (
    SELECT DISTINCT 
      MBRS_KY,
      MAX(MBRS_UPDT_DT) AS  MBRS_UPDT_DT    
      FROM (
        SELECT
          MBRS_KY,
          MBRS_RTD2K_UPDT_DT AS MBRS_UPDT_DT
        FROM
          M_PRD.DBO.MBRSHIP
        WHERE
          CONVERT(DATE,
            MBRSHIP.MBRS_RTD2K_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
            UNION
        SELECT
          DISTINCT MBRSHIP_COMMENT.MBRS_KY AS MBRS_KY,
          MBRSHIP_COMMENT.MCMT_UPDT_DT AS MBRS_UPDT_DT
        FROM
        M_PRD.DBO.MBRSHIP_COMMENT
        WHERE
          CONVERT(DATE,
            MBRSHIP_COMMENT.MCMT_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
         ) SQ GROUP BY MBRS_KY ) CDC
		 ON
    CDC.MBRS_KY= MBR.MBRS_KY
    
    UNION 
select 
COUNT(*), 2 AS JOB_ID

FROM M_PRD.DBO.MBRSHIP MBR
INNER JOIN (
    SELECT DISTINCT 
      MBRS_KY,
      MAX(MBRS_UPDT_DT) AS  MBRS_UPDT_DT    
      FROM (
        SELECT
          MBRS_KY,
          MBRS_RTD2K_UPDT_DT AS MBRS_UPDT_DT
        FROM
          M_PRD.DBO.MBRSHIP
        WHERE
          CONVERT(DATE,
            MBRSHIP.MBRS_RTD2K_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
            UNION
        SELECT
          DISTINCT MBRSHIP_COMMENT.MBRS_KY AS MBRS_KY,
          MBRSHIP_COMMENT.MCMT_UPDT_DT AS MBRS_UPDT_DT
        FROM
         M_PRD.DBO.MBRSHIP_COMMENT
        WHERE
          CONVERT(DATE,
            MBRSHIP_COMMENT.MCMT_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
         ) SQ GROUP BY MBRS_KY ) CDC
		 ON
    CDC.MBRS_KY= MBR.MBRS_KY
    
    
UNION 
select 
COUNT(*), 3 AS JOB_ID

FROM M_PRD.DBO.MBR MBR
INNER JOIN (
    SELECT DISTINCT 
      MBRS_KY,
      MAX(MBRS_UPDT_DT) AS  MBRS_UPDT_DT    
      FROM (
        SELECT
          MBRS_KY,
          MBRS_RTD2K_UPDT_DT AS MBRS_UPDT_DT
        FROM
          M_PRD.DBO.MBRSHIP
        WHERE
          CONVERT(DATE,
            MBRSHIP.MBRS_RTD2K_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
            UNION
        SELECT
          DISTINCT MBRSHIP_COMMENT.MBRS_KY AS MBRS_KY,
          MBRSHIP_COMMENT.MCMT_UPDT_DT AS MBRS_UPDT_DT
        FROM
          M_PRD.DBO.MBRSHIP_COMMENT
        WHERE
          CONVERT(DATE,
            MBRSHIP_COMMENT.MCMT_UPDT_DT) = CONVERT(DATE,GETDATE() - 1)
         ) SQ GROUP BY MBRS_KY ) CDC
		 ON
    CDC.MBRS_KY= MBR.MBRS_KY
UNION
select 
COUNT(*), 4 AS JOB_ID
FROM
  M_PRD.dbo.DAILY_EVENT
 WHERE CONVERT(DATE,
 DAILY_EVENT.DLY_TRANS_DT) =  CONVERT(DATE,GETDATE() - 1)
)AS TEMP