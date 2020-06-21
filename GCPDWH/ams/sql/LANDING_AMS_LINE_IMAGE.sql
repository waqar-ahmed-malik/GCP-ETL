SELECT
  LineImage.UniqLineImage
  ,LineImage.UniqLine
  ,LineImage.EffectiveDate
  ,LineImage.UniqPersonalApplication
  ,LineImage.UniqCommercialApplication
  ,LineImage.UniqMarketingLine
  ,LineImage.UniqAgricultureApplication
  ,LineImage.UniqCFPolicyInformation
FROM dbo.LineImage
INNER JOIN dbo.Line
ON LineImage.UniqLine = Line.UniqLine
WHERE (SELECT CONVERT(DATE, MAX(v)) FROM (VALUES (Line.InsertedDate), (Line.UpdatedDate)) AS value(v)) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx);