SELECT
  PAImage.UniqPAImage
  ,PAImage.UniqLineImage
FROM dbo.PAImage
INNER JOIN dbo.LineImage
ON PAImage.UniqLineImage = LineImage.UniqLineImage
INNER JOIN dbo.Line
ON LineImage.UniqLine = Line.UniqLine
WHERE (SELECT CONVERT(DATE, MAX(v)) FROM (VALUES (Line.InsertedDate), (Line.UpdatedDate)) AS value(v)) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx);