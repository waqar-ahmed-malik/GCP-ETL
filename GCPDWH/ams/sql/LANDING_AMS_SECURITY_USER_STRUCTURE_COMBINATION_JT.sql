SELECT
  SecurityUserStructureCombinationJT.UniqSecurityUser
  ,SecurityUserStructureCombinationJT.UniqStructure
FROM dbo.SecurityUserStructureCombinationJT
INNER JOIN dbo.StructureCombination
ON SecurityUserStructureCombinationJT.UniqStructure = StructureCombination.UniqStructure
WHERE (SELECT CONVERT(DATE, MAX(v)) FROM (VALUES (StructureCombination.InsertedDate), (StructureCombination.UpdatedDate)) AS value(v)) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx);