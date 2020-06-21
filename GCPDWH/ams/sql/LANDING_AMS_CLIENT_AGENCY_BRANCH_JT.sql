SELECT DISTINCT
  ClientAgencyBranchJT.UniqEntity
  ,ClientAgencyBranchJT.UniqAgency
  ,ClientAgencyBranchJT.UniqBranch
FROM dbo.ClientAgencyBranchJT
INNER JOIN dbo.StructureCombination
ON ClientAgencyBranchJT.UniqAgency = StructureCombination.UniqAgency
AND ClientAgencyBranchJT.UniqBranch = StructureCombination.UniqBranch
WHERE (SELECT CONVERT(DATE, MAX(v)) FROM (VALUES (StructureCombination.InsertedDate), (StructureCombination.UpdatedDate)) AS value(v)) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx);