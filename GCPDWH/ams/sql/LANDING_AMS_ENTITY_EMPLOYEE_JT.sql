SELECT
  EntityEmployeeJT.UniqEntity
  ,EntityEmployeeJT.UniqCdServicingRole
  ,EntityEmployeeJT.UniqEmployee
  ,EntityEmployeeJT.RoleDescription
  ,EntityEmployeeJT.UpdatedByProcess
  ,EntityEmployeeJT.InsertedByProcess
FROM dbo.EntityEmployeeJT
INNER JOIN dbo.Client
ON EntityEmployeeJT.UniqEntity = Client.UniqEntity
WHERE (SELECT CONVERT(DATE, MAX(v)) FROM (VALUES (Client.InsertedDate), (Client.UpdatedDate)) AS value(v)) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx);