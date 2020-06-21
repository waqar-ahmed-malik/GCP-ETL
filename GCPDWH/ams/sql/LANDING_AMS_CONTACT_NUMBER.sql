SELECT *
FROM dbo.ContactNumber
WHERE (SELECT CONVERT(DATE, MAX(v)) FROM (VALUES (InsertedDate), (UpdatedDate)) AS value(v)) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx);