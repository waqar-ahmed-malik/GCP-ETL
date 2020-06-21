SELECT 
	CONVERT(char, campaignid) AS campaignid,
	CONVERT(char, name) AS name,
	CONVERT(char, businesslinecode) AS businesslinecode,
	CONVERT(char, code) AS code,
	CONVERT(char, description) AS description,
	CONVERT(char, startdate) AS startdate,
	CONVERT(char, enddate) AS enddate,
	CONVERT(char, active) AS active
FROM mrm_sam_prd.dbo.campaigns