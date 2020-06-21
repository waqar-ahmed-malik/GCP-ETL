SELECT 
	dataset, 
	table_name, 
	column_name, 
	description
FROM LANDING.dwh_data_dictionary
WHERE column_name IS NOT NULL 
AND column_name <> '<FUTURE FIELDS>'