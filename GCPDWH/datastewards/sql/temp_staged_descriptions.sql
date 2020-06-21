WITH table_filter AS (
  SELECT 
    aa.table_catalog, 
    aa.table_schema, 
    aa.table_name
  FROM (
    SELECT * FROM CUSTOMER_PRODUCT.INFORMATION_SCHEMA.TABLES
    UNION ALL 
    SELECT * FROM CUSTOMERS.INFORMATION_SCHEMA.TABLES
    UNION ALL
    SELECT * FROM OPERATIONAL.INFORMATION_SCHEMA.TABLES
    UNION ALL 
    SELECT * FROM REFERENCE.INFORMATION_SCHEMA.TABLES
    ) AA
  JOIN (
  	SELECT DISTINCT 
  		dataset, 
  		table_name AS sheet_table_name 
  	FROM LANDING.field_descriptions) BB ON (
  	aa.table_schema = bb.dataset 
  	AND aa.table_name = bb.sheet_table_name)
), corrected_columns AS (
   SELECT 
    aa.table_catalog, 
    aa.table_schema, 
    aa.table_name, 
    aa.column_name AS name, 
    aa.ordinal_position, 
    aa.data_type AS type,
    coalesce(cc.description, 'TBD') as description,
    CASE WHEN aa.is_nullable = 'YES' THEN 'NULLABLE' ELSE 'REQUIRED' END AS mode
   FROM (
    SELECT * FROM CUSTOMER_PRODUCT.INFORMATION_SCHEMA.COLUMNS
    UNION ALL 
    SELECT * FROM CUSTOMERS.INFORMATION_SCHEMA.COLUMNS
    UNION ALL
    SELECT * FROM OPERATIONAL.INFORMATION_SCHEMA.COLUMNS
    UNION ALL 
    SELECT * FROM REFERENCE.INFORMATION_SCHEMA.COLUMNS
   ) aa
   JOIN table_filter bb ON (aa.table_schema = bb.table_schema AND aa.table_name = bb.table_name)
   LEFT JOIN LANDING.field_descriptions cc ON (aa.table_schema = cc.dataset AND aa.table_name = cc.table_name AND aa.column_name = cc.column_name)
 ), completed_fields AS (
 SELECT
  table_catalog,
  table_schema,
  table_name,
  description,
  name,
  type,
  mode
 FROM corrected_columns) 
SELECT
  table_catalog,
  table_schema,
  table_name,
  description,
  name,
  type,
  mode
FROM completed_fields
ORDER BY 1,2,3