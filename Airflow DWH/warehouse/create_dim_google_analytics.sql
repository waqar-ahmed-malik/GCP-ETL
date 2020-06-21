CREATE VIEW IF NOT EXISTS `usps-bigquery.data_warehouse.dim_google_analytics`
AS
SELECT
    full_visitor_id,
    client_id,
    visitor_id,
    visit_number,
    visit_id,
    visit_start_time
FROM 
    `usps-bigquery.operational.google_analytics`