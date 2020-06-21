CREATE OR REPLACE VIEW `usps-bigquery.data_warehouse.dim_anp_leads`
AS
SELECT
    mac_id,
    mac_description,
    media,
    product_group,
    lead_status,
    lead_owner,
    lead_owner_user_type,
    lead_district,
    lead_owner_area,
    estimated_revenue,
    dismissed_reason,
    created_date,
    last_modified_date
FROM 
    `usps-bigquery.operational.anp_leads`