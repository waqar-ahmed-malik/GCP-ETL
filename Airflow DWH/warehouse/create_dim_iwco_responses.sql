CREATE VIEW IF NOT EXISTS `usps-bigquery.data_warehouse.dim_iwco_responses`
AS
SELECT
    mac_id,
    mac_description,
    division_code,
    creative_code,
    campaign_code,
    campaign_description,
    batch_date,
    cycle_date,
    response_channel_code,
    extension_code,
    referring_url,
    iom_code,
    count
FROM 
    `usps-bigquery.operational.iwco_responses`