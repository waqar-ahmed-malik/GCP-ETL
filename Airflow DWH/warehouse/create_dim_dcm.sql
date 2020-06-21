CREATE OR REPLACE VIEW `usps-bigquery.data_warehouse.dim_dcm`
AS
SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'display' AS dcm_type,
    ad_format,
    ad_group,
    ad_name,
    creative,
    -- creative_pixel_size,
    campaign_sub_product,
    site_name_universal,
    -- clean_site,
    device,
    sub_site,
    sub_tactic,
    tactic,
    campaign_name,
    quarter,
    week,
    date(full_date) as date,
    country
FROM 
    `usps-bigquery.operational.dcm_display` 

UNION DISTINCT

SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'audio' AS dcm_type,
    ad_format,
    ad_group,
    ad_name,
    creative,
    -- creative_pixel_size,
    campaign_sub_product,
    site_name_universal,
    -- clean_site,
    device,
    sub_site,
    sub_tactic,
    tactic,
    campaign_name,
    quarter,
    week,
    date(full_date) as date,
    country
FROM 
    `usps-bigquery.operational.dcm_audio`

UNION DISTINCT

SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'video' AS dcm_type,
    ad_format,
    ad_group,
    ad_name,
    creative,
    -- creative_pixel_size,
    campaign_sub_product,
    site_name_universal,
    -- clean_site,
    device,
    sub_site,
    sub_tactic,
    tactic,
    campaign_name,
    quarter,
    week,
    date(full_date) as date,
    country
FROM 
    `usps-bigquery.operational.dcm_video` 