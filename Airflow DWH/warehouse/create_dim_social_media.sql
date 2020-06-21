CREATE OR REPLACE VIEW `usps-bigquery.data_warehouse.dim_social_media`
AS
SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'facebook' AS social_media_type,
    ad_format,
    ad_group,
    ad_name,
    campaign_sub_product,
    date(full_date) as date,
    campaign_name
    quarter,
    week,
    campaign
    date 
    promoted_pin_name
FROM
    `usps-bigquery.operational.social_facebook`

UNION DISTINCT

SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'twitter' AS social_media_type,
    ad_format,
    ad_group,
    ad_name,
    campaign_sub_product,
    date(full_date) as date,
    campaign_name
    quarter,
    week,
    campaign
    date 
    promoted_pin_name
FROM
    `usps-bigquery.operational.social_twitter`

UNION DISTINCT

SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'pinterest' AS social_media_type,
    ad_format,
    ad_group,
    ad_name,
    campaign_sub_product,
    date(full_date) as date,
    campaign_name
    quarter,
    week,
    campaign
    date 
    promoted_pin_name
FROM
    `usps-bigquery.operational.social_pinterest`

UNION DISTINCT

SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'reddit' AS social_media_type,
    ad_format,
    ad_group,
    ad_name,
    campaign_sub_product,
    date(full_date) as date,
    campaign_name
    quarter,
    week,
    campaign
    date 
    promoted_pin_name
FROM
    `usps-bigquery.operational.social_reddit`

UNION DISTINCT

SELECT
    CONCAT(campaign_name, ad_group, ad_name) as dcm_id,
    'linkedin' AS social_media_type,
    ad_format,
    ad_group,
    ad_name,
    campaign_sub_product,
    date(full_date) as date,
    campaign_name
    quarter,
    week,
    campaign
    date 
    promoted_pin_name
FROM
    `usps-bigquery.operational.social_linkedin`