CREATE VIEW IF NOT EXISTS `usps-bigquery.data_warehouse.dim_cost`
AS
SELECT
    quantity_impressions,
    media_cost,
    rental_list_cost,
    list_data_processing_cost,
    image_purchase_cost,
    print_production_cost,
    tech_cost,
    studio_art_retouching_cost,
    travel_cost,
    total_cost,
    load_timestamp
FROM 
    `usps-bigquery.operational.cost` 