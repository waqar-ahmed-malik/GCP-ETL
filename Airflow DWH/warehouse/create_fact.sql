CREATE VIEW IF NOT EXISTS `usps-bigquery.data_warehouse.fact`
AS
SELECT
    fact_id,
    mac_id
    medallia_id
    dcm_id (campaign + adgroup + adname)
    social_media_id(type + campaign + adgroup + adname)
    Ga_websites_visit_id
    full_visitor_id
    tota_form_completes_not_cross_environment
    total_landing_page_hits_not_cross_environment
    total_promo_reg_page_hits_not_cross_environment
    total_revenue_not_cross_environment
    clicks
    impressions
    ctr
    creative_pixel_size
    spend
    form_completes (SUM)
    landing_page_hits (SUM)
    total_promo_reg_page_hits (SUM)
    revenue (SUM)
    clicks (SUM)
    full_date
    impressions (SUM)
    video_completes_capped (SUM)
    video_playes_capped (SUM)
    cost per click
    cost per form complete
    cost per landing page hit
    click through rate
    effective_cpm
    form_completion_rate
    quarter
    visit_rate
    week
    cost_per_view
    video_completion_rate
    totals.bounces
    totals.hits
    totals.newVisits
    totals.pageviews
    totals.screenviews
    totals.sessionQualityDim
    totals.timeOnScreen
    totals.timeOnSite
    totals.totalTransactionRevenue
    totals.transactionRevenue
    totals.transactions
    totals.UniqueScreenViews
    totals.visits
    customDimensions.value
    geoNetwork.latitude
    geoNetwork.longitude
    hits.exceptionInfo.fatalExceptions
    hits.hitNumber
    hits.hour
    hits.latencyTracking.domainLookupTime
    hits.latencyTracking.domContentLoadedTime
    hits.latencyTracking.domInteractiveTime
    hits.latencyTracking.domLatencyMetricsSample
    hits.latencyTracking.pageDownloadTime
    hits.latencyTracking.pageLoadSample
    hits.latencyTracking.pageLoadTime
    hits.latencyTracking.redirectionTime
    hits.latencyTracking.serverConnectionTime
    hits.latencyTracking.serverResponseTime
    hits.latencyTracking.speedMetricsSample
    hits.latencyTracking.userTimingValue
    hits.minute
    hits.product.isClick
    hits.product.customDimensions
    hits.product.customDimensions.value
    hits.publisher.adsenseBackfillDfpClicks
    hits.publisher.adsenseBackfillDfpImpressions
    hits.publisher.adsenseBackfillDfpMatchedQueries
    hits.publisher.adsenseBackfillDfpMeasurableImpressions
    hits.publisheradsenseBackfillDfpPagesViewed
    hits.publisher.adsenseBackfillDfpQueries
    hits.publisher.adsenseBackfillDfpRevenueCpc
    hits.publisher.adsenseBackfillDfpRevenueCpm
    hits.publisher.adsenseBackfillDfpViewableImpressions
    hits.publisher.adxBackfillDfpClicks
    hits.publisher.adxBackfillDfpImpressions
    hits.publisher.adxBackfillDfpMatchedQueries
    hits.publisher.adxBackfillDfpMeasurableImpressions
    hits.publisher.adxBackfillDfpPagesViewed
    hits.publisher.adxBackfillDfpQueries
    hits.publisher.adxBackfillDfpRevenueCpc
    hits.publisher.adxBackfillDfpRevenueCpm
    hits.publisher.adxBackfillDfpViewableImpressions
    hits.publisher.dfpClicks
    hits.publisher.dfpImpressions
    hits.publisher.dfpMatchedQueries
    hits.publisher.dfpMeasurableImpressions
    hits.publisher.dfpPagesViewed
    hits.publisher.dfpQueries
    hits.publisher.dfpRevenueCpc
    hits.publisher.dfpRevenueCpm
    hits.publisher.dfpViewableImpressions
    hits.time
    hits.refund.localRefundAmount
    hits.refund.refundAmount
    hits.page
    hits.product.localProductPrice
    hits.product.localProductRefundAmount
    hits.product.localProductRevenue
    hits.product.productPrice
    hits.product.productQuantity
    hits.product.productRefundAmount
    hits.product.productRevenue
    hits.product.productSKU
    hits.promotionActionInfo.promoIsView
    hits.promotionActionInfo.promoIsClick
    hits.transaction.transactionRevenue
    hits.transaction.transactionTax
    hits.transaction.transactionShipping
    hits.transaction.localTransactionRevenue
    hits.transaction.localTransactionTax
    hits.transaction.localTransactionShipping
    hits.item.productSku
    hits.item.itemQuantity
    hits.item.itemRevenue
    hits.item.localItemRevenue
    hits.eventInfo.eventValue
    hits.customVariables.customVarValue
    hits.customDimensions.value
    hits.customMetrics.value
    Revenue_FY19
    Transactions_FY19
    hits.Click URL - Link Target
    hits.Form Target
    rt:users
    rt:pageviews 
    rt:totalEvents 
    impressions
    media_cost
    rental_list_cost
    list_processing_cost
    image_purchase_cost
    print_production_cost
    tech_cost
    studio_art_retouching_cost
    travel_cost
    total_cost
    landing_page_visits
    -- Landing Page Visits (SUM)
    -- Clicks (SUM)
    -- Impressions (SUM)
    -- KPI for Form Completes (SUM)
    -- Revenue (SUM)
    -- Spend (SUM)
    -- Quantity/Impressions

FROM
    `usps-bigquery.operational.{}` --paramterize query