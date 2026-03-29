WITH int_housing_data AS (
SELECT 
    --TIMESTAMPS
    period_begin, 
    period_end,
    --REGION DATA 
    region_type, 
    region_type_id, 
    SPLIT(region_name, ',')[SAFE_OFFSET(0)] AS region_name, 
    TRIM(SUBSTRING(
        SPLIT(region_name, ',')[SAFE_OFFSET(1)], 1, 3
    )) AS state_code, 
    region_id,
    duration,
    --METRICS CURRENT
    active_listings,
    pct_active_listings_with_price_drops,
    adj_avg_new_listings,
    avg_pending_sales_listing_updates,
    adj_avg_homes_sold,
    avg_sale_to_list_ratio,
    median_sale_price,
    median_sale_ppsf,
    median_new_listing_price, 
    median_new_listing_ppsf, 
    median_days_on_market,
    median_days_to_close, 
    median_pending_sqft, 
    age_of_inventory, 
    weeks_of_supply,
    off_market_in_two_weeks,
    -- METRICS YOY% 
    active_listings_yoy,
    pct_active_listings_with_price_drops_yoy, 
    adj_avg_new_listings_yoy,
    avg_pending_sales_listing_updates_yoy, 
    adj_avg_homes_sold_yoy, 
    avg_sale_to_list_ratio_yoy, 
    median_sale_price_yoy,
    median_sale_ppsf_yoy,
    median_new_listing_price_yoy, 
    median_new_listing_ppsf_yoy,
    median_days_on_market_yoy, 
    median_days_to_close_yoy,
    median_pending_sqft_yoy,
    age_of_inventory_yoy, 
    weeks_of_supply_yoy, 
    off_market_in_two_weeks_yoy,
    -- LAST UPDATED DATE 
    last_updated

FROM {{ref('stg_us_housing_data')}}
WHERE active_listings IS NOT NULL
), 

cleaned_housing_data AS (
SELECT 
    --TIMESTAMPS
    H.period_begin, 
    H.period_end,
    --REGION DATA 
    H.region_type, 
    H.region_type_id, 
    H.region_name, 
    H.state_code,
    S.state_name,
    S.region,
    S.division,   
    H.region_id,
    H.duration,
    --METRICS CURRENT
    H.active_listings,
    H.pct_active_listings_with_price_drops,
    H.adj_avg_new_listings,
    H.avg_pending_sales_listing_updates,
    H.adj_avg_homes_sold,
    H.avg_sale_to_list_ratio,
    H.median_sale_price,
    H.median_sale_ppsf,
    H.median_new_listing_price, 
    H.median_new_listing_ppsf, 
    H.median_days_on_market,
    H.median_days_to_close, 
    H.median_pending_sqft, 
    H.age_of_inventory, 
    H.weeks_of_supply,
    H.off_market_in_two_weeks,
    -- METRICS YOY% 
    H.active_listings_yoy,
    H.pct_active_listings_with_price_drops_yoy, 
    H.adj_avg_new_listings_yoy,
    H.avg_pending_sales_listing_updates_yoy, 
    H.adj_avg_homes_sold_yoy, 
    H.avg_sale_to_list_ratio_yoy, 
    H.median_sale_price_yoy,
    H.median_sale_ppsf_yoy,
    H.median_new_listing_price_yoy, 
    H.median_new_listing_ppsf_yoy,
    H.median_days_on_market_yoy, 
    H.median_days_to_close_yoy,
    H.median_pending_sqft_yoy,
    H.age_of_inventory_yoy, 
    H.weeks_of_supply_yoy, 
    H.off_market_in_two_weeks_yoy,
    -- LAST UPDATED DATE 
    H.last_updated

FROM int_housing_data H 
LEFT JOIN {{ref('us_states')}} S
    ON H.state_code = S.state_code_id

WHERE state_code IS NOT NULL
QUALIFY (ROW_NUMBER() OVER(
    PARTITION BY 
        H.period_begin, 
        H.period_end,
        H.region_type,  
        H.region_name, 
        S.state_name,
        H.duration,  
        H.active_listings
    ORDER BY 
        H.period_begin,
        H.period_end
    )
) = 1
)

SELECT 
   *

FROM cleaned_housing_data




