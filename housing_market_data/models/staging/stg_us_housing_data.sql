SELECT 
  -- TIMESTAMPS
  CAST(PERIOD_BEGIN AS DATETIME) AS period_begin, 
  CAST(PERIOD_END AS DATETIME) AS period_end,
  -- REGION DATA  
  CAST(REGION_TYPE AS STRING)  AS region_type, 
  CAST(REGION_TYPE_ID AS INT) AS region_type_id, 
  CAST(REGION_NAME AS STRING) AS region_name, 
  CAST(REGION_ID AS INT) AS region_id,
  CAST(DURATION AS STRING) AS duration,
  -- METRICS CURRENT
  CAST(ACTIVE_LISTINGS AS INT) AS active_listings,
  CAST(PERCENT_ACTIVE_LISTINGS_WITH_PRICE_DROPS AS NUMERIC) AS pct_active_listings_with_price_drops,
  CAST(ADJUSTED_AVERAGE_NEW_LISTINGS AS NUMERIC) AS adj_avg_new_listings,
  CAST(AVERAGE_PENDING_SALES_LISTING_UPDATES AS NUMERIC) AS avg_pending_sales_listing_updates,
  CAST(ADJUSTED_AVERAGE_HOMES_SOLD AS NUMERIC) AS adj_avg_homes_sold,
  CAST(AVERAGE_SALE_TO_LIST_RATIO AS NUMERIC) AS avg_sale_to_list_ratio,
  CAST(MEDIAN_SALE_PRICE AS NUMERIC) AS median_sale_price,
  CAST(MEDIAN_SALE_PPSF AS NUMERIC) AS median_sale_ppsf,
  CAST(MEDIAN_NEW_LISTING_PRICE AS NUMERIC) AS median_new_listing_price, 
  CAST(MEDIAN_NEW_LISTING_PPSF AS NUMERIC) AS median_new_listing_ppsf, 
  CAST(MEDIAN_DAYS_ON_MARKET AS NUMERIC) AS median_days_on_market,
  CAST(MEDIAN_DAYS_TO_CLOSE AS NUMERIC) AS median_days_to_close, 
  CAST(MEDIAN_PENDING_SQFT AS NUMERIC) AS median_pending_sqft, 
  CAST(AGE_OF_INVENTORY AS NUMERIC) AS age_of_inventory, 
  CAST(WEEKS_OF_SUPPLY AS NUMERIC) AS weeks_of_supply,
  CAST(OFF_MARKET_IN_TWO_WEEKS AS INT) AS off_market_in_two_weeks, 
  -- METRICS YOY%
  CAST(ACTIVE_LISTINGS_YOY AS NUMERIC) AS active_listings_yoy,
  CAST(PERCENT_ACTIVE_LISTINGS_WITH_PRICE_DROPS_YOY AS NUMERIC) AS pct_active_listings_with_price_drops_yoy, 
  CAST(ADJUSTED_AVERAGE_NEW_LISTINGS_YOY AS NUMERIC) AS adj_avg_new_listings_yoy,
  CAST(AVERAGE_PENDING_SALES_LISTING_UPDATES_YOY AS NUMERIC) AS avg_pending_sales_listing_updates_yoy, 
  CAST(ADJUSTED_AVERAGE_HOMES_SOLD_YOY AS NUMERIC) AS adj_avg_homes_sold_yoy, 
  CAST(AVERAGE_SALE_TO_LIST_RATIO_YOY AS NUMERIC) AS avg_sale_to_list_ratio_yoy, 
  CAST(MEDIAN_SALE_PRICE_YOY AS NUMERIC) AS median_sale_price_yoy,
  CAST(MEDIAN_SALE_PPSF_YOY AS NUMERIC) AS median_sale_ppsf_yoy,
  CAST(MEDIAN_NEW_LISTING_PRICE_YOY AS NUMERIC) AS median_new_listing_price_yoy, 
  CAST(MEDIAN_NEW_LISTING_PPSF_YOY AS NUMERIC) AS median_new_listing_ppsf_yoy,
  CAST(MEDIAN_DAYS_ON_MARKET_YOY AS NUMERIC) AS median_days_on_market_yoy, 
  CAST(MEDIAN_DAYS_TO_CLOSE_YOY AS NUMERIC) AS median_days_to_close_yoy,
  CAST(MEDIAN_PENDING_SQFT_YOY AS NUMERIC) AS median_pending_sqft_yoy,
  CAST(AGE_OF_INVENTORY_YOY AS NUMERIC) AS age_of_inventory_yoy, 
  CAST(WEEKS_OF_SUPPLY_YOY AS NUMERIC) AS weeks_of_supply_yoy, 
  CAST(OFF_MARKET_IN_TWO_WEEKS_YOY AS NUMERIC) AS off_market_in_two_weeks_yoy,
  -- LAST UPDATED DATE 
  CAST(LAST_UPDATED AS TIMESTAMP)AS last_updated

FROM {{source(
    'raw_housing_market_data', 
    'market_housing_data_consolidated'
    )
}}
--ORDER BY period_begin DESC