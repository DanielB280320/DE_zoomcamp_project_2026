SELECT 
    H.period_begin,
    H.period_end,
    S.state_name,
    T.region_type,
    H.duration,
    COUNT(*) AS Count_regions, 
    AVG(H.median_sale_price) AS Avg_median_sale_price,
    AVG(H.median_sale_ppsf) AS Avg_median_sale_ppsf,
    AVG(H.active_listings) AS Avg_active_listings,
    AVG(H.pct_active_listings_with_price_drops) * 100 AS Avg_active_listings_with_price_drops,
    AVG(H.median_days_on_market) AS Avg_median_days_on_market,
    AVG(H.adj_avg_homes_sold) AS Avg_homes_sold 
    
FROM {{ref("fact_housing_data")}} H
LEFT JOIN {{ref("dim_region_type")}} T
    ON H.region_type_id = T.region_type_id

LEFT JOIN {{ref("dim_region")}} R
    ON H.region_id = R.region_id

LEFT JOIN {{ref("dim_state")}} S
    ON H.state_code = S.state_code

WHERE H.duration = '1 weeks'
    AND T.region_type = 'metro'
GROUP BY 
    H.period_begin,
    H.period_end,
    S.state_name,
    T.region_type,
    H.duration
ORDER BY H.period_begin DESC
