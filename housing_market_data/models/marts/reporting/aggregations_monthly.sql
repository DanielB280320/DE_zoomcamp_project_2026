SELECT 
    H.period_begin,
    H.period_end,
    S.state_name,
    T.region_type,
    R.region_name, 
    H.duration,
    MAX(median_sale_price) AS Max_median_sale_price,
    MAX(adj_avg_homes_sold) AS Max_avg_homes_sold,
    MAX(H.active_listings) AS Max_active_listings,
    MAX(H.pct_active_listings_with_price_drops) * 100 AS Max_active_listings_with_price_drops

FROM {{ref("fact_housing_data")}} H
LEFT JOIN {{ref("dim_region_type")}} T
    ON H.region_type_id = T.region_type_id

LEFT JOIN {{ref("dim_region")}} R
    ON H.region_id = R.region_id

LEFT JOIN {{ref("dim_state")}} S
    ON H.state_code = S.state_code

WHERE H.duration = '4 weeks'
    AND T.region_type = 'metro'
GROUP BY 
    H.period_begin,
    H.period_end,
    S.state_name,
    T.region_type,
    R.region_name, 
    H.duration
ORDER BY Max_median_sale_price DESC