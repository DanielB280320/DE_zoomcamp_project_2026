WITH dim_region AS(
    SELECT
        DISTINCT region_id, 
        region_name

    FROM {{ref("int_us_housing_data")}}
)

SELECT
    *
    
FROM dim_region
ORDER BY region_name ASC