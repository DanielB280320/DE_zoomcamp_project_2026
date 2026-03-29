WITH dim_region_type AS(
    SELECT
        DISTINCT region_type_id, 
        region_type

    FROM {{ref("int_us_housing_data")}}
)

SELECT
    *
    
FROM dim_region_type
ORDER BY region_type ASC