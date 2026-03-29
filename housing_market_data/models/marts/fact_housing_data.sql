WITH fact_housing_data AS(
    SELECT
        {{dbt_utils.generate_surrogate_key([
            'period_begin', 
            'state_code', 
            'region_id'
            ])
        }} AS unique_id,
        * EXCEPT(
            region_type,
            region_name, 
            state_name,
            region,
            division
        )

    FROM {{ref("int_us_housing_data")}}
)

SELECT
    *

FROM fact_housing_data
--ORDER BY state_code ASC