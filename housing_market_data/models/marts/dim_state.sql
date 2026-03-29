WITH dim_state AS(
    SELECT
        DISTINCT state_code, 
        state_name,
        region,
        division

    FROM {{ref("int_us_housing_data")}}
)

SELECT
    *

FROM dim_state
ORDER BY state_code ASC