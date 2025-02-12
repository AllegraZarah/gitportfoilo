{{ config (materialized = 'ephemeral') }}

WITH transport_matrix AS ( -- This is the cost of moving products across multiple locations and is used to calculate the final cost of the product purchased.
    SELECT * 
    FROM {{ source('analytics_matrix', 'transport_matrix') }}
)

SELECT * 
FROM transport_matrix