{{ config(
    tags=['layer3'],
    materialized='view') }}

WITH source AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_status,
        total_amount,
        payment_method,
        shipping_city,
        shipping_state
    FROM dbt_cloud.raw_schema.orders),

transformed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        TRY_TO_DATE(order_date) AS order_date_clean,
        order_status,
        INITCAP(order_status) AS order_status_clean,
        total_amount,
        payment_method,
        UPPER(payment_method) AS payment_method_std,
        shipping_city,
        INITCAP(shipping_city) AS shipping_city_clean,
        shipping_state,
        UPPER(shipping_state) AS shipping_state_code,
        YEAR(order_date) AS order_year,
        MONTH(order_date) AS order_month,
        CASE 
            WHEN total_amount > 400 THEN 'High'
            WHEN total_amount > 200 THEN 'Medium'
            ELSE 'Low'
        END AS order_value_category
    FROM source
)

SELECT * FROM transformed
