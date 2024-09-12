{{ config(
    tags=["fact"],
    materialized='incremental',
    unique_key=['CommercialId', 'PurchaseOrderId']) }}

SELECT  {{ dbt_utils.star(from=ref('transform_fact_purchase_order')) }},
        CURRENT_TIMESTAMP AS LoadDate
FROM {{ ref('transform_fact_purchase_order') }}

 -- this filter will only be applied on an incremental run
 -- over-ride this filter by passing --full-refresh flag into dbt run
{% if is_incremental() %}
    WHERE WatermarkDate >= coalesce((select max(WatermarkDate) from {{ this }}), '1900-01-01')
{% endif %}