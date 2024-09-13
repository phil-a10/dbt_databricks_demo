
{{ config(
    tags=["fact"],
    materialized='incremental',
    on_schema_change='sync_all_columns',  
    unique_key=['CommercialId', 'PurchaseOrderId']) }}
-- on_schema_change - will add and remove all columns on schema change

SELECT  {{ dbt_utils.star(from=ref('transform_fact_purchase_order')) }},
        CURRENT_TIMESTAMP AS LoadDate
FROM {{ ref('transform_fact_purchase_order') }}

 -- this filter will only be applied on an incremental run
 -- over-ride this filter by passing --full-refresh flag into dbt run
 -- use select "+fact_purchase_order" to run this and dependent models
{% if is_incremental() %}
    WHERE WatermarkDate >= coalesce((select max(WatermarkDate) from {{ this }}), '1900-01-01')
{% endif %}