{{ config(
    tags=["dim"]) }}

SELECT 
        {{ dbt_utils.generate_surrogate_key(['PurchaseOrderId']) }} as PurchaseOrderKey,
        PONumber,
        Notes,
        Active
FROM {{ source('pmo_portal', 'purchase_order') }}
