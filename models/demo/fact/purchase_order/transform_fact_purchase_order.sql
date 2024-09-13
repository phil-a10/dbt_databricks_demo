{{ config(
    tags=["transform"],
    materialized="view") }}

SELECT  com.CommercialId,
        com.ContractId,
        po.PurchaseOrderId, 
        {{gen_yyyymmdd('po.CreatedDate')}} AS PurchaseOrderDateKey,
        {{ dbt_utils.generate_surrogate_key(['con.ContractId','cls.db_valid_from','cls.db_valid_to']) }} AS CustomerContractKey,
        {{ dbt_utils.generate_surrogate_key(['po.PurchaseOrderId']) }} AS PurchaseOrderKey,
        po.POAmount,
        CASE    WHEN coalesce(com.UpdatedDate, com.CreatedDate) > coalesce(po.UpdatedDate, po.CreatedDate) 
                THEN coalesce(com.UpdatedDate, com.CreatedDate)
                ELSE coalesce(po.UpdatedDate, po.CreatedDate)
        END AS WatermarkDate        
FROM    {{source('pmo_portal', 'commercial')}} com
INNER JOIN {{source('pmo_portal', 'purchase_order')}} po ON com.CommercialId = po.CommercialId
INNER JOIN dbt.snapshot_client scl ON com. = scl.ContractId AND scl.dbt_valid_from >= po.CreatedDate AND scl.dbt_valid_to < po.CreatedDate
