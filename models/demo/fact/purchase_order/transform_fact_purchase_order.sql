{{ config(
    tags=["transform"]) }}

SELECT  com.CommercialId,
        com.ContractId,
        po.PurchaseOrderId, 
        po.PONumber, 
        {{gen_yyyymmdd('po.CreatedDate')}} AS PurchaseOrderDateKey,
        {{ dbt_utils.generate_surrogate_key(['com.ContractId']) }} AS CustomerContractKey,
        po.POAmount,
        CASE    WHEN coalesce(com.UpdatedDate, com.CreatedDate) > coalesce(po.UpdatedDate, po.CreatedDate) 
                THEN coalesce(com.UpdatedDate, com.CreatedDate)
                ELSE coalesce(po.UpdatedDate, po.CreatedDate)
        END AS WatermarkDate        
FROM    {{source('pmo_portal', 'commercial')}} com
INNER JOIN {{source('pmo_portal', 'purchase_order')}} po on com.CommercialId = po.CommercialId
