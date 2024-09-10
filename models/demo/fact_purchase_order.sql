{{ config(
    tags=["fact"]) }}

SELECT  com.CommercialId,
        po.PurchaseOrderId, 
        po.PONumber, 
        {{gen_yyyymmdd('po.CreatedDate')}} AS PurchaseOrderDate,
        {{ dbt_utils.generate_surrogate_key(['com.ContractId']) }} as CustomerContractKey,
        po.POAmount
FROM    pmo_portal.commercial com
INNER JOIN pmo_portal.purchase_order po on com.CommercialId = po.CommercialId
INNER JOIN pmo_portal.contract c on com.ContractId = c.ContractId