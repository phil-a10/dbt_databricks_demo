{{ config(
    tags=["dim"],
    materialization='table') }}

select  
        {{ dbt_utils.generate_surrogate_key(['con.ContractId']) }} as CustomerContractKey,
        cl.ClientId AS CustomerId,
        con.ContractId AS ContractId,
        cl.Client AS CustomerName,
        cl.ClientShortCode AS CustomerShortCode,
        cl.AccountManager,
        cl.ExecSponsor,
        con.Name AS ContractName,
        cl.Active AS CustomerActiveFlag,
        con.ContractValue,
        con.ScrumMaster,
        con.CDL
from pmo_portal.client cl
inner join pmo_portal.contract con on cl.ClientId = con.ClientId