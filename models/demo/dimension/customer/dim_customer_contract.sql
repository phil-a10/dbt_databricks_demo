{{ config(
    tags=["dim"]) }}

select  
        {{ dbt_utils.generate_surrogate_key(['con.ContractId','cls.db_valid_from','cls.db_valid_to']) }} as CustomerContractKey,
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
        con.CDL,
        cls.db_valid_from as ValidFrom,
        cls.dbt_valid_to as ValidTo 
from {{source('pmo_portal', 'client')}} cl
inner join dbt.snapshot_client cls on cl.ClientId = cls.ClientId
inner join {{source('pmo_portal', 'contract')}} con on cl.ClientId = con.ClientId