{% {% snapshot snapshot_client %}

{{
   config(
       target_schema='dbt',
       unique_key='ClientId',
       strategy='check',
       check_cols=['AccountManager', 'ExecSponsor']
   )
}}
SELECT 
        ClientId, 
        AccountManager, 
        ExecSponsor
FROM 
        {{ source('pmo_portal', 'client') }}

{% endsnapshot %}%}