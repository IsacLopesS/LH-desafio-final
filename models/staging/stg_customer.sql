with 
    dados_cliente as (
      select 
        customerid
        , personid
        , storeid
        , territoryid
        , rowguid
        , cast(modifieddate as TIMESTAMP) as modifieddate
      
      from {{ source('raw_data','customer') }}
      )

select *
from dados_cliente