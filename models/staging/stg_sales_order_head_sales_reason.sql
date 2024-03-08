with 
    id_da_razao_da_compra as (
      select
          salesorderid
          , salesreasonid
          , modifieddate

      from {{source('raw_data', 'salesorderheadersalesreason') }}
    )

select *
from id_da_razao_da_compra