with 
    razao_da_compra as (
      select
          salesreasonid
          , name as reason_name
          , reasontype
          , modifieddate

      from {{source('raw_data', 'salesreason') }}
    )

select *
from razao_da_compra