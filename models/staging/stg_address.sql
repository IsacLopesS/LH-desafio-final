with 
    dados_endereco as (
      select 
      addressid
      , addressline1
      , addressline2
      , city
      , stateprovinceid
      , postalcode
      , spatiallocation
      , rowguid
      , modifieddate
              
      from {{ source('raw_data','address') }}
      )

select *
from dados_endereco