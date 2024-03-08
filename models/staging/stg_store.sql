with 
    dados_loja as (
      select  	
        businessentityid as storeid
        , name as storename
        , salespersonid
        , demographics
        , rowguid
        , modifieddate as  store_modifieddate   
      from {{ source('raw_data','store') }}
      )

select *
from dados_loja