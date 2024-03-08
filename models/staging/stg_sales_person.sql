with 
    dados_salesperson as (
      select 
        businessentityid
        , territoryid
        , salesquota
        , bonus
        , commissionpct
        , salesytd
        , saleslastyear
        , rowguid
        , modifieddate
                      
      from {{ source('raw_data','salesperson') }}
      )

select *
from dados_salesperson