with 
    dados_estado as (
      select 
        stateprovinceid
        , stateprovincecode
        , countryregioncode
        , isonlystateprovinceflag
        , name as stateprovince_name
        , territoryid
        , rowguid
        , modifieddate
              
      from {{ source('raw_data','stateprovince') }}
      )

select *
from dados_estado