with 
    dados_pais as (
      select 
      countryregioncode
      , name as country_name
      , modifieddate
              
      from {{ source('raw_data','countryregion') }}
      )

select *
from dados_pais