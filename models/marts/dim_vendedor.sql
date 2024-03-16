with 
  dados_vendedor as (
    select 
        businessentityid
        , jobtitle
        , persontype
        , complete_name
        , emailaddress
        , salesquota
        , bonus
        , commissionpct
        , salesytd
        , saleslastyear
        , territoryid
        , sales_territory_name
        , countryregioncode
        , sales_territory_group
        , costytd
        , costlastyear
    from {{ ref('int_vendedor') }}
  )

select * 
from dados_vendedor