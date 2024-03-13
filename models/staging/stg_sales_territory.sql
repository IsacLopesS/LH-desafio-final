with 
    dados_territorio_venda as (
      select
          territoryid
          , name as sales_territory_name
          , countryregioncode
          , `group` as sales_territory_group
          , salesytd
          , saleslastyear
          , costytd
          , costlastyear
          , rowguid
          , modifieddate

      from {{source('raw_data', 'salesterritory') }}
    )

select *
from dados_territorio_venda