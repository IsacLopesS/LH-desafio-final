with 
    dados_territorio_venda as (
      select
          territoryid
          , sales_territory_name
          , countryregioncode
          , sales_territory_group
          , salesytd
          , saleslastyear
          , costytd
          , costlastyear
          , rowguid
          , modifieddate

      from {{ref('stg_sales_territory') }}
    )

select *
from dados_territorio_venda