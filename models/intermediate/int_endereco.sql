with 
    dados_pais as (
      select 
      countryregioncode
      , country_name
      , modifieddate
              
      from {{ ref('stg_country_region') }}
    )
,   dados_territorio_venda as (
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

,   dados_estado as (
      select 
        stateprovinceid
        , stateprovincecode
        , countryregioncode
        , isonlystateprovinceflag
        , stateprovince_name
        , territoryid
        , rowguid
        , modifieddate
              
      from {{ ref('stg_state_province') }}
      )        

,     dados_endereco as (
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
              
      from {{ ref('stg_address') }}
      )

,     join_pais_estado as (
        select 
          dados_pais.countryregioncode
          , dados_pais.country_name
          , dados_estado.stateprovinceid
          , dados_estado.stateprovincecode
          , dados_estado.isonlystateprovinceflag
          , dados_estado.stateprovince_name
          , dados_estado.territoryid
          , dados_estado.modifieddate
          from dados_estado
        left join dados_pais
          on dados_estado.countryregioncode = dados_pais.countryregioncode

      )  

,     join_territorio_venda_pais_estado as (
        select 
          dados_territorio_venda.territoryid
          , dados_territorio_venda.sales_territory_name
          , dados_territorio_venda.sales_territory_group
          , dados_territorio_venda.salesytd
          , dados_territorio_venda.saleslastyear
          , dados_territorio_venda.costytd
          , dados_territorio_venda.costlastyear
          , join_pais_estado.countryregioncode
          , join_pais_estado.country_name
          , join_pais_estado.stateprovinceid
          , join_pais_estado.stateprovincecode
          , join_pais_estado.isonlystateprovinceflag
          , join_pais_estado.stateprovince_name
        from join_pais_estado
        left join dados_territorio_venda
          on dados_territorio_venda.territoryid = join_pais_estado.territoryid
      ) 

,     final_join_endereco as (
        select 
          dados_endereco.addressid
          , dados_endereco.addressline1
          , dados_endereco.addressline2
          , dados_endereco.city
          , dados_endereco.stateprovinceid
          , dados_endereco.postalcode
          , dados_endereco.spatiallocation
          , dados_endereco.rowguid
          , dados_endereco.modifieddate
          , join_territorio_venda_pais_estado.territoryid
          , join_territorio_venda_pais_estado.sales_territory_name
          , join_territorio_venda_pais_estado.countryregioncode
          , join_territorio_venda_pais_estado.sales_territory_group
          , join_territorio_venda_pais_estado.salesytd
          , join_territorio_venda_pais_estado.saleslastyear
          , join_territorio_venda_pais_estado.costytd
          , join_territorio_venda_pais_estado.costlastyear
          , join_territorio_venda_pais_estado.country_name
          , join_territorio_venda_pais_estado.stateprovincecode
          , join_territorio_venda_pais_estado.isonlystateprovinceflag
          , join_territorio_venda_pais_estado.stateprovince_name
        from dados_endereco
        left join join_territorio_venda_pais_estado
          on dados_endereco.stateprovinceid = join_territorio_venda_pais_estado.stateprovinceid
      )

select *
from final_join_endereco