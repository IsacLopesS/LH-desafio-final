with 
  fct_vendas as (
    select 
      venda_sk
      , orderdate
      , onlineorderflag
      , territoryid
      , salespersonid
      , shipmethodid
      , subtotal
      , taxamt
      , freight
      , totaldue
      , linetotal
      , salesreasonid
      , reason_name
      , orderqty
      , unitprice
      , unitpricediscount
      , productid
      , specialofferid
      , description_special_offer
      , discountpct
      , type_special_offer
      , category
      , startdate
      , enddate
      , minqty
      , maxqty
    
  from {{ ref('fct_vendas') }}     
  )

  ,dim_vendedor as (
    select 
        businessentityid
        , jobtitle
        , persontype
        , complete_name
        , emailaddress
    from {{ ref('dim_vendedor') }}     
  )

  , sales_territory as (
      select 
          territoryid
          , sales_territory_name
          , countryregioncode
          , sales_territory_group
          , salesytd
          , saleslastyear
          , costytd
          , costlastyear
      from {{ ref('stg_sales_territory')}}
  )

  ,join_fctvendas_vendedor as (
    select 
      dim_vendedor.businessentityid
      , dim_vendedor.jobtitle
      , dim_vendedor.persontype
      , dim_vendedor.complete_name
      , dim_vendedor.emailaddress
      , fct_vendas.*
    from fct_vendas 
    left join dim_vendedor 
      on dim_vendedor.businessentityid = fct_vendas.salespersonid
  )

  ,final_join as (
    select
      join_fctvendas_vendedor.*
      , sales_territory.sales_territory_name
      , sales_territory.countryregioncode
      , sales_territory.sales_territory_group
      , sales_territory.salesytd
      , sales_territory.saleslastyear
      , sales_territory.costytd
      , sales_territory.costlastyear
    from join_fctvendas_vendedor
    left join sales_territory 
      on sales_territory.territoryid = join_fctvendas_vendedor.territoryid
  )

  select * 
  from final_join