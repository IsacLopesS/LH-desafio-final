with 
  int_agg as (
    select * 
    from {{ ref('int_agg')}}
  )

  ,id_vendedor_nulo as (
      select
        int_agg.businessentityid
        , int_agg.jobtitle
        , int_agg.persontype
        , int_agg.complete_name
        , int_agg.emailaddress
        , int_agg.venda_sk
        , int_agg.orderdate
        , int_agg.onlineorderflag
        , int_agg.territoryid
        , case 
            when int_agg.salespersonid is null then 291
            else int_agg.salespersonid
          end as salespersonid
        , int_agg.shipmethodid
        , int_agg.subtotal
        , int_agg.taxamt
        , int_agg.freight
        , int_agg.totaldue
        , int_agg.linetotal
        , int_agg.salesreasonid territo
        , int_agg.reason_name
        , int_agg.orderqty
        , int_agg.unitprice
        , int_agg.unitpricediscount
        , int_agg.productid
        , int_agg.specialofferid
        , int_agg.description_special_offer
        , int_agg.discountpct
        , int_agg.type_special_offer
        , int_agg.category
        , int_agg.startdate
        , int_agg.enddate
        , int_agg.minqty
        , int_agg.maxqty
        , int_agg.sales_territory_name
        , int_agg.countryregioncode
        , int_agg.sales_territory_group
        , int_agg.salesytd
        , int_agg.saleslastyear
        , int_agg.costytd
        , int_agg.costlastyear
      from int_agg
  )

  , agrupar_por_dia as (
      select 
        territoryid
        , salespersonid
        , orderdate
        , min(sales_territory_name) as sales_territory_name
        , min(sales_territory_group) as sales_territory_group
        , min(countryregioncode) as countryregioncode
        , min(persontype) as persontype
        , min(complete_name) as complete_name
        , min(emailaddress) as emailaddress
        , min(onlineorderflag) as onlineorderflag
        , COUNT(venda_sk) AS numero_de_vendas
        , SUM(subtotal) AS valor_liquido_total
        , SUM(totaldue) AS valor_bruto_total
        , AVG(subtotal) AS avg_valor_liquido
        , avg(totaldue) as avg_valor_bruto
      from id_vendedor_nulo
      group by 
        territoryid
        , salespersonid
        , orderdate
  )
  
select *
from  agrupar_por_dia

