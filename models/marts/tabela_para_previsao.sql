with 

  fct_vendas as (
    select 
      venda_sk
      , salesorderid
      , revisionnumber
      , orderdate
      , duedate
      , shipdate
      , status_num
      , onlineorderflag
      , purchaseordernumber
      , accountnumber
      , customerid
      , salespersonid
      , territoryid
      , billtoaddressid
      , shiptoaddressid
      , shipmethodid
      , creditcardid
      , creditcardapprovalcode
      , currencyrateid
      , subtotal
      , taxamt
      , freight
      , totaldue
      , comment
      , rowguid
      , modifieddate
      , salesreasonid
      , reason_name
      , reasontype
      , salesorderdetailid
      , carriertrackingnumber
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
    from {{ref('fct_vendas')}}
  )

  , endereco_venda as (
      select
          addressid
          , sales_territory_name
          , country_name
          , stateprovince_name
          , city
      from {{ref('int_endereco') }}
    )


  , dim_produto as (
    select 
      productid
      , product_name
      , productnumber
      , makeflag
      , finishedgoodsflag
      , color
      , safetystocklevel
      , reorderpoint
      , standardcost
      , listprice
      , size
      , sizeunitmeasurecode
      , weightunitmeasurecode
      , product_weight
      , daystomanufacture
      , productline
      , class
      , style
      , productsubcategoryid
      , productmodelid
      , sellstartdate
      , sellenddate
      , discontinueddate
      , modifieddate
      , productcategoryid
      , subcategory_name
      , category_name
    from {{ (ref('dim_produto')) }}
  )

  , dim_cliente as (
    select 
      customerid
      , personid
      , storeid
      , person_id
      , persontype
      , namestyle
      , title
      , firstname
      , middlename
      , lastname
      , suffix
      , emailpromotion
      , additionalcontactinfo
      , persondemographics
      , emailaddress
      , storename
      , storedemographics
      , store_modifieddate
    from {{ ref('dim_clientes') }}
  )
, tabelao as (
    select 
      fct_vendas.salesorderid
      , fct_vendas.productid
      , fct_vendas.orderqty
      , fct_vendas.orderdate
      ,dim_produto.product_name
      , dim_produto.category_name
      , dim_produto.subcategory_name
      , dim_produto.makeflag as manufactured_in_house
      , dim_produto.class /*	H = High, M = Medium, L = Low*/
      , dim_produto.style /*W = Womens, M = Mens, U = Universal*/
      , dim_cliente.storeid /*se nulo, venda online*/
      , dim_cliente.storename
      , dim_cliente.storedemographics
      , fct_vendas.onlineorderflag
      , fct_vendas.salespersonid
      , endereco_venda.sales_territory_name
      , endereco_venda.country_name
      , endereco_venda.stateprovince_name
      , endereco_venda.city
    from fct_vendas
    left join dim_produto 
      on fct_vendas.productid = dim_produto.productid
    left join dim_cliente
      on dim_cliente.customerid = fct_vendas.customerid
    left join endereco_venda 
      on fct_vendas.shiptoaddressid = endereco_venda.addressid

  ) 

select * 
from tabelao

