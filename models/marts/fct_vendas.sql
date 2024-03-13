with 
    sales_data as (
        select
            salesorderid
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
        from {{ ref('stg_sales_order_header')}}
    )

,   razao_da_compra as (
            select 
                salesorderid
                , salesreasonid
                , reason_name
                , reasontype
            from {{ ref('int_razao_compra') }}
    )

,   sales_data_join_razao_compra as (
            select 
                sales_data.*
                , razao_da_compra.salesreasonid
                , razao_da_compra.reason_name
                , razao_da_compra.reasontype
            from sales_data
            left join razao_da_compra
                on sales_data.salesorderid = razao_da_compra.salesorderid
    )

,   detalhes_pedidos as (
            select
                salesorderid
                , salesorderdetailid
                , carriertrackingnumber
                , orderqty
                , unitprice
                , unitpricediscount
                , linetotal
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
            from {{ ref('int_detalhes_do_pedido') }}
    )



,   sales_data_razao_compra_join_detalhes_pedido as (
            select 
                sales_data_join_razao_compra.*
                , detalhes_pedidos.salesorderdetailid
                , detalhes_pedidos.carriertrackingnumber
                , detalhes_pedidos.orderqty
                , detalhes_pedidos.unitprice
                , detalhes_pedidos.unitpricediscount
                , detalhes_pedidos.productid
                , detalhes_pedidos.specialofferid
                , detalhes_pedidos.description_special_offer
                , detalhes_pedidos.discountpct
                , detalhes_pedidos.linetotal
                , detalhes_pedidos.type_special_offer
                , detalhes_pedidos.category
                , detalhes_pedidos.startdate
                , detalhes_pedidos.enddate
                , detalhes_pedidos.minqty
                , detalhes_pedidos.maxqty
            from sales_data_join_razao_compra
            left join detalhes_pedidos
                on sales_data_join_razao_compra.salesorderid = detalhes_pedidos.salesorderid
    )

,   chave_sk as (
    /*chave surrogate primaria*/
    select 
    {{ dbt_utils.generate_surrogate_key(['salesorderid','salesorderdetailid','salesreasonid']) }} as venda_sk
    ,*

    from sales_data_razao_compra_join_detalhes_pedido 
    )

select *
from chave_sk