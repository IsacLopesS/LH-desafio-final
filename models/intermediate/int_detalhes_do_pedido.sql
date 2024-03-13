with 
    ids_oferta_especial_e_produto as (
      select 
        specialofferid
        , productid     
      from {{ ref('stg_special_offer_product') }}
    )

,   dados_oferta_especial as (
      select
          specialofferid
          , description_special_offer
          , discountpct
          , type_special_offer
          , category
          , startdate
          , enddate
          , minqty
          , maxqty
          
      from {{ ref('stg_special_offer') }}
    )

,   join_oferta_especial_e_ids as (
      select 
        ids_oferta_especial_e_produto.productid
        , dados_oferta_especial.*

      from ids_oferta_especial_e_produto
      left join dados_oferta_especial
        on dados_oferta_especial.specialofferid = ids_oferta_especial_e_produto.specialofferid
    )

,   detalhes_do_pedido as (
      select 
          salesorderid
          , salesorderdetailid
          , carriertrackingnumber
          , orderqty
          , productid
          , specialofferid
          , unitprice
          , unitpricediscount
          , linetotal
      from {{ ref('stg_sales_order_detail')}}
    )

,   final_join as (
      select 
          detalhes_do_pedido.salesorderid
          , detalhes_do_pedido.salesorderdetailid
          , detalhes_do_pedido.carriertrackingnumber
          , detalhes_do_pedido.orderqty
          , detalhes_do_pedido.unitprice
          , detalhes_do_pedido.unitpricediscount
          , detalhes_do_pedido.linetotal
          , join_oferta_especial_e_ids.*

      from detalhes_do_pedido  
      left join join_oferta_especial_e_ids 
        on detalhes_do_pedido.productid = join_oferta_especial_e_ids.productid
          and detalhes_do_pedido.specialofferid = join_oferta_especial_e_ids.specialofferid       
    )

select *
from final_join