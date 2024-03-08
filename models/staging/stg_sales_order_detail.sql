with 
    detalhes_do_pedido as (
      select
          salesorderid
          , salesorderdetailid
          , carriertrackingnumber
          , orderqty
          , productid
          , specialofferid
          , unitprice
          , unitpricediscount
          , rowguid
          , modifieddate

      from {{source('raw_data', 'salesorderdetail') }}
    )

select *
from detalhes_do_pedido
