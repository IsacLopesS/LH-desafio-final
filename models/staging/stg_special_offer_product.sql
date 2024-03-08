with 
    oferta_especial_produto as (
      select
          specialofferid
          , productid
          , rowguid
          , modifieddate

      from {{source('raw_data', 'specialofferproduct') }}
    )

select *
from oferta_especial_produto
