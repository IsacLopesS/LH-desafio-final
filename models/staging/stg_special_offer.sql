with 
    oferta_especial as (
      select
          specialofferid
          , description as description_special_offer
          , discountpct
          , type as type_special_offer
          , category
          , startdate
          , enddate
          , minqty
          , maxqty
          , rowguid
          , modifieddate

      from {{source('raw_data', 'specialoffer') }}
    )

select *
from oferta_especial
