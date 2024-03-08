with 
    dados_produto as (
      select
          productid
          , name as product_name
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
          , weight as product_weight
          , daystomanufacture
          , productline
          , class
          , style
          , productsubcategoryid
          , productmodelid
          , sellstartdate
          , sellenddate
          , discontinueddate
          , rowguid
          , modifieddate

      from {{source('raw_data', 'product') }}
    )

select *
from dados_produto
