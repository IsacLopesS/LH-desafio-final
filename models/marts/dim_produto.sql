with 
    dados_produto as (
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
      from {{ ref('int_produto') }} 
    )

select *
from dados_produto
