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
          , rowguid
          , modifieddate

      from {{ ref('stg_product') }}
    )

,   dados_subcategoria_produto as (
      select
          productsubcategoryid
          , productcategoryid
          , subcategory_name
          , rowguid
          , modifieddate

      from {{ ref('stg_product_subcategory') }}
    )

,   dados_categoria_produto as (
      select
          productcategoryid
          , category_name
          , rowguid
          , modifieddate

      from {{ref('stg_product_category') }}
    )   

,   join_subcategoria_categoria as (
      select 
          dados_subcategoria_produto.productsubcategoryid
          , dados_subcategoria_produto.productcategoryid
          , dados_subcategoria_produto.subcategory_name
          , dados_categoria_produto.category_name
      from dados_subcategoria_produto
      left join dados_categoria_produto
        on dados_subcategoria_produto.productcategoryid = dados_categoria_produto.productcategoryid
    )

,   join_produto_catgoria_subcategoria as (
      select
        dados_produto.*
        , join_subcategoria_categoria.productcategoryid
        , join_subcategoria_categoria.subcategory_name
        , join_subcategoria_categoria.category_name
      from dados_produto
      left join join_subcategoria_categoria
        on dados_produto.productsubcategoryid = join_subcategoria_categoria.productsubcategoryid

    )  

select *
from join_produto_catgoria_subcategoria