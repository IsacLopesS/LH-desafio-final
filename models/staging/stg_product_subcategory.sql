with 
    dados_subcategoria_produto as (
      select
          productsubcategoryid
          , productcategoryid
          , name as subcategory_name
          , rowguid
          , date(modifieddate) as modifieddate

      from {{source('raw_data', 'productsubcategory') }}
    )

select *
from dados_subcategoria_produto
