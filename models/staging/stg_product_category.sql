with 
    dados_categoria_produto as (
      select
          productcategoryid
          , name as category_name
          , rowguid
          , date(modifieddate) as modifieddate

      from {{source('raw_data', 'productcategory') }}
    )

select *
from dados_categoria_produto
