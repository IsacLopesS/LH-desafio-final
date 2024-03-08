with 
    endereco_completo as (
      select 
      *             
      from {{ ref('int_endereco') }}
    )
select *
from endereco_completo