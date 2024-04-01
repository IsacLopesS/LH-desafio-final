with 
  dados_vendedor as (
    select
    businessentityid
    , jobtitle
    , persontype 
    , complete_name
    , emailaddress
    , salesquota
    , bonus
    , commissionpct
    , salesytd
    , saleslastyear
    from {{ ref('int_vendedor') }}
  )

select * 
from dados_vendedor