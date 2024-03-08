with 
    dados_pessoa as (
      select
          businessentityid
          ,persontype
          ,namestyle
          ,title
          ,lower(firstname) as firstname
          ,lower(middlename) as middlename
          ,lower(lastname) as lastname
          ,suffix
          ,emailpromotion
          ,additionalcontactinfo
          ,demographics
          ,rowguid
          ,cast(modifieddate as TIMESTAMP) as modifieddate

      from {{source('raw_data', 'person') }}
    )

,   updated_persontype as (
      select 
          *,
          case 
              when persontype = 'SC' then 'contato da loja'
              when persontype = 'IN' then 'cliente individual'
              when persontype = 'SP' then 'vendedor'
              when persontype = 'EM' then 'funcionario'
              when persontype = 'VC' then 'contato do fornecedor'
              when persontype = 'GC' then 'contato geral'
              else persontype

          end as new_persontype
      from dados_pessoa
    )

,   person_type as (
      select
        businessentityid
          ,new_persontype as persontype
          ,namestyle
          ,title
          ,firstname
          ,middlename
          ,lastname
          ,suffix
          ,emailpromotion
          ,additionalcontactinfo
          ,demographics
          ,rowguid
          ,modifieddate

      from updated_persontype
)    

select *
from person_type






