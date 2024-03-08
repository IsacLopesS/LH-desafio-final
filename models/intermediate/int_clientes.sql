with 
    dados_cliente as (
      select 
        customerid
        , personid
        , storeid
        , territoryid
        , rowguid
        , modifieddate
      
      from {{ ref('stg_customer') }}
    )

,   dados_pessoa as (
      select
          businessentityid
          ,persontype
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

      from {{ ref('stg_person') }}
    )

,   dados_email as (
      select 
          businessentityid
          ,emailaddressid
          ,emailaddress
          ,rowguid
          ,modifieddate
      from {{ ref('stg_emailadress') }}
    )

,   join_cliente_pessoa as (
      select 
        dados_cliente.customerid
        , dados_cliente.personid
        , dados_cliente.storeid
        , dados_cliente.territoryid
        , dados_cliente.modifieddate as clienteModifieddate
        , dados_pessoa.businessentityid
        , dados_pessoa.persontype
        , dados_pessoa.namestyle
        , dados_pessoa.title
        , dados_pessoa.firstname
        , dados_pessoa.middlename
        , dados_pessoa.lastname
        , dados_pessoa.suffix
        , dados_pessoa.emailpromotion
        , dados_pessoa.additionalcontactinfo
        , dados_pessoa.demographics
        , dados_pessoa.modifieddate as pessoaModifieddate
      from dados_cliente
      left join dados_pessoa
        on dados_cliente.personid = dados_pessoa.businessentityid
    )

,   join_cliente_pessoa_email as (
      select 
        join_cliente_pessoa.*
        , dados_email.emailaddressid
        , dados_email.emailaddress
        , dados_email.rowguid
        , dados_email.modifieddate
      from join_cliente_pessoa
      left join dados_email
        on join_cliente_pessoa.businessentityid = dados_email.businessentityid

)

select *
from join_cliente_pessoa_email