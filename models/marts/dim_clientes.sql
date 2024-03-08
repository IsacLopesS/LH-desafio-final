with 
    dados_cliente as (
      select 	
        customerid
        , personid
        , storeid
        , businessentityid as person_id
        , persontype
        , namestyle
        , title
        , firstname
        , middlename
        , lastname
        , suffix
        , emailpromotion
        , additionalcontactinfo
        , demographics as persondemographics
        , emailaddressid
        , emailaddress
      from {{ ref('int_clientes') }} 
    )

,   dados_loja as (
      select   	
        storeid
        , storename
        , salespersonid
        , demographics as storedemographics
        , rowguid
        , store_modifieddate
      from {{ ref('stg_store') }}
    )   

,   final_join as (
      select 
        dados_cliente.*
        , dados_loja.storename
        , dados_loja.storedemographics
        , dados_loja.store_modifieddate 
      from dados_cliente
      left join dados_loja
        on dados_cliente.storeid = dados_loja.storeid
)


select *
from final_join