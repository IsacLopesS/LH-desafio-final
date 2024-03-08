with 
    salesperson as (
      select 
        businessentityid
        , territoryid
        , salesquota
        , bonus
        , commissionpct
        , salesytd
        , saleslastyear
                      
      from {{ ref('salesperson') }}
      )

,   territorio_venda as (
      select
          territoryid
          , sales_territory_name
          , countryregioncode
          , sales_territory_group
          , salesytd
          , saleslastyear
          , costytd
          , costlastyear
          , rowguid
          , modifieddate

      from {{ref('stg_sales_territory') }}
    )
,   dados_email as (
        select 
            businessentityid
            ,emailaddressid
            ,emailaddress
            ,rowguid
            ,modifieddate
        from {{ref('stg_emailadress') }}
    )


,   employee as (
          select 
            businessentityid
            , nationalidnumber
            , loginid
            , jobtitle
            , birthdate
            , maritalstatus
            , gender
            , hiredate
            , salariedflag
            , vacationhours
            , sickleavehours
            , currentflag
            , rowguid
            , modifieddate
            , organizationnode
          from {{ ref('employee') }}
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
          ,lower(firstname) || ' ' || lower(middlename) || ' ' || lower(lastname) as complete_name
          ,suffix
          ,emailpromotion
          ,additionalcontactinfo
          ,demographics
          ,rowguid
          ,cast(modifieddate as TIMESTAMP) as modifieddate

      from {{ ref('stg_person') }}
    )

,   join_salesperson_salesterritory as (

      select 
        salesperson.businessentityid
        , salesperson.salesquota
        , salesperson.bonus
        , salesperson.commissionpct
        , salesperson.salesytd
        , salesperson.saleslastyear
        , territorio_venda.territoryid
        , territorio_venda.sales_territory_name
        , territorio_venda.countryregioncode
        , territorio_venda.sales_territory_group
        , territorio_venda.costytd
        , territorio_venda.costlastyear
      from salesperson
      left join territorio_venda
        on territorio_venda.territoryid = salesperson.territoryid
)
/* dados pessoa, funcionario e email */
, join_employee_person_email as (
    select
      employee.businessentityid
      , employee.jobtitle
      , dados_pessoa.persontype
      , dados_pessoa.complete_name
      , dados_email.emailaddress
    from employee
    left join dados_pessoa
      on dados_pessoa.businessentityid = employee.businessentityid
    left join dados_email
      on dados_email.businessentityid = employee.businessentityid
)

, final_join as (
    select 
        join_employee_person_email.*
        , join_salesperson_salesterritory.salesquota
        , join_salesperson_salesterritory.bonus
        , join_salesperson_salesterritory.commissionpct
        , join_salesperson_salesterritory.salesytd
        , join_salesperson_salesterritory.saleslastyear
        , join_salesperson_salesterritory.territoryid
        , join_salesperson_salesterritory.sales_territory_name
        , join_salesperson_salesterritory.countryregioncode
        , join_salesperson_salesterritory.sales_territory_group
        , join_salesperson_salesterritory.costytd
        , join_salesperson_salesterritory.costlastyear
    from join_salesperson_salesterritory
    left join join_employee_person_email 
      on join_employee_person_email.businessentityid = join_salesperson_salesterritory.businessentityid
)

select *
from final_join
