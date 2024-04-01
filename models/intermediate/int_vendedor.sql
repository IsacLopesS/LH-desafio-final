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
                      
      from {{ ref('stg_sales_person') }}
      )

,   dados_email as (
        select 
            businessentityid
            ,emailaddressid
            ,emailaddress
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
        , salesperson.salesquota
        , salesperson.bonus
        , salesperson.commissionpct
        , salesperson.salesytd
        , salesperson.saleslastyear
    from salesperson
    left join join_employee_person_email 
      on join_employee_person_email.businessentityid = salesperson.businessentityid
)

select *
from final_join
