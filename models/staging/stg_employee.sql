with 
    employee as (
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
      from {{source('raw_data', 'employee') }}
    )
select * 
from employee