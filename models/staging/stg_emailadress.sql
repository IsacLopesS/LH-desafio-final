with 
    email_data as (
      select 
          businessentityid
          ,emailaddressid
          ,emailaddress
          ,rowguid
          ,modifieddate
      from {{source('raw_data', 'emailaddress') }}
    )

select 
    businessentityid
    ,emailaddressid
    ,emailaddress.emailaddress as emailaddress
from email_data

