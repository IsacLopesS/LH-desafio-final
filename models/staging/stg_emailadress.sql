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

select *
from email_data

