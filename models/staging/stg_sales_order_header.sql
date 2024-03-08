with 
    sales_data as (
      select
          salesorderid
          , revisionnumber
          , orderdate
          , duedate
          , shipdate
          , status as status_num
          , onlineorderflag
          , purchaseordernumber
          , accountnumber
          , customerid
          , salespersonid
          , territoryid
          , billtoaddressid
          , shiptoaddressid
          , shipmethodid
          , creditcardid
          , creditcardapprovalcode
          , currencyrateid
          , subtotal
          , taxamt
          , freight
          , totaldue
          , comment
          , rowguid
          , modifieddate
      from {{source('raw_data', 'salesorderheader') }}
    )

select *
from sales_data