with 
    id_da_razao_da_compra as (
        select
            salesorderid
            , salesreasonid
            , modifieddate

        from {{ ref('stg_sales_order_head_sales_reason') }}
    )

,   razao_da_compra as (
            select
                salesreasonid
                , reason_name
                , reasontype
                , modifieddate

            from {{ ref('stg_sales_reason') }}
    )

,   join_razao_da_compra as (
            select 
                id_da_razao_da_compra.salesorderid
                , id_da_razao_da_compra.salesreasonid
                , razao_da_compra.reason_name
                , razao_da_compra.reasontype

            from id_da_razao_da_compra
            left join razao_da_compra 
                on id_da_razao_da_compra.salesreasonid = razao_da_compra.salesreasonid

    ) 

select * 
from join_razao_da_compra  
