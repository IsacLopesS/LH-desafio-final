/*Todos os casos de vendas sem vendedor implica em venda online*/

select salespersonid
from {{ ref('int_agg') }}
where onlineorderflag = true and salespersonid is not null