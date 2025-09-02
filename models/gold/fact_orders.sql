{{ config(
    materialized='table',
    cluster_by='ORDER_DATE'
) }}

select
    o.O_ORDERKEY  as ORDERKEY,
    o.O_CUSTKEY   as CUSTOMER_KEY,
    o.O_ORDERDATE as ORDER_DATE,
    sum(l.L_QUANTITY)        as TOTAL_QUANTITY,
    sum(l.L_EXTENDEDPRICE)   as TOTAL_PRICE,
    sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as TOTAL_NET_SALES,
    sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) * (1 + l.L_TAX)) as TOTAL_GROSS_SALES
from {{ ref('orders') }} o
join {{ ref('lineitem') }} l
  on o.O_ORDERKEY = l.L_ORDERKEY
group by
    o.O_ORDERKEY,
    o.O_CUSTKEY,
    o.O_ORDERDATE
