{{ config(
    materialized='table',
    cluster_by='L_SHIPDATE'
) }}

with base as (

    select
        l.L_ORDERKEY,
        l.L_PARTKEY,
        l.L_SUPPKEY,
        l.L_LINENUMBER,

        -- Hechos
        l.L_QUANTITY,
        l.L_EXTENDEDPRICE,
        l.L_DISCOUNT,
        l.L_TAX,
        l.L_RETURNFLAG,
        l.L_LINESTATUS,

        -- Fechas de negocio
        l.L_SHIPDATE,
        l.L_COMMITDATE,
        l.L_RECEIPTDATE,
        o.O_ORDERDATE,

        -- Enlaces a dimensiones
        c.C_CUSTKEY,
        p.P_PARTKEY,
        s.S_SUPPKEY

    from {{ ref('lineitem') }} l
    join {{ ref('orders') }} o on l.L_ORDERKEY = o.O_ORDERKEY
    join {{ ref('customer') }} c on o.O_CUSTKEY  = c.C_CUSTKEY
    join {{ ref('part') }}     p on l.L_PARTKEY  = p.P_PARTKEY
    join {{ ref('supplier') }} s on l.L_SUPPKEY  = s.S_SUPPKEY
)

select
    -- FKs a dimensiones
    b.C_CUSTKEY  as CUSTOMER_KEY,
    b.P_PARTKEY  as PART_KEY,
    b.S_SUPPKEY  as SUPPLIER_KEY,
    b.L_ORDERKEY as ORDER_KEY,
    b.L_LINENUMBER,

    -- Claves de fecha (usar DATE_KEY de dim_dates)
    d1.DATE_KEY as ORDER_DATE_KEY,
    d2.DATE_KEY as SHIP_DATE_KEY,
    d3.DATE_KEY as COMMIT_DATE_KEY,
    d4.DATE_KEY as RECEIPT_DATE_KEY,

    -- Hechos
    b.L_QUANTITY,
    b.L_EXTENDEDPRICE,
    b.L_DISCOUNT,
    b.L_TAX,
    b.L_RETURNFLAG,
    b.L_LINESTATUS

from base b
left join {{ ref('dim_dates') }} d1 on b.O_ORDERDATE   = d1.DATE_VALUE
left join {{ ref('dim_dates') }}  d2 on b.L_SHIPDATE    = d2.DATE_VALUE
left join {{ ref('dim_dates') }}  d3 on b.L_COMMITDATE  = d3.DATE_VALUE
left join {{ ref('dim_dates') }}  d4 on b.L_RECEIPTDATE = d4.DATE_VALUE