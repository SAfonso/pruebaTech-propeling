{{ config(
    materialized='table',
    cluster_by='SHIP_DATE_KEY'
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

        to_number(to_char(o.O_ORDERDATE,  'YYYYMMDD')) as ORDER_DATE_KEY,
        to_number(to_char(l.L_SHIPDATE,   'YYYYMMDD')) as SHIP_DATE_KEY,
        to_number(to_char(l.L_COMMITDATE, 'YYYYMMDD')) as COMMIT_DATE_KEY,
        to_number(to_char(l.L_RECEIPTDATE,'YYYYMMDD')) as RECEIPT_DATE_KEY,

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

    -- Claves de fecha (NUMÉRICAS)
    b.ORDER_DATE_KEY,
    b.SHIP_DATE_KEY,
    b.COMMIT_DATE_KEY,
    b.RECEIPT_DATE_KEY,

    -- Fechas legibles
    d1.DATE_VALUE as ORDER_DATE,
    d2.DATE_VALUE as SHIP_DATE,
    d3.DATE_VALUE as COMMIT_DATE,
    d4.DATE_VALUE as RECEIPT_DATE,

    -- Hechos
    b.L_QUANTITY as QUANTITY,
    b.L_EXTENDEDPRICE as EXTENDED_PRICE,
    b.L_DISCOUNT as DISCOUNT,
    b.L_TAX as TAX,
    b.L_RETURNFLAG as RETURN_FLAG,
    b.L_LINESTATUS as LINESTATUS

from base b
left join {{ ref('dim_dates') }} d1 on b.ORDER_DATE_KEY   = d1.DATE_KEY
left join {{ ref('dim_dates') }} d2 on b.SHIP_DATE_KEY    = d2.DATE_KEY
left join {{ ref('dim_dates') }} d3 on b.COMMIT_DATE_KEY  = d3.DATE_KEY
left join {{ ref('dim_dates') }} d4 on b.RECEIPT_DATE_KEY = d4.DATE_KEY