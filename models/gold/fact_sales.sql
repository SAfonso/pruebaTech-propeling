{{ config(
    materialized='table',
    schema='GOLD'
) }}

with base as (

    select
        l.L_ORDERKEY,
        l.L_LINENUMBER,
        o.C_CUSTKEY,
        l.L_PARTKEY,
        l.L_SUPPKEY,
        o.O_ORDERDATE,
        l.L_SHIPDATE,
        l.L_COMMITDATE,
        l.L_RECEIPTDATE,
        l.L_QUANTITY,
        l.L_EXTENDEDPRICE,
        l.L_DISCOUNT,
        l.L_TAX,

        -- métricas
        (l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as NET_SALES,
        (l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) * (1 + l.L_TAX)) as GROSS_SALES

    from {{ ref('orders') }} o     -- desde SILVER
    join {{ ref('lineitem') }} l   -- desde SILVER
      on l.L_ORDERKEY = o.O_ORDERKEY
)

select
    -- claves de unión con dimensiones
    L_ORDERKEY      as ORDERKEY,
    L_LINENUMBER    as LINENUMBER,
    C_CUSTKEY       as CUSTOMER_KEY,
    L_PARTKEY       as PART_KEY,
    L_SUPPKEY       as SUPPLIER_KEY,
    O_ORDERDATE     as ORDER_DATE,
    L_SHIPDATE      as SHIP_DATE,
    L_COMMITDATE    as COMMITDATE,
    L_RECEIPTDATE   as RECEIPTDATE,

    -- métricas
    L_QUANTITY      as QUANTITY ,
    L_EXTENDEDPRICE as EXTENDEDPRICE,
    L_DISCOUNT      as DISCOUNT,
    L_TAX           as TAX,
    NET_SALES,
    GROSS_SALES
from base
