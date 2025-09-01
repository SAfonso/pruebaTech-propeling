{{ config(
    materialized='table'
) }}

select
    C_CUSTKEY       as CUSTOMER_ID,
    C_NAME          as CUSTOMER_NAME,
    C_ADDRESS       as CUSTOMER_ADDRESS,
    C_NATIONKEY     as NATION_ID,
    C_PHONE         as CUSTOMER_PHONE,
    C_ACCTBAL       as ACCOUNT_BALANCE,
    C_MKTSEGMENT    as MARKET_SEGMENT,
    C_COMMENT       as CUSTOMER_COMMENT,
    LOAD_TS
from {{ source('SILVER','CUSTOMER') }}
