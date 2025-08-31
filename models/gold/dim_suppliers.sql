{{ config(
    materialized='table'
) }}


select
    S_SUPPKEY        as SUPPLIER_ID,
    S_NAME           as SUPPLIER_NAME,
    S_ADDRESS        as SUPPLIER_ADDRESS,
    S_NATIONKEY      as NATION_ID,
    S_PHONE          as SUPPLIER_PHONE,
    S_ACCTBAL        as ACCOUNT_BALANCE,
    S_COMMENT        as SUPPLIER_COMMENT,
    LOAD_TS
from {{ ref('supplier') }}
