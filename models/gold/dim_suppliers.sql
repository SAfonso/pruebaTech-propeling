{{ config(
    materialized='table'
) }}


select
    S_SUPPKEY        as ID,
    S_NAME           as NAME,
    S_ADDRESS        as ADDRESS,
    S_NATIONKEY      as NATION_ID,
    S_PHONE          as PHONE,
    S_COMMENT        as SUPPLIER_COMMENT,
    LOAD_TS
from {{ source('SILVER','SUPPLIER') }}
