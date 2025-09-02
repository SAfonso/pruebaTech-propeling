{{ config(
    materialized='table',
    cluster_by='NATION_ID'

) }}


select
    S_SUPPKEY        as SUPPLIER_ID,
    S_NAME           as SUPPLIER_NAME,
    S_ADDRESS        as SUPPLIER_ADDRESS,
    S_NATIONKEY      as NATION_ID,
    S_PHONE          as PHONE,
    S_COMMENT        as SUPPLIER_COMMENT,
    LOAD_TS
from {{ ref('supplier') }}
