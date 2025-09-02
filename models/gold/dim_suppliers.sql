{{ config(
    materialized='table',
    cluster_by='S_NATIONKEY'

) }}


select
    S_SUPPKEY        as ID,
    S_NAME           as NAME,
    S_ADDRESS        as ADDRESS,
    S_NATIONKEY      as NATION_ID,
    S_PHONE          as PHONE,
    S_COMMENT        as SUPPLIER_COMMENT,
    LOAD_TS
from {{ ref('supplier') }}
