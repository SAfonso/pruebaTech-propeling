{{ config(
    materialized='table',
    cluster_by='P_SIZE'
) }}


select
    P_PARTKEY        as PART_ID,
    P_NAME           as PART_NAME,
    P_MANUFACTURER   as MANUFACTURER,
    P_BRAND          as BRAND,
    P_TYPE           as P_TYPE,
    P_SIZE           as P_SIZE,
    P_CONTAINER      as CONTAINER,
    P_RETAILPRICE    as RETAIL_PRICE,
    P_COMMENT        as PART_COMMENT,
    LOAD_TS
from {{ ref('part') }}
