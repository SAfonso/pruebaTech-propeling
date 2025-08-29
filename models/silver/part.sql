{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='P_PARTKEY',
    on_schema_change='sync_all_columns'
) }}

with base as (
    select
        P_PARTKEY,
        upper(trim(P_NAME)) as P_NAME,
        trim(P_MFGR) as P_MANUFACTURER,
        trim(P_BRAND) as P_BRAND,
        trim(P_TYPE) as P_TYPE,
        P_SIZE,
        P_CONTAINER,
        P_RETAILPRICE,
        trim(P_COMMENT) as P_COMMENT,
        LOAD_TS
    from {{ source('RAW', 'PART') }}
    {% if is_incremental() %}
        where LOAD_TS > (select coalesce(max(LOAD_TS), to_timestamp('1970-01-01')) from {{ this }})
    {% endif %}
)
select *
from base
qualify row_number() over (partition by P_PARTKEY order by LOAD_TS desc) = 1