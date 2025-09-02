{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='S_SUPPKEY',
    on_schema_change='sync_all_columns',
    cluster_by='S_NATIONKEY'

) }}

with base as (
    select
        S_SUPPKEY,
        -- Transformamos en nombre
        regexp_replace(trim(S_NAME), '^Supplier#', 'S#') as S_NAME,
        S_ADDRESS,
        S_NATIONKEY,
        S_PHONE,
        trim(S_COMMENT) as S_COMMENT,
        LOAD_TS
    from {{ source('RAW', 'SUPPLIER') }}
    {% if is_incremental() %}
        where LOAD_TS > (select coalesce(max(LOAD_TS), to_timestamp('1970-01-01')) from {{ this }})
    {% endif %}
)
select *
from base
qualify row_number() over (partition by S_SUPPKEY order by LOAD_TS desc) = 1
