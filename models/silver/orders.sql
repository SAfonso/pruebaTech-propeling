{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='O_ORDERKEY',
    on_schema_change='sync_all_columns',
    cluster_by='O_ORDERDATE'
) }}

with base as (

    select
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERSTATUS,
        O_TOTALPRICE,
        O_ORDERDATE,

        -- Dividir prioridad en código y valor
        split_part(O_ORDERPRIORITY, '-', 1) as O_ORDERPRIORITY_CODE,
        split_part(O_ORDERPRIORITY, '-', 2) as O_ORDERPRIORITY_VALUE,

        O_ORDERPRIORITY,  -- lo dejamos por trazabilidad
        -- Normalización Clerk
        regexp_replace(O_CLERK, '^Clerk#', 'CL#') as O_CLERK_STD,

        O_SHIPPRIORITY,
        trim(O_COMMENT) as O_COMMENT,
        LOAD_TS

    from {{ source('RAW','ORDERS') }}
    where O_ORDERKEY is not null

    {% if is_incremental() %}
      and LOAD_TS >
          (select coalesce(max(LOAD_TS), to_timestamp('1970-01-01')) from {{ this }})
    {% endif %}
)

-- Deduplicación (última versión por clave primaria)
select *
from base
qualify row_number() over (partition by O_ORDERKEY order by LOAD_TS desc) = 1
