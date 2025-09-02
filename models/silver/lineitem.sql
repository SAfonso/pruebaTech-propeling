{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ORDERLINE_ID',
    on_schema_change='sync_all_columns',
    cluster_by='L_SHIPDATE'
) }}

with base as (

    select
        L_ORDERKEY,
        L_PARTKEY,
        L_SUPPKEY,
        L_LINENUMBER,
        L_QUANTITY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_TAX,
        upper(trim(L_RETURNFLAG)) as L_RETURNFLAG,
        upper(trim(L_LINESTATUS)) as L_LINESTATUS,
        L_SHIPDATE,
        L_COMMITDATE,
        L_RECEIPTDATE,
        trim(L_SHIPINSTRUCT) as L_SHIPINSTRUCT,
        trim(L_SHIPMODE) as L_SHIPMODE,
        trim(L_COMMENT) as L_COMMENT,
        LOAD_TS
    from {{ source('RAW','LINEITEM') }}
    where L_ORDERKEY is not null
      and L_LINENUMBER is not null
      and L_PARTKEY is not null
      and L_QUANTITY > 0
      and L_EXTENDEDPRICE >= 0

    {% if is_incremental() %}
      and LOAD_TS >
          (select coalesce(max(LOAD_TS), to_timestamp('1970-01-01')) from {{ this }})
    {% endif %}
),

dedup as (
    select
        *,
        row_number() over (
          partition by L_ORDERKEY, L_LINENUMBER
          order by LOAD_TS desc
        ) as rn
    from base
)

select
    -- clave surrogate para incrementalidad
    {{ dbt_utils.generate_surrogate_key(['L_ORDERKEY','L_LINENUMBER']) }} as ORDERLINE_ID,
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT,
    LOAD_TS
from dedup
where rn = 1
