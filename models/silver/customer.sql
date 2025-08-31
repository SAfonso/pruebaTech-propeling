{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='C_CUSTKEY',
    on_schema_change='sync_all_columns'
) }}

with base as (

    select
        C_CUSTKEY,

        -- Transformamos en nombre
        regexp_replace(C_NAME, '^Customer#', 'C#') as C_NAME,

        -- Limpiamos el campo Address
        regexp_replace(trim(C_ADDRESS), '[^a-zA-Z0-9 ]', '') as C_ADDRESS,

        C_NATIONKEY,

        -- Normalizamos el prefijo d elos teléfonos
        -- Normalización de teléfono con soporte 2 y 4 dígitos
        case
            -- Formato 00NN-XXX-XXXX
            when regexp_like(C_PHONE, '^00([0-9]{2,4})-') then regexp_replace(C_PHONE, '^00([0-9]{2,4})-', '+\1 ')
            -- Formato NN-XXX-XXXX
            when regexp_like(C_PHONE, '^([0-9]{2,4})-([0-9]{3})-([0-9]{3})-([0-9]{4})$')
                then regexp_replace(C_PHONE, '^([0-9]{2,4})-([0-9]{3})-([0-9]{3})-([0-9]{4})$', '+\1 \2-\3-\4')
            else C_PHONE
        end as C_PHONE_STD,

        C_ACCTBAL,
        upper(trim(C_MKTSEGMENT)) as C_MKTSEGMENT,
        trim(C_COMMENT) as C_COMMENT,
        LOAD_TS
    from {{ source('RAW','CUSTOMER') }}
    where C_CUSTKEY is not null

    {% if is_incremental() %}
      and LOAD_TS >
          (select coalesce(max(LOAD_TS), to_timestamp('1970-01-01')) from {{ this }})
    {% endif %}
)

select * from base
