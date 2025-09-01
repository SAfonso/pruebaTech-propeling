{{ config(
    materialized='table'
) }}


with calendar as (
    select 
        dateadd(day, seq4(), '1990-01-01') as DATE_VALUE
    from table(generator(rowcount => 15000)) -- ~41 años
)

select
    to_number(to_char(DATE_VALUE, 'YYYYMMDD')) as DATE_KEY,
    DATE_VALUE,

    year(DATE_VALUE)                                  as YEAR,
    month(DATE_VALUE)                                 as MONTH,
    day(DATE_VALUE)                                   as DAY,
    quarter(DATE_VALUE)                               as QUARTER,
    weekofyear(DATE_VALUE)                            as WEEK_OF_YEAR,
    dayofweekiso(DATE_VALUE)                          as DAY_OF_WEEK_ISO,   -- 1=Mon..7=Sun
    to_char(DATE_VALUE, 'YYYY-MM')                    as YEAR_MONTH,
    to_char(DATE_VALUE, 'YYYY-MM-DD')                 as YEAR_MONTH_DAY,
    to_char(DATE_VALUE, 'Mon')                        as MONTH_ABBR,
    to_char(DATE_VALUE, 'DY')                         as DAY_ABBR,

    -- Flags útiles
    case when dayofweekiso(DATE_VALUE) in (6,7) then 1 else 0 end as IS_WEEKEND
from calendar
where DATE_VALUE is not null
