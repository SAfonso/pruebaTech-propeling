{{ config(
    materialized='table'
) }}


with calendar as (
    select 
        dateadd(day, seq4(), '1990-01-01') as DATE_VALUE
    from table(generator(rowcount => 15000)) -- ~41 años
)

select
    DATE_VALUE,
    year(DATE_VALUE) as YEAR,
    month(DATE_VALUE) as MONTH,
    day(DATE_VALUE) as DAY,
    to_char(DATE_VALUE, 'YYYY-MM') as YEAR_MONTH,
    quarter(DATE_VALUE) as QUARTER,
    dayofweek(DATE_VALUE) as DAY_OF_WEEK
from calendar
