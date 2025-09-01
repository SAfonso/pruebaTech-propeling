use role SYSADMIN;

create database if not exists DEMO_DB;

create schema if not exists DEMO_DB.RAW;
create schema if not exists DEMO_DB.SILVER;
create schema if not exists DEMO_DB.GOLD;

create warehouse if not exists WH_DBT_TRANSFORM
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;

use warehouse WH_DBT_TRANSFORM;
use database DEMO_DB;
use schema RAW;

-- Stage (ajusta el nombre si ya tienes otro)
create stage if not exists DEMO_DB.RAW.DEMO_STAGE;

-- File formats
create or replace file format DEMO_DB.RAW.FF_CSV_WITH_HEADER
  type = csv
  field_optionally_enclosed_by = '"';

create or replace file format DEMO_DB.RAW.FF_CSV_NO_HEADER
  type = csv
  field_optionally_enclosed_by = '"'
  error_on_column_count_mismatch = false;
