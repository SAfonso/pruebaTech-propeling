use role SYSADMIN;
use warehouse WH_DBT_TRANSFORM;
use database DEMO_DB;
use schema RAW;

create or replace procedure DEMO_DB.RAW.export_import_tables(table_list STRING)
returns string
language javascript
as
$$
var stage = "@DEMO_DB.RAW.DEMO_STAGE";   // ← usa el stage totalmente cualificado
var schema_bronze = "RAW";
var tgt_db = "DEMO_DB";

var ff      = "DEMO_DB.RAW.FF_CSV_WITH_HEADER";
var ff_load = "DEMO_DB.RAW.FF_CSV_NO_HEADER";

// tablas separadas por comas
var js_tables = arguments[0].split(",").map(function(t){ return t.trim(); });

for (var i = 0; i < js_tables.length; i++) {
    var full_table = js_tables[i];
    var parts = full_table.split(".");
    var db = parts[0];
    var schema = parts[1];
    var table = parts[2];

    // timestamp para el nombre del archivo
    var now = new Date();
    var timestamp = now.getFullYear().toString() +
                    ('0' + (now.getMonth()+1)).slice(-2) +
                    ('0' + now.getDate()).slice(-2) + "_" +
                    ('0' + now.getHours()).slice(-2) +
                    ('0' + now.getMinutes()).slice(-2) +
                    ('0' + now.getSeconds()).slice(-2);

    var file_name = table.toLowerCase() + "_" + timestamp + ".csv";

    // 1) UNLOAD al stage
    var unload_sql = `
      copy into ${stage}/${file_name}
      from (select * from ${full_table})
      file_format = ${ff}
    `;
    snowflake.execute({sqlText: unload_sql});

    // 2) Crear tabla RAW si no existe, con LOAD_TS
    var create_sql = `
      create table if not exists ${tgt_db}.${schema_bronze}.${table} as
      select *, cast(null as timestamp) as LOAD_TS
      from ${full_table}
      where 1=0
    `;
    snowflake.execute({sqlText: create_sql});

    // 3) Comparar número de columnas origen vs destino
    var src_cols_rs = snowflake.execute({sqlText: `
      select count(*) as cnt
      from ${db}.information_schema.columns
      where table_schema = '${schema}'
        and table_name   = '${table}'
    `});
    src_cols_rs.next();
    var src_cols = src_cols_rs.getColumnValue("CNT");

    var tgt_cols_rs = snowflake.execute({sqlText: `
      select count(*) as cnt
      from ${tgt_db}.information_schema.columns
      where table_schema = '${schema_bronze}'
        and table_name   = '${table}'
    `});
    tgt_cols_rs.next();
    var tgt_cols = tgt_cols_rs.getColumnValue("CNT");

    var diff = tgt_cols - src_cols;

    if (diff == 1) {
        // 4) COPY desde el stage (permitimos mismatch de 1 por LOAD_TS)
        var copy_sql = `
          copy into ${tgt_db}.${schema_bronze}.${table}
          from ${stage}/${file_name}
          file_format = ${ff_load}
        `;
        snowflake.execute({sqlText: copy_sql});

        // 5) Rellenar LOAD_TS
        var update_sql = `
          update ${tgt_db}.${schema_bronze}.${table}
          set LOAD_TS = current_timestamp
          where LOAD_TS is null
        `;
        snowflake.execute({sqlText: update_sql});
    } else {
        throw "Número de columnas no coincide correctamente para tabla " + table +
              ". Origen: " + src_cols + ", Destino: " + tgt_cols;
    }
}

return "Tablas procesadas: " + js_tables.length;
$$;
