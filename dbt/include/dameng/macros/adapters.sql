/* For examples of how to fill out the macros please refer to the postgres adapter and docs
postgres adapter macros: https://github.com/dbt-labs/dbt-core/blob/main/plugins/postgres/dbt/include/postgres/macros/adapters.sql
dbt docs: https://docs.getdbt.com/docs/contributing/building-a-new-adapter
*/

{% macro dameng__alter_column_type(relation,column_name,new_column_type) -%}
  {#
    Changes column name or data type
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type 1', fetch_result=False) %}
    alter table {{ relation }} add {{ tmp_column }} {{ new_column_type }}
  {% endcall %}
  {% call statement('alter_column_type 2', fetch_result=False) %}
    update {{ relation  }} set {{ tmp_column }} = {{ column_name }}
  {% endcall %}
  {% call statement('alter_column_type 3', fetch_result=False) %}
    alter table {{ relation }} drop column {{ column_name }} cascade constraints
  {% endcall %}
  {% call statement('alter_column_type 4', fetch_result=False) %}
    alter table {{ relation }} rename column {{ tmp_column }} to {{ column_name }}
  {% endcall %}
{% endmacro %}

{% macro dameng__check_schema_exists(information_schema,schema) -%}
  {% if information_schema.database -%}
    {{ adapter.verify_database(information_schema.database) }}
  {%- endif -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    select count(*) from sys.all_users where username = upper('{{ schema }}')
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro dameng__create_schema(relation) -%}
 {% if relation.database -%}
   {{ adapter.verify_database(relation.database) }}
 {%- endif -%}
 {%- call statement('create_schema') -%}
     create schema {{ relation.without_identifier().include(database=False) }}
 {%- endcall -%}
{% endmacro %}


{% macro dameng__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade
  {%- endcall -%}
{% endmacro %}


{% macro dameng__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
   DECLARE
     dne_942    EXCEPTION;
     PRAGMA EXCEPTION_INIT(dne_942, -942);
     attempted_ddl_on_in_use_GTT EXCEPTION;
     pragma EXCEPTION_INIT(attempted_ddl_on_in_use_GTT, -14452);
  BEGIN
     SAVEPOINT start_transaction;
     EXECUTE IMMEDIATE 'DROP {{ relation.type }} {{ relation }} cascade constraint';
     COMMIT;
  EXCEPTION
     WHEN attempted_ddl_on_in_use_GTT THEN
        NULL; -- if it its a global temporary table, leave it alone.
     WHEN dne_942 THEN
        NULL;
  END;
  {%- endcall %}
{% endmacro %}


{% macro dameng__get_columns_in_relation(relation) -%}
{% call statement('get_columns_in_relation', fetch_result=True) %}
      with columns as (
        select
            SYS_CONTEXT('userenv', 'DB_NAME') table_catalog,
            owner table_schema,
            table_name,
            column_name,
            data_type,
            data_type_mod,
            decode(data_type_owner, null, TO_CHAR(null), SYS_CONTEXT('userenv', 'DB_NAME')) domain_catalog,
            data_type_owner domain_schema,
            data_length character_maximum_length,
            data_length character_octet_length,
            data_length,
            data_precision numeric_precision,
            data_scale numeric_scale,
            nullable is_nullable,
            column_id ordinal_position,
            default_length,
            data_default column_default,
            num_distinct,
            low_value,
            high_value,
            density,
            num_nulls,
            num_buckets,
            last_analyzed,
            sample_size,
            SYS_CONTEXT('userenv', 'DB_NAME') character_set_catalog,
            'SYS' character_set_schema,
            SYS_CONTEXT('userenv', 'DB_NAME') collation_catalog,
            'SYS' collation_schema,
            character_set_name,
            char_col_decl_length,
            global_stats,
            user_stats,
            avg_col_len,
            char_length,
            char_used,
            v80_fmt_image,
            data_upgraded,
            histogram
          from sys.all_tab_columns
      )
      select
          column_name as "name",
          data_type as "type",
          char_length as "character_maximum_length",
          numeric_precision as "numeric_precision",
          numeric_scale as "numeric_scale"
      from columns
      where upper(table_name) = upper('{{ relation.identifier }}')
        {% if relation.schema %}
        and upper(table_schema) = upper('{{ relation.schema }}')
        {% endif %}
      order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro dameng__list_relations_without_caching(schema_relation) -%}
{% call statement('list_relations_without_caching', fetch_result=True) -%}
    with tables as
      (select SYS_CONTEXT('userenv', 'DB_NAME') table_catalog,
         owner table_schema,
         table_name,
         case
           when iot_type = 'Y'
           then 'IOT'
           when temporary = 'Y'
           then 'TEMP'
           else 'BASE TABLE'
         end table_type
       from sys.all_tables
       union all
       select SYS_CONTEXT('userenv', 'DB_NAME'),
         owner,
         view_name,
         'VIEW'
       from sys.all_views
  )
  select table_catalog as "database_name"
    ,table_name as "name"
    ,table_schema as "schema_name"
    ,case table_type
      when 'BASE TABLE' then 'table'
      when 'VIEW' then 'view'
    end as "kind"
  from tables
  where table_type in ('BASE TABLE', 'VIEW')
    and upper(table_schema) = upper('{{ schema_relation.schema }}')
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro dameng__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
     	select username as "name"
      from sys.all_users
      order by username
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro dameng__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    ALTER {{ from_relation.type }} {{ from_relation }} rename to {{ to_relation.include(schema=False) }}
  {%- endcall %}
{% endmacro %}

{% macro dameng__truncate_relation(relation) -%}
  {% if relation.is_table %}
    {% call statement('truncate_relation') -%}
        truncate table {{ relation }}
    {%- endcall %}
  {% endif %}
{% endmacro %}


{% macro dameng__current_timestamp() -%}
{# docs show not to be implemented currently. #}
 now()
{% endmacro %}

{% macro get_database_name() %}
    {% set results = run_query("select SYS_CONTEXT('userenv', 'DB_NAME') FROM DUAL") %}
    {% set db_name = results.columns[0].values()[0] %}
    {{ return(db_name) }}
{% endmacro %}
