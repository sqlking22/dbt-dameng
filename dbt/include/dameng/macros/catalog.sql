{% macro dameng__get_catalog(information_schema, schemas)-%}

    {%- call statement('catalog', fetch_result=True) -%}

       {% set database = information_schema.database %}
        {% if database == 'None' or database is undefined or database is none %}
          {% set database = get_database_name() %}
        {% endif %}
        {{ adapter.verify_database(database) }}

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
          ),
          tables as
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
          select
              tables.table_catalog as "table_database",
              tables.table_schema as "table_schema",
              tables.table_name as "table_name",
              tables.table_type as "table_type",
              all_tab_comments.comments as "table_comment",
              columns.column_name as "column_name",
              ordinal_position as "column_index",
              case
                when data_type like '%CHAR%'
                then data_type || '(' || cast(char_length as varchar(10)) || ')'
                else data_type
              end as "column_type",
              all_col_comments.comments as "column_comment",
              tables.table_schema as "table_owner"
          from tables
          inner join columns on upper(columns.table_catalog) = upper(tables.table_catalog)
            and upper(columns.table_schema) = upper(tables.table_schema)
            and upper(columns.table_name) = upper(tables.table_name)
          left join all_tab_comments
            on upper(all_tab_comments.owner) = upper(tables.table_schema)
              and upper(all_tab_comments.table_name) = upper(tables.table_name)
          left join all_col_comments
            on upper(all_col_comments.owner) = upper(columns.table_schema)
              and upper(all_col_comments.table_name) = upper(columns.table_name)
              and upper(all_col_comments.column_name) = upper(columns.column_name)
          where (
              {%- for schema in schemas -%}
                upper(tables.table_schema) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
              {%- endfor -%}
            )
          order by
              tables.table_schema,
              tables.table_name,
              ordinal_position
         {%- endcall -%}

         {{ return(load_result('catalog').table) }}
{%- endmacro %}
