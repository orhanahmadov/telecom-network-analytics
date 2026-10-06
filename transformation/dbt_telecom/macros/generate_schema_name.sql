{#
  Found live: dbt's default generate_schema_name macro PREFIXES a model's custom schema
  (the `+schema: staging` / `+schema: curated` config in dbt_project.yml) with the
  profile's base schema (profiles.yml `schema: staging`), producing `staging_staging` and
  `staging_curated` - not the `raw` / `staging` / `curated` schemas infra/postgres/init.sql
  actually creates. This override makes a model's `schema` config the literal schema name,
  matching every other reference to `staging.*` / `curated.*` in this project (dbt docs,
  SQL, profiles.yml, init.sql). See https://docs.getdbt.com/docs/build/custom-schemas.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
