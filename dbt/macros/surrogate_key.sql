{#
  A surrogate key: a short, stable id for a dimension row, built by hashing its natural key.
  md5('TAMPINES') is the same on every run, so the fact and the dimension compute the same key
  independently and rebuilds never reshuffle ids (row_number() would, as soon as a new town appears).
#}
{% macro surrogate_key(column) -%}
    md5(coalesce(cast({{ column }} as text), ''))
{%- endmacro %}
