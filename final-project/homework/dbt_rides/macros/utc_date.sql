{#-
  Календарна дата за UTC. Приведення `timestamptz::date` без цього використовує таймзону
  СЕСІЇ (клієнт psycopg, JDBC, DBeaver — у кожного своя), тож та сама подія о 23:30 UTC
  могла б потрапити в різні доби залежно від того, хто запустив запит.
-#}
{% macro utc_date(column) -%}
    (({{ column }}) at time zone 'UTC')::date
{%- endmacro %}
