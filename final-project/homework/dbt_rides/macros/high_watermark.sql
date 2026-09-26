{#-
  Нижня межа інкременту: найбільший `_ingested_at`, який ця модель уже обробила.
  Перший запуск (таблиці ще немає) сюди не заходить — див. is_incremental() у моделях.

  `-infinity` замість NULL: порожня таблиця після `--full-refresh` теж дає валідну межу.
-#}
{% macro high_watermark(column='_ingested_at') -%}
    (select coalesce(max({{ column }}), '-infinity'::timestamptz) from {{ this }})
{%- endmacro %}
