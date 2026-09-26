# Lesson 15 — Optimization Walkthrough

Практична частина заняття — у ноутбуці **`lesson_15.ipynb`** (у цій же теці).

Формат: кожен параметр показано двома сусідніми клітинками з різними значеннями.
Запусти клітинку → подивись на число у виводі та у **Spark UI** (`localhost:4040`) → запусти наступну → порівняй.

| # | Параметр / тема | Що порівнюємо |
|---|---|---|
| 1 | `spark.sql.shuffle.partitions` | `.rdd.getNumPartitions()` результату `groupBy`: 200 vs 16 |
| 2 | `spark.sql.adaptive.enabled` | та сама кількість партицій: 200 vs «стисло» (AQE coalesce) |
| 3 | `spark.sql.adaptive.advisoryPartitionSizeInBytes` | кількість вихідних файлів = партицій: 32 MB vs 2 MB |
| 4 | `spark.sql.autoBroadcastJoinThreshold` | тип join у compact-плані: SortMergeJoin (-1) vs BroadcastHashJoin (10m) |
| 5 | Spill: `spark.sql.shuffle.partitions` 1 vs 64 | час + колонки Spill (memory/disk) у Stages |
| 6 | Skew + salting | таблиця Tasks у Stages: одна задача ≫ решти; salted-план — два `Exchange` |
| 7 | `spark.sql.adaptive.skewJoin.enabled` | час join false vs true; max Duration задачі падає |
| 8 | UDF | compact-план: codegen vs `ArrowEvalPython` vs `BatchEvalPython` + час |
| 9 | `coalesce(1)` vs `repartition(1)` | `Coalesce` (1 task на все) vs `Exchange` (паралельно) + час |
| 10 | Кеш і повторне обчислення | час 2 дій; `Scan parquet` → `InMemoryTableScan`; вкладка Storage |
| 11 | Розмір вихідних файлів | `(к-ть файлів, середній KB)`: 200 партицій vs `coalesce(16)` |
| 12 | Модель памʼяті executor-а | вкладка Executors: Execution / Storage / GC Time; exit 137 |

Теоретичний конспект — `../knowledge.md`. Демо-рунбук для викладача — `../demo.md`.
Event log кожного прогону пишеться у `spark-events/` (Spark History Server).
