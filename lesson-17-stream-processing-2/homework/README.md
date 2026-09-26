# L17 — Домашнє завдання: Stream processing зі Spark Structured Streaming

Ви будуєте потоковий job, який читає події **GitHub Archive** з файлового source,
рахує їх у **tumbling-вікнах** за **event time** з **watermark** і пише результат у
parquet. Це продовження L16 (там був event broker — Kafka; тут — *обробка* потоку).

Повна специфікація, контракти і бали — у [SPEC.md](SPEC.md).

## Передумови

- Python-середовище курсу (`uv`), де є `pyspark`, `polars`, `pytest`.
- Java для Spark (як і в L12–L15).
- Мережа **не потрібна**: семпл `data/landing/2024-01-15-12.json.gz` закомічений.

## Структура

```
homework/
├── streaming_job.py        # ← ваш код (заповнити TODO)
├── verify.sh               # прогін job + pytest
├── data/landing/*.json.gz  # реальний семпл gharchive (дано)
└── tests/test_output.py    # перевірки на згенерованих даних
```

## Як працювати

Усі команди — з каталогу `homework/` (шляхи в коді відносні до нього).

```bash
cd homework

# 1. Реалізуйте TODO у streaming_job.py (6 функцій, див. SPEC.md).

# 2. Прогоніть свій job — він згенерує data/output/windowed/ і data/output/summary.json
uv run python streaming_job.py

# 3. Самоперевірка — тести звіряють вивід із контрольними числами
uv run pytest -q

# Або все разом:
./verify.sh tasks       # ваш код; має стати PASS, коли все реалізовано
```

`./verify.sh solution` запускає довідкову реалізацію (`../solution/streaming_job.py`) —
підглянути очікувану поведінку можна, але здавати треба **свій** `streaming_job.py`.

## Що оцінюється

`./verify.sh tasks` має давати **8 passed** (= як `solution`). Бали по функціях — у
[SPEC.md](SPEC.md). Стартові стаби кидають `NotImplementedError`, тож до реалізації
job падає — це нормально.

## Підказки

- File source **вимагає явної схеми** — Spark не виводить її з JSON у стрімі.
- `event_time = to_timestamp(created_at)`; вікна рахуються за **event time**, не за часом
  обробки.
- `data/output/` і `data/checkpoints/` — у `.gitignore`; не комітьте їх.
- Якщо вивід порожній — згадайте, чому тут `foreachBatch`, а не append-sink (SPEC.md).
- Полари читають Spark-вивід через glob: `data/output/windowed/*.parquet` (бо поряд є
  `_SUCCESS`/`.crc`).
