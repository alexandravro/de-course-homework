# Діаграми: Потокова обробка даних. Частина 1 — event brokers

Чотири Mermaid-діаграми для Lesson 16. Розкривають архітектуру Kafka від рівня cluster
до рівня CDC pipeline та загального контексту medallion-архітектури проєкту.

---

## 1. Kafka — основна архітектура

```mermaid
flowchart LR
    subgraph PRODUCERS["Producers"]
        P1["producer.py\n(NYC Taxi replay)"]
        P2["App producer\n(інший сервіс)"]
    end

    subgraph CLUSTER["Kafka Cluster (KRaft, без ZooKeeper)"]
        direction TB
        subgraph B1["Broker 1 (Controller)"]
            T0P0["topic: taxi-trips\nPartition 0 — Leader\nPartition 1 — Replica"]
        end
        subgraph B2["Broker 2"]
            T0P1["topic: taxi-trips\nPartition 1 — Leader\nPartition 2 — Replica"]
        end
        subgraph B3["Broker 3"]
            T0P2["topic: taxi-trips\nPartition 2 — Leader\nPartition 0 — Replica"]
        end
    end

    subgraph CGA["Consumer Group A — Spark Streaming"]
        CA1["Consumer A1\n→ Partition 0"]
        CA2["Consumer A2\n→ Partition 1"]
        CA3["Consumer A3\n→ Partition 2"]
    end

    subgraph CGB["Consumer Group B — Analytics dashboard"]
        CB1["Consumer B1\n→ Partition 0, 1, 2"]
    end

    P1 -- "key=pu_location_id\n(hash → partition)" --> CLUSTER
    P2 --> CLUSTER

    CLUSTER --> CA1
    CLUSTER --> CA2
    CLUSTER --> CA3
    CLUSTER --> CB1
```

Kafka Cluster складається з кількох broker-ів. Кожен broker зберігає певні partition-и як
Leader (приймає запис/читання) і решту як Replica (резервні копії). Producer надсилає
повідомлення з ключем — broker маршрутизує до partition за формулою
`partition = hash(key) % num_partitions`. Дві consumer groups читають той самий topic
незалежно: Group A розподіляє партиції між трьома consumer-ами (паралелізм),
Group B читає всі partition одним consumer-ом. Ordering гарантований лише всередині
однієї partition.

---

## 2. Topic / partition / offset — деталь

```mermaid
flowchart TD
    subgraph TOPIC["Topic: taxi-trips"]
        direction TB

        subgraph P0["Partition 0  (key: pu_location_id=132 → hash mod 3 = 0)"]
            direction LR
            E00["offset 0\nevent₀"] --> E01["offset 1\nevent₁"] --> E02["offset 2\nevent₂"] --> E03["offset 3\nevent₃"] --> TAIL0["... append-only log"]
        end

        subgraph P1["Partition 1  (key: pu_location_id=161 → hash mod 3 = 1)"]
            direction LR
            E10["offset 0\nevent₀"] --> E11["offset 1\nevent₁"] --> E12["offset 2\nevent₂"] --> TAIL1["..."]
        end

        subgraph P2["Partition 2  (key: pu_location_id=48 → hash mod 3 = 2)"]
            direction LR
            E20["offset 0\nevent₀"] --> E21["offset 1\nevent₁"] --> TAIL2["..."]
        end
    end

    subgraph GROUPS["Consumer Groups — незалежні offsets"]
        direction LR
        GrpA["Group A\nP0 @ offset 3\nP1 @ offset 2\nP2 @ offset 1"]
        GrpB["Group B\nP0 @ offset 1\nP1 @ offset 1\nP2 @ offset 0"]
    end

    P0 --> GrpA
    P1 --> GrpA
    P2 --> GrpA
    P0 --> GrpB
    P1 --> GrpB
    P2 --> GrpB
```

Кожна partition — це append-only ordered log. Нові events дописуються в кінець;
старі не видаляються (до закінчення retention, default 7 днів). Offset — монотонно
зростаючий ID події всередині partition, що дозволяє replay: consumer може перечитати
дані, встановивши offset назад. Ключ повідомлення (`pu_location_id` у прикладі проєкту)
визначає partition через хеш — всі поїздки з одного zone потрапляють до однієї
partition, забезпечуючи локальний ordering per zone. Кожна consumer group зберігає
свій власний offset у внутрішньому топіку `__consumer_offsets` — Group A і Group B
читають повністю незалежно.

---

## 3. CDC pipeline (Change Data Capture)

> Концептуальна діаграма — слайди, без live-демо цього заняття (`code/` більше не
> піднімає Postgres/Kafka Connect/Debezium; спрощення стеку).

```mermaid
flowchart LR
    subgraph SRC["Source OLTP DB — Postgres"]
        direction TB
        TBL["Таблиця: public.taxi_zone\n(INSERT / UPDATE / DELETE)"]
        WAL["WAL\n(Write-Ahead Log)\nwal_level=logical\nreplication slot: taxi_zone_slot"]
        TBL -. "кожна зміна\nзаписується у" .-> WAL
    end

    subgraph CONNECT["Kafka Connect + Debezium"]
        DBZ["Debezium\nPostgresConnector\n(pgoutput plugin)\ntopic.prefix=cdc"]
        SNAP["snapshot.mode=initial\n(перший знімок таблиці\nпри старті)"]
        DBZ --- SNAP
    end

    subgraph KAFKA["Kafka Broker"]
        direction TB
        TOPIC1["topic: cdc.public.taxi_zone\n(один топік на таблицю)"]
        SCHEMA["Schema Registry\n(JSON / Avro contract)"]
        TOPIC1 --- SCHEMA
    end

    subgraph SINKS["Downstream Consumers (sinks)"]
        LAKE["Data Lake\n(Bronze layer — raw events)"]
        DWH["Data Warehouse\n(Silver/Gold — materialize)"]
        DASH["Dashboard /\nreal-time app"]
    end

    WAL -- "log-based CDC\n(реплікація потоку змін)" --> DBZ
    DBZ -- "change events\n{op: c/u/d, before, after}" --> TOPIC1
    TOPIC1 --> LAKE
    TOPIC1 --> DWH
    TOPIC1 --> DASH
```

**Log-based CDC vs query-based polling:**

| | Log-based CDC (Debezium + WAL) | Query-based polling |
|---|---|---|
| Механізм | Читає WAL / binlog бази даних | Виконує `SELECT WHERE updated_at > last_run` |
| Overhead на БД | Мінімальний | Зростає з розміром таблиці |
| DELETE видимий? | Так — через WAL | Ні — рядок вже видалено |
| Затримка | Мілісекунди | Хвилини (залежить від polling interval) |

Debezium читає PostgreSQL WAL через replication slot (`pgoutput` plugin), перетворює
кожну зміну рядка на change event з полями `op` (c=create, u=update, d=delete),
`before` (стан до) і `after` (стан після) — і публікує в Kafka topic `cdc.public.taxi_zone`
(формат: `{prefix}.{schema}.{table}`). Downstream consumers отримують повний потік змін
у реальному часі без додаткового навантаження на БД.

---

## 4. End-to-end streaming — Kafka у medallion-архітектурі NYC TLC

```mermaid
flowchart TD
    subgraph OPS["Операційні системи"]
        APP["NYC TLC\nParquet-файли\n(офлайн batch)"]
        DEVICES["Диспетчерські\nсистеми\n(майбутній live feed)"]
    end

    subgraph KAFKA["Kafka Cluster (Lesson 16)"]
        TRIPS_T["topic: taxi-trips\n(Avro, Schema Registry)\nproducer.py replay"]
    end

    subgraph LAKE["Data Lakehouse — Apache Iceberg на локальному диску / S3"]
        BRONZE["Bronze Layer\n(raw events — незмінені)"]
        SILVER["Silver Layer\n(очищені, de-duped,\nwatermark-фільтрація — L17)"]
        GOLD["Gold Layer\n(агрегати: поїздки/зона/година)"]
    end

    subgraph CONSUMERS["Consumers / Processing"]
        SPARK["Spark Structured Streaming\n(Lesson 17)"]
        PY_CON["consumer.py\n(Python demo, L16)"]
    end

    APP -- "producer.py\nSPEED=1000" --> TRIPS_T
    DEVICES -. "майбутній\nreal-time feed" .-> TRIPS_T

    TRIPS_T --> PY_CON
    TRIPS_T --> SPARK

    SPARK --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
```

Kafka є центральним event broker-ом між операційними системами та аналітичним
Lakehouse. На Lesson 16 встановлюється інфраструктура: producer публікує NYC TLC
taxi trips як Avro events (ключ = `pu_location_id` → ordering per zone). Consumer Group A
(Spark Structured Streaming) підключиться у Lesson 17 — саме там з'являться watermark,
late data handling і запис у Bronze/Silver шари Iceberg. Константа `LATE_EVENT_FRACTION`
у `producer.py` вмикається (вручну, у файлі) у L17 для демонстрації watermark-поведінки.
CDC (діаграма 3 вище) сюди навмисно не включений — цього заняття він лишається
слайдовим матеріалом, без живого Postgres/Debezium у `code/`.
