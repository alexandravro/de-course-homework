# Заняття 12. Структуровані дані у Spark. Частина 1: DataFrame API

## Про що це заняття

DataFrame — центральна абстракція сучасного Spark і той інтерфейс, через який виконується переважна більшість продакшн-навантажень. Це заняття — систематичний розбір самого API: з чого складається DataFrame, як адресуються колонки, які класи операцій існують, як поводиться кожна з них щодо shuffle, і які семантичні пастки (передусім NULL) чекають на того, хто переносить звички з SQL або pandas.

Матеріал побудований навколо одного питання: **яку фізичну операцію породжує кожен метод, який ви пишете**. `filter` — це локальний прохід по партиції; `groupBy` — перерозподіл усього датасету по мережі; `join` — одне з чотирьох принципово різних фізичних рішень. Знання цієї відповідності — різниця між кодом, який працює, і кодом, який працює швидко.

Після заняття ви зможете: описати схему явно й пояснити, чому це важливо; свідомо обрати між `select`, `selectExpr` і SQL; коректно працювати з NULL у трьохзначній логіці; будувати агрегації, включно з `rollup`, `cube` і `pivot`; розрізняти типи join-ів і їхні фізичні стратегії; будувати віконні функції з правильним frame; розуміти, коли UDF виправдана, і як записати результат без породження тисяч дрібних файлів.

## Що треба знати заздалегідь

- Архітектура Spark: driver, executor, партиції, job → stage → task, shuffle.
- Ліниве обчислення: трансформації та дії.
- Роль Catalyst: логічний план → оптимізований план → фізичний план.
- SQL на рівні `GROUP BY`, `JOIN`, віконних функцій.
- Колонкові формати зберігання (Parquet).

---

## Основний матеріал

### 1. Що таке DataFrame

#### 1.1 Визначення

**DataFrame** у Spark — це розподілена колекція даних, організована в **іменовані колонки з типами**. Формально це `Dataset[Row]`: незмінна розподілена колекція об'єктів `Row`, до якої додано **схему**.

Три компоненти, що відрізняють DataFrame від RDD:

1. **Схема** — перелік колонок, їхніх типів і nullability. Відома driver-у до початку виконання.
2. **Логічний план** — DataFrame не містить даних; він містить опис того, як їх отримати. `df` — це вузол дерева операцій, а не буфер у пам'яті.
3. **Catalyst** — оскільки Spark знає і схему, і намір, він може переписати ваш запит.

```python
df = spark.read.parquet("s3://lake/trips/")
type(df)         # pyspark.sql.dataframe.DataFrame
df.schema        # StructType([...])  — метадані, доступні одразу
df.columns       # ['trip_id', 'pickup_ts', 'fare_amount', ...]
df.dtypes        # [('trip_id', 'bigint'), ('pickup_ts', 'timestamp'), ...]
```

Жоден із цих викликів не читає дані — вони відповідають з метаданих джерела.

#### 1.2 Система типів

Схема будується з `StructType`, що містить список `StructField(name, dataType, nullable)`.

| Категорія | Типи Spark | Python-відповідник |
|---|---|---|
| Цілі | `ByteType`, `ShortType`, `IntegerType`, `LongType` | `int` |
| Дробові | `FloatType`, `DoubleType`, `DecimalType(p, s)` | `float`, `decimal.Decimal` |
| Рядкові | `StringType`, `VarcharType(n)`, `CharType(n)` | `str` |
| Бінарні | `BinaryType` | `bytes` |
| Логічні | `BooleanType` | `bool` |
| Час | `DateType`, `TimestampType`, `TimestampNTZType` | `datetime.date`, `datetime.datetime` |
| Складені | `ArrayType(T)`, `MapType(K, V)`, `StructType([...])` | `list`, `dict`, `Row` |
| Інтервали | `YearMonthIntervalType`, `DayTimeIntervalType` | — |

Два зауваження, які регулярно коштують часу:

- **`DecimalType` vs `DoubleType`.** Для грошових сум використовуйте `DecimalType(18, 2)`. `DoubleType` — двійкове представлення з плаваючою комою; `0.1 + 0.2 != 0.3`, і накопичена похибка на мільйонах рядків стає видимою у звітах.
- **`TimestampType` vs `TimestampNTZType`.** Перший прив'язаний до часової зони сесії (`spark.sql.session.timeZone`) і при читанні/записі конвертується. Другий (Spark 3.4+) — «наївний» час без зони. Мовчазна конвертація часових зон — класичне джерело зсунутих на кілька годин агрегатів.

#### 1.3 Nullable

Поле `nullable` у `StructField` — це **обіцянка оптимізатору**, а не перевірка під час виконання. Якщо ви оголосили колонку `nullable=False`, а в даних трапиться NULL, Spark не викине помилку — він може згенерувати код, що припускає відсутність NULL, і дати некоректний результат. Nullability — контракт, який дотримуєтеся ви, а не engine.

---

### 2. Читання даних і схема

#### 2.1 Уніфікований інтерфейс читання

```python
df = (spark.read
        .format("parquet")            # parquet | csv | json | orc | avro | jdbc | iceberg | delta
        .option("mergeSchema", "false")
        .load("s3://lake/trips/"))

# Скорочення для поширених форматів
df = spark.read.parquet("s3://lake/trips/")
df = spark.read.csv("data.csv", header=True, inferSchema=False)
df = spark.read.json("events.ndjson")
```

Шлях може вказувати на файл, директорію або glob-шаблон (`/year=2024/month=*/`). Spark сам розкриє Hive-style партиції з імен директорій і додасть їх як колонки.

#### 2.2 Schema inference проти явної схеми

**Schema inference** — Spark читає частину (для JSON/CSV — потенційно всі) даних, щоб вивести типи.

Три причини не покладатися на неї в продакшені:

1. **Вартість.** Для CSV і JSON виведення схеми — це **додатковий повний прохід** по даних перед основним читанням. Для терабайтного датасету це подвоєння роботи.
2. **Недетермінованість.** Типи виводяться з того, що трапилося. Колонка `zip_code`, у якій у першому файлі всі значення числові, стане `bigint` і втратить провідні нулі; у наступному місяці там з'явиться `"01234-5678"`, і тип стане `string`. Схема таблиці змінилася сама по собі.
3. **Втрата контролю.** Гроші стануть `double`, дати — рядками, вкладені структури — довільно впорядкованими.

**Явна схема** усуває всі три проблеми і дає ще одну перевагу: **читання стає точковим**. Spark знає структуру наперед і не відкриває файли заради метаданих.

```python
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DoubleType, TimestampType, IntegerType,
)

schema = StructType([
    StructField("trip_id",     LongType(),      nullable=False),
    StructField("pickup_ts",   TimestampType(), nullable=True),
    StructField("dropoff_ts",  TimestampType(), nullable=True),
    StructField("zone_id",     IntegerType(),   nullable=True),
    StructField("fare_amount", DoubleType(),    nullable=True),
    StructField("payment",     StringType(),    nullable=True),
])

df = spark.read.schema(schema).json("s3://lake/raw/")
```

Схему можна задати і DDL-рядком — коротше й читабельніше:

```python
df = spark.read.schema(
    "trip_id BIGINT, pickup_ts TIMESTAMP, zone_id INT, fare_amount DOUBLE"
).csv(path, header=True)
```

Для Parquet і ORC схема зберігається у footer файлу, тому inference дешевий (читаються лише метадані). Але явна схема все одно корисна: вона документує контракт і ловить розбіжність між очікуваним і фактичним.

#### 2.3 Режими обробки некоректних записів

Для текстових форматів (CSV, JSON) важливо, що робити з рядком, який не відповідає схемі.

| Режим | Поведінка |
|---|---|
| `PERMISSIVE` (за замовчуванням) | Некоректні поля → NULL; увесь сирий рядок можна зберегти в колонку `_corrupt_record` |
| `DROPMALFORMED` | Некоректні рядки мовчки відкидаються |
| `FAILFAST` | Виняток при першому некоректному рядку |

```python
df = (spark.read
        .schema(schema.add("_corrupt_record", StringType()))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path))

bad = df.where(F.col("_corrupt_record").isNotNull())
```

`PERMISSIVE` без збереження `_corrupt_record` — найнебезпечніший варіант: помилки перетворюються на NULL і зникають безслідно. Для pipeline-ів, де коректність важлива, розумний вибір — `PERMISSIVE` з відведенням поганих рядків у окрему quarantine-таблицю або `FAILFAST` там, де краще впасти, ніж пропустити.

#### 2.4 Predicate і projection pushdown

Для колонкових форматів Catalyst передає фільтри й перелік потрібних колонок безпосередньо в reader:

```python
(df.select("trip_id", "fare_amount")
   .where(F.col("fare_amount") > 100)
   .explain("formatted"))

# У плані з'явиться:
#   PushedFilters: [IsNotNull(fare_amount), GreaterThan(fare_amount,100.0)]
#   ReadSchema: struct<trip_id:bigint,fare_amount:double>
```

`ReadSchema` показує, що з файлу читаються **тільки дві колонки**. `PushedFilters` — що row groups із непідходящою статистикою не будуть відкриті взагалі.

Це не працює для CSV і JSON (рядкові формати, статистики немає) і ламається, якщо фільтр загорнуто в Python UDF — Catalyst не бачить його середини.

---

### 3. Колонки й вирази

#### 3.1 Способи адресувати колонку

```python
from pyspark.sql import functions as F

df.select("fare_amount")            # рядок — найпростіше
df.select(df["fare_amount"])        # через індексацію DataFrame
df.select(df.fare_amount)           # через атрибут
df.select(F.col("fare_amount"))     # через col() — універсальний спосіб
F.expr("fare_amount * 2")           # SQL-вираз як рядок
```

`F.col()` — рекомендований варіант: працює з іменами, що містять пробіли й спецсимволи (через backtick-и), не конфліктує з методами DataFrame і дозволяє писати перевикористовувані функції, не прив'язані до конкретного `df`.

Прив'язка `df["col"]` має практичне значення при join-ах: якщо в обох таблицях є колонка `id`, `F.col("id")` буде неоднозначним, а `left["id"]` і `right["id"]` — ні.

#### 3.2 Column — це вираз, а не дані

`Column` не містить значень. Це вузол дерева виразів. Оператори перевантажені й будують дерево:

```python
expr = (F.col("fare_amount") + F.col("tip_amount")) * 1.1
type(expr)     # pyspark.sql.column.Column
```

Практичні наслідки:

- **Логічні оператори — `&`, `|`, `~`, а не `and`, `or`, `not`.** Python-ключові слова не перевантажуються, і `a and b` мовчки поверне не те, що ви очікуєте.
- **Дужки обов'язкові.** `&` має вищий пріоритет за порівняння, тому `F.col("a") > 1 & F.col("b") < 2` розбереться неправильно. Пишіть `(F.col("a") > 1) & (F.col("b") < 2)`.

#### 3.3 Основні родини функцій

Модуль `pyspark.sql.functions` містить кілька сотень функцій. Групи, які покривають більшість задач:

| Родина | Приклади |
|---|---|
| Математичні | `abs`, `round`, `floor`, `ceil`, `sqrt`, `pow`, `log`, `greatest`, `least` |
| Рядкові | `concat`, `concat_ws`, `substring`, `trim`, `lower`, `upper`, `split`, `regexp_extract`, `regexp_replace`, `lpad`, `length` |
| Дата й час | `current_date`, `to_date`, `to_timestamp`, `date_format`, `date_add`, `datediff`, `months_between`, `year`, `month`, `dayofweek`, `hour`, `unix_timestamp`, `date_trunc` |
| Умовні | `when(...).otherwise(...)`, `coalesce`, `nvl`, `nullif`, `isnull`, `isnan` |
| Агрегатні | `count`, `countDistinct`, `sum`, `avg`, `min`, `max`, `stddev`, `variance`, `collect_list`, `collect_set`, `first`, `last` |
| Наближені | `approx_count_distinct`, `percentile_approx` |
| Колекції | `array`, `array_contains`, `explode`, `posexplode`, `size`, `sort_array`, `map_keys`, `map_values`, `struct`, `arrays_zip` |
| Хешування | `md5`, `sha2`, `crc32`, `hash`, `xxhash64` |
| JSON | `from_json`, `to_json`, `get_json_object`, `schema_of_json` |

Правило, яке варто засвоїти раз і назавжди: **якщо задачу можна виразити вбудованою функцією — використовуйте її, а не UDF**. Вбудовані функції потрапляють у whole-stage codegen і виконуються нативно в JVM.

#### 3.4 Умовна логіка

```python
df.withColumn(
    "trip_class",
    F.when(F.col("trip_distance") < 2, "short")
     .when(F.col("trip_distance") < 10, "medium")
     .otherwise("long")
)
```

Без `.otherwise()` рядки, що не відповідають жодній умові, отримають **NULL**, а не помилку. Це один із найчастіших джерел «незрозуміло звідки взялися порожні значення».

---

### 4. Базові операції над DataFrame

#### 4.1 Проєкція

```python
df.select("trip_id", "fare_amount")
df.select(F.col("fare_amount").alias("fare"))
df.selectExpr("trip_id", "fare_amount * 1.2 AS fare_with_fee")

df.withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))
df.withColumns({                       # Spark 3.3+: кілька колонок за один виклик
    "hour":  F.hour("pickup_ts"),
    "is_wk": F.dayofweek("pickup_ts").isin(1, 7),
})
df.withColumnRenamed("old", "new")
df.drop("column_a", "column_b")
```

**Про `withColumn` у циклі.** Кожен виклик додає рівень до логічного плану. Сто послідовних `withColumn` створюють дерево на сто рівнів, і аналіз плану в driver-і починає займати відчутний час (класичний симптом — job «висить» перед стартом). Правильний варіант — один `select` зі списком виразів або `withColumns`:

```python
# Погано
for c in columns:
    df = df.withColumn(c, F.trim(F.col(c)))

# Добре
df = df.select(*[F.trim(F.col(c)).alias(c) for c in columns])
```

#### 4.2 Фільтрація

```python
df.filter(F.col("fare_amount") > 0)
df.where(F.col("fare_amount") > 0)              # where — синонім filter
df.where("fare_amount > 0 AND zone_id IS NOT NULL")   # SQL-рядок
df.where(F.col("payment").isin("cash", "card"))
df.where(F.col("pickup_ts").between("2024-01-01", "2024-02-01"))
df.where(F.col("name").rlike("^A.*"))           # регулярний вираз
```

Фільтрація — **narrow**-трансформація: виконується локально в межах партиції без мережевого обміну.

#### 4.3 Сортування, обмеження, вибірка

```python
df.orderBy(F.col("fare_amount").desc())          # ГЛОБАЛЬНЕ сортування → shuffle
df.sortWithinPartitions("fare_amount")           # локальне, без shuffle
df.limit(100)
df.sample(fraction=0.01, seed=42)                # випадкова вибірка
df.sampleBy("payment", fractions={"cash": 0.1, "card": 0.5}, seed=42)  # стратифікована
```

**`orderBy` — дорога операція.** Глобальне сортування вимагає range-партиціонування: Spark спочатку семплює дані, щоб визначити межі діапазонів, потім шафлить усе за цими межами, потім сортує кожну партицію. Якщо вам не потрібен глобально впорядкований результат (а при записі в файли він зазвичай не потрібен), використовуйте `sortWithinPartitions`.

#### 4.4 Описова статистика

```python
df.describe("fare_amount", "trip_distance").show()   # count, mean, stddev, min, max
df.summary("count", "min", "25%", "50%", "75%", "max").show()
df.select(F.approx_count_distinct("zone_id")).show()
df.stat.corr("trip_distance", "fare_amount")         # кореляція Пірсона
df.stat.approxQuantile("fare_amount", [0.5, 0.95, 0.99], relativeError=0.01)
df.stat.freqItems(["payment"], support=0.05)
```

`approxQuantile` і `approx_count_distinct` використовують імовірнісні структури даних (Greenwald-Khanna і HyperLogLog++). Вони дають відповідь із контрольованою похибкою за один прохід і без повного сортування — на великих обсягах це різниця між секундами й десятками хвилин.

---

### 5. NULL: трьохзначна логіка

Це розділ, який дає найбільше несподіванок, і його варто розібрати окремо.

#### 5.1 Правила

Spark, як і SQL, використовує **трьохзначну логіку**: `TRUE`, `FALSE`, `UNKNOWN` (NULL). Ключові наслідки:

- **NULL не дорівнює нічому, включно з NULL.** `NULL = NULL` → `NULL`, а не `TRUE`.
- **Будь-яка арифметика з NULL дає NULL.** `NULL + 5` → `NULL`.
- **`WHERE` пропускає лише рядки з `TRUE`.** Рядок, для якого умова дала `NULL`, відкидається — так само, як якби вона дала `FALSE`.

Звідси найпоширеніша пастка:

```python
# Здається, що це "всі рядки, крім cash". Насправді — рядки з payment IS NULL теж зникнуть.
df.where(F.col("payment") != "cash")

# Правильно, якщо NULL має потрапити у вибірку:
df.where((F.col("payment") != "cash") | F.col("payment").isNull())
```

Те саме стосується `NOT IN`: якщо в списку є NULL, результат ніколи не буде `TRUE`.

#### 5.2 Null-safe порівняння

```python
df.where(F.col("a").eqNullSafe(F.col("b")))     # NULL <=> NULL → TRUE
```

Оператор `<=>` (у SQL) і `eqNullSafe` (в API) трактує два NULL як рівні. Незамінний при join-ах по колонках, де NULL — легітимне значення.

#### 5.3 Агрегати й NULL

- `count(col)` **не рахує** NULL; `count(*)` рахує всі рядки.
- `sum`, `avg`, `min`, `max` **ігнорують** NULL. `avg` ділить на кількість не-NULL значень, а не на кількість рядків.
- Якщо всі значення NULL, `sum` поверне NULL, а не 0.

```python
df.select(
    F.count("*").alias("rows"),
    F.count("tip_amount").alias("non_null_tips"),
    F.sum(F.col("tip_amount").isNull().cast("int")).alias("null_tips"),
)
```

#### 5.4 Робота з NULL

```python
df.na.drop()                                  # рядки, де є хоч один NULL
df.na.drop(how="all")                         # рядки, де ВСІ колонки NULL
df.na.drop(subset=["trip_id", "pickup_ts"])   # тільки за ключовими колонками
df.na.drop(thresh=5)                          # залишити рядки з ≥5 не-NULL значеннями

df.na.fill(0)                                 # усі числові NULL → 0
df.na.fill({"tip_amount": 0.0, "payment": "unknown"})
df.na.replace(["N/A", ""], None, subset=["payment"])

F.coalesce(F.col("a"), F.col("b"), F.lit(0))  # перше не-NULL
F.nullif(F.col("trip_distance"), F.lit(0))    # 0 → NULL, щоб уникнути ділення на нуль
```

`nullif` варто запам'ятати окремо: `fare / nullif(distance, 0)` замість помилки ділення дає NULL, який далі обробляється звичайними правилами.

#### 5.5 Дедублікація

```python
df.distinct()                                  # повні дублікати
df.dropDuplicates()                            # те саме
df.dropDuplicates(["trip_id"])                 # за підмножиною колонок
```

**Важливо:** `dropDuplicates(["trip_id"])` залишає **недетермінований** рядок із групи — який саме, залежить від порядку обробки партицій. Якщо потрібна детермінована дедублікація (наприклад, «залишити найсвіжіший запис»), використовуйте віконну функцію:

```python
w = Window.partitionBy("trip_id").orderBy(F.col("updated_at").desc())
df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
```

Обидві операції — wide: вимагають shuffle за ключем.

---

### 6. Агрегації

#### 6.1 Базовий синтаксис

```python
(df.groupBy("zone_id")
   .agg(
       F.count("*").alias("trips"),
       F.avg("fare_amount").alias("avg_fare"),
       F.sum("total_amount").alias("revenue"),
       F.max("trip_distance").alias("max_dist"),
       F.countDistinct("payment").alias("payment_types"),
   ))

df.agg(F.sum("total_amount"))     # глобальна агрегація, без groupBy
```

#### 6.2 Що фізично відбувається

`groupBy().agg()` — wide-трансформація, але Spark виконує її у **дві фази**:

```
Stage 1 (partial aggregation, локально в кожній партиції):
    HashAggregate(keys=[zone_id], functions=[partial_count, partial_sum])
              ↓  Exchange hashpartitioning(zone_id, 200)   ← shuffle
Stage 2 (final aggregation):
    HashAggregate(keys=[zone_id], functions=[count, sum])
```

Часткова агрегація на map-стороні критично важлива: замість пересилання всіх сирих рядків через мережу пересилаються вже згорнуті проміжні результати — по одному на унікальний ключ у кожній партиції. Саме тому `groupBy().count()` у DataFrame API поводиться як `reduceByKey`, а не як `groupByKey` у RDD.

Виняток — агрегати, які неможливо згорнути частково: `collect_list`, `collect_set`, точний `countDistinct` над великою кардинальністю. Вони пересилають повний обсяг і є типовим джерелом OOM.

#### 6.3 Багаторівневі агрегації

```python
df.rollup("year", "month").agg(F.sum("revenue"))
# Ієрархічні підсумки: (year, month), (year, NULL), (NULL, NULL)

df.cube("year", "payment").agg(F.sum("revenue"))
# Усі комбінації: (year, payment), (year, NULL), (NULL, payment), (NULL, NULL)

df.groupBy("year").pivot("payment", ["cash", "card", "wallet"]).sum("revenue")
# payment стає колонками
```

Для `pivot` **завжди вказуйте список значень явно**. Без нього Spark виконає додаткову дію, щоб зібрати унікальні значення колонки — тобто окремий повний прохід по даних перед основним запитом.

У результатах `rollup`/`cube` рядки підсумків містять NULL у згорнутих колонках. Щоб відрізнити «це підсумок» від «тут справді NULL», використовуйте `grouping_id()` або `grouping(col)`.

#### 6.4 Наближені агрегати

```python
F.approx_count_distinct("user_id", rsd=0.01)     # HyperLogLog++, похибка ~1%
F.percentile_approx("fare_amount", [0.5, 0.95])  # Greenwald-Khanna
```

Точний `countDistinct` над мільярдом унікальних значень вимагає їх усіх зібрати й порівняти. HyperLogLog++ дає відповідь із похибкою кількох відсотків, використовуючи кілька кілобайтів на групу. Для аналітики це майже завжди прийнятний обмін.

---

### 7. Join-и

#### 7.1 Синтаксис і типи

```python
trips.join(zones, on="zone_id", how="inner")
trips.join(zones, trips["zone_id"] == zones["id"], how="left")
trips.join(zones, on=["zone_id", "borough"], how="inner")   # складений ключ
```

| Тип | Що повертає |
|---|---|
| `inner` | Лише рядки зі збігом в обох таблицях |
| `left` / `left_outer` | Усі рядки лівої; NULL для колонок правої без збігу |
| `right` / `right_outer` | Дзеркально |
| `full` / `full_outer` | Усі рядки обох таблиць |
| `cross` | Декартів добуток |
| `left_semi` | Рядки лівої, для яких **є** збіг; колонки правої не додаються |
| `left_anti` | Рядки лівої, для яких **немає** збігу |

`left_semi` та `left_anti` заслуговують окремої уваги: вони роблять те саме, що `WHERE EXISTS` / `WHERE NOT EXISTS`, але без ризику розмноження рядків. Класична помилка — використати `inner join` для перевірки наявності й отримати дублікати, бо в правій таблиці ключ не унікальний.

#### 7.2 Синтаксис `on` і дублювання колонок

- **`on="zone_id"`** (рядок або список рядків) — Spark склеює колонку й залишає **одну** `zone_id` у результаті.
- **`on=left["zone_id"] == right["id"]`** (умова) — у результаті залишаються **обидві** колонки. Якщо їх назви збігаються, подальше звернення `F.col("zone_id")` буде неоднозначним і впаде з `AMBIGUOUS_REFERENCE`.

Практичне правило: після join за умовою одразу робіть `select` із явними префіксами або перейменовуйте колонки до join-у.

#### 7.3 Фізичні стратегії

Catalyst обирає одну з чотирьох реалізацій. Це та частина, яку варто перевіряти в `explain()`.

| Стратегія | Умова вибору | Механіка | Вартість |
|---|---|---|---|
| **Broadcast Hash Join** | Одна сторона менша за `spark.sql.autoBroadcastJoinThreshold` (10 МБ) | Мала таблиця збирається в driver і розсилається на всі executor-и; велика не шафлиться | Найдешевша |
| **Shuffle Hash Join** | Обидві великі, одна помітно менша; `spark.sql.join.preferSortMergeJoin=false` | Обидві шафляться за ключем; на кожній партиції будується хеш-таблиця з меншої сторони | Середня |
| **Sort-Merge Join** | Обидві великі (варіант за замовчуванням) | Обидві шафляться і сортуються за ключем, потім злиття | Дорога |
| **Broadcast Nested Loop Join** | Немає рівності в умові з'єднання | Порівняння кожного з кожним | Дуже дорога |

**Broadcast join** — найважливіший практичний прийом. Lookup-таблиці (зони, категорії, валюти, календар) майже завжди маленькі, і broadcast усуває найдорожчу частину join-у:

```python
from pyspark.sql.functions import broadcast

trips.join(broadcast(zones), on="zone_id", how="left")
```

Явна підказка `broadcast()` потрібна, коли Spark не знає розміру таблиці (немає статистики, джерело без метаданих) або коли оцінка помилкова. Обмеження: таблиця має вміститися в пам'ять **driver-а** (він її збирає) і в пам'ять **кожного** executor-а. Broadcast таблиці на 2 ГБ на 100 executor-ів — надійний спосіб покласти застосунок.

#### 7.4 Пастки

**Розмноження рядків.** Якщо ключ не унікальний у правій таблиці, кількість рядків після inner join зростає мультиплікативно. Перед join-ом варто перевірити:

```python
zones.groupBy("zone_id").count().where("count > 1").show()
```

**NULL у ключі.** `NULL = NULL` → NULL, тому рядки з NULL у ключі не з'єднаються **ніколи**, навіть з іншими NULL. При цьому в left join вони збережуться з NULL-ами праворуч. Якщо NULL має вважатися значенням — `eqNullSafe`.

**Skew.** Якщо 40 % рядків мають один і той самий ключ (часто — `NULL` або дефолтне значення), відповідна партиція після shuffle стає гігантською, і один task працює на порядок довше за решту.

**Порядок фільтрації.** Фільтруйте **до** join-у. Catalyst часто робить це сам (predicate pushdown через join), але не завжди — особливо для outer join-ів, де перенесення фільтра змінює семантику.

---

### 8. Операції над множинами

```python
a.union(b)              # за ПОЗИЦІЄЮ колонок; схеми мають збігатися за порядком
a.unionByName(b)        # за ІМЕНАМИ колонок
a.unionByName(b, allowMissingColumns=True)   # відсутні колонки → NULL

a.intersect(b)          # спільні рядки, з дедублікацією
a.intersectAll(b)       # зі збереженням дублікатів
a.exceptAll(b)          # рядки a, яких немає в b, зі збереженням дублікатів
a.subtract(b)           # те саме, з дедублікацією
```

**`union` працює за позицією.** Це найпідступніша поведінка в цьому розділі: якщо в двох DataFrame однакові колонки, але в різному порядку, `union` мовчки склеїть `fare_amount` з `tip_amount`. Помилки не буде — буде некоректний результат. У продакшн-коді використовуйте **тільки `unionByName`**.

`union` — narrow-операція (просто об'єднання партицій, без shuffle). `intersect`, `except`, `distinct` — wide.

---

### 9. Віконні функції

#### 9.1 Структура вікна

Віконна функція обчислює значення для кожного рядка на основі **групи пов'язаних рядків**, не згортаючи їх у один. Вікно задається трьома компонентами:

```python
from pyspark.sql.window import Window

w = (Window
     .partitionBy("zone_id")                        # 1. розбиття
     .orderBy(F.col("pickup_ts"))                   # 2. упорядкування
     .rowsBetween(Window.unboundedPreceding, 0))    # 3. frame
```

1. **`partitionBy`** — на які групи розбити дані. Без нього вікном стає **весь датасет**, і всі дані збираються в одну партицію: `WARN WindowExec: No Partition Defined for Window operation!` — гарантована деградація.
2. **`orderBy`** — порядок усередині групи. Обов'язковий для ранжуючих і зсувних функцій.
3. **Frame** — які саме рядки навколо поточного враховувати.

#### 9.2 Frame: ROWS проти RANGE

| Тип | Семантика |
|---|---|
| `rowsBetween(a, b)` | Фізична кількість **рядків** до й після поточного |
| `rangeBetween(a, b)` | Логічний діапазон **значень** колонки сортування |

```python
# Ковзне середнє за 3 рядки
Window.partitionBy("zone").orderBy("ts").rowsBetween(-2, 0)

# Сума за останні 7 днів (за значенням, а не кількістю рядків)
days = lambda n: n * 86400
Window.partitionBy("zone").orderBy(F.col("ts").cast("long")).rangeBetween(-days(7), 0)
```

Межі: `Window.unboundedPreceding`, `Window.currentRow`, `Window.unboundedFollowing` або цілі числа.

**Frame за замовчуванням залежить від наявності `orderBy`:**
- З `orderBy` — `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (наростаючий підсумок).
- Без `orderBy` — `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` (уся партиція).

Тобто `F.sum("x").over(Window.partitionBy("z").orderBy("t"))` дає **наростаючу суму**, а не суму по групі. Це регулярне джерело плутанини.

#### 9.3 Родини віконних функцій

**Ранжуючі:**
```python
F.row_number().over(w)     # 1, 2, 3, 4 — унікальний номер, нічия розривається довільно
F.rank().over(w)           # 1, 1, 3, 4 — однакові значення → однаковий ранг, з пропуском
F.dense_rank().over(w)     # 1, 1, 2, 3 — без пропуску
F.ntile(4).over(w)         # номер квартиля
F.percent_rank().over(w)   # відносний ранг у [0, 1]
```

**Зсувні:**
```python
F.lag("fare", 1).over(w)          # значення попереднього рядка
F.lead("fare", 1).over(w)         # значення наступного
F.lag("fare", 1, 0.0).over(w)     # зі значенням за замовчуванням замість NULL
```

**Агрегатні у вікні:** будь-який агрегат (`sum`, `avg`, `count`, `min`, `max`) працює з `.over(w)`, повертаючи результат для кожного рядка.

#### 9.4 Типові патерни

```python
# Top-N у кожній групі
w = Window.partitionBy("zone_id").orderBy(F.col("fare").desc())
top3 = df.withColumn("rn", F.row_number().over(w)).where("rn <= 3").drop("rn")

# Різниця з попереднім значенням
w = Window.partitionBy("zone_id").orderBy("ts")
df.withColumn("delta", F.col("fare") - F.lag("fare", 1).over(w))

# Наростаючий підсумок
df.withColumn("running_total", F.sum("fare").over(
    Window.partitionBy("zone_id").orderBy("ts")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)))

# Частка від підсумку групи
df.withColumn("share", F.col("fare") / F.sum("fare").over(Window.partitionBy("zone_id")))
```

#### 9.5 Вартість

Віконна функція — **wide**-операція: `Exchange hashpartitioning(partitionBy-колонки)` плюс сортування всередині партиції. У плані це виглядає як `Window` над `Sort` над `Exchange`.

Оптимізації: якщо кілька віконних функцій використовують **однакове** вікно, Spark обчислює їх за один прохід — тому групуйте функції з однаковою специфікацією. Уникайте вікна без `partitionBy`. Стежте за skew: непропорційно велика група означає один довгий task.

---

### 10. Spark SQL: той самий engine, інший синтаксис

#### 10.1 Еквівалентність

```python
df.createOrReplaceTempView("trips")

sql_result = spark.sql("""
    SELECT zone_id, COUNT(*) AS trips, AVG(fare_amount) AS avg_fare
    FROM trips
    WHERE fare_amount > 0
    GROUP BY zone_id
    ORDER BY trips DESC
""")

api_result = (df.where(F.col("fare_amount") > 0)
                .groupBy("zone_id")
                .agg(F.count("*").alias("trips"),
                     F.avg("fare_amount").alias("avg_fare"))
                .orderBy(F.desc("trips")))
```

Обидва варіанти проходять через **той самий Catalyst** і дають **ідентичний фізичний план**. Перевіряється це порівнянням `explain()`. Вибір між ними — питання читабельності й контексту, а не продуктивності.

![Вкладка SQL у Spark UI](https://spark.apache.org/docs/latest/img/webui-sql-tab.png)

*Вкладка SQL/DataFrame у Spark UI: кожен запит — незалежно від того, написаний він на SQL чи через DataFrame API — потрапляє сюди з тривалістю, пов'язаними job-ами й посиланням на фізичний план. Джерело: [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)*

#### 10.2 Види view-ів

| Метод | Область видимості | Час життя |
|---|---|---|
| `createOrReplaceTempView(name)` | Поточна `SparkSession` | До завершення сесії |
| `createOrReplaceGlobalTempView(name)` | Усі сесії застосунку, база `global_temp` | До завершення застосунку |
| `saveAsTable(name)` | Записується в каталог (Hive/Glue) | Постійно, з даними |

#### 10.3 Каталог

```python
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("trips")
spark.catalog.tableExists("db.trips")
spark.catalog.cacheTable("trips")
```

#### 10.4 Коли що обирати

**SQL зручніший** для складних багатоступеневих запитів із CTE, для команди з сильним SQL-бекграундом, для перенесення наявної логіки зі сховища.

**DataFrame API зручніший** там, де потрібна композиція: функції, що приймають і повертають DataFrame, параметризовані списки колонок, перевикористовувані бібліотеки трансформацій, юніт-тести.

```python
def clean_amounts(df):
    return df.where((F.col("fare_amount") > 0) & (F.col("trip_distance") > 0))

def add_time_parts(df):
    return df.withColumns({
        "pickup_date": F.to_date("pickup_ts"),
        "pickup_hour": F.hour("pickup_ts"),
    })

result = df.transform(clean_amounts).transform(add_time_parts)
```

Метод `df.transform(fn)` дозволяє будувати ланцюжки з власних функцій, зберігаючи читабельність — це основа тестованого Spark-коду.

---

### 11. UDF, Arrow і pandas API

#### 11.1 Чому Python UDF дорогі

Spark виконується в JVM. Python UDF означає, що для кожної партиції запускається окремий Python-процес, і дані ходять між ними:

```
Executor JVM ──серіалізація──► Python worker ──обробка──► ──десеріалізація──► JVM
```

![Вартість копіювання й серіалізації між системами](https://arrow.apache.org/img/copy.png)

*Класична проблема обміну даними між рантаймами: кожен перехід межі вимагає серіалізації й копіювання. Джерело: [Apache Arrow Overview](https://arrow.apache.org/overview/)*

До вартості серіалізації додається головне: **Catalyst не бачить середини UDF**. Він не може протягнути через неї фільтр, не може викинути невикористані колонки, не може згенерувати код. UDF стає чорною скринькою в середині оптимізованого плану.

#### 11.2 Apache Arrow і векторизовані UDF

**Apache Arrow** — стандартний колонковий формат представлення даних у пам'яті. Його призначення — усунути серіалізацію при переході між системами: обидві сторони працюють з однаковим бінарним макетом.

![Спільне представлення в пам'яті через Arrow](https://arrow.apache.org/img/shared.png)

*Arrow як спільний формат у пам'яті: замість попарних конвертацій між системами всі використовують одне представлення. Джерело: [Apache Arrow Overview](https://arrow.apache.org/overview/)*

![Колонковий макет і векторизація](https://arrow.apache.org/img/simd.png)

*Колонкове розташування дозволяє процесору обробляти значення пачками через SIMD-інструкції. Джерело: [Apache Arrow Overview](https://arrow.apache.org/overview/)*

**Pandas UDF** використовують Arrow для передачі даних пачками: функція отримує не окреме значення, а `pandas.Series`.

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def normalize(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()

df.withColumn("fare_z", normalize("fare_amount"))
```

Типовий виграш проти звичайної UDF — **у 10–100 разів**. Ввімкнення Arrow для конвертацій:

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

**Ієрархія вибору:** вбудована функція → SQL-вираз через `expr` → pandas UDF → звичайна Python UDF.

#### 11.3 Pandas API on Spark

`pyspark.pandas` надає pandas-сумісний інтерфейс, що виконується розподілено:

```python
import pyspark.pandas as ps

psdf = df.pandas_api()
psdf["hour"] = psdf["pickup_ts"].dt.hour
result = psdf.groupby("payment").agg({"fare_amount": "mean"})
sdf = result.to_spark()
```

| | `pandas` | `pyspark.pandas` | DataFrame API |
|---|---|---|---|
| Виконання | Одна машина | Розподілено | Розподілено |
| Обмеження обсягу | RAM машини | Немає | Немає |
| Синтаксис | pandas | pandas | Spark |
| Контроль над планом | — | Обмежений | Повний |

Призначення — міграція наявного pandas-коду й дослідницький аналіз. Для продакшн-pipeline-ів кращий нативний DataFrame API: він явніший щодо того, де відбувається shuffle. Окрема пастка — операції, які в pandas безкоштовні, а в розподіленому середовищі означають глобальне сортування (`sort_index`, робота з індексом, `iloc` по позиції).

---

### 12. Кешування

#### 12.1 Навіщо

DataFrame не зберігає результат. Кожна дія перераховує весь ланцюг від джерела. Якщо той самий проміжний результат потрібен кільком діям — він рахується стільки ж разів.

```python
clean = df.where(...).join(...).withColumn(...)

clean.count()                 # прохід 1: читання + join + трансформації
clean.groupBy(...).count()    # прохід 2: те саме заново
clean.write.parquet(...)      # прохід 3: те саме заново
```

```python
clean.cache()
clean.count()      # матеріалізація + збереження
clean.groupBy(...) # з кешу
clean.unpersist()  # звільнити
```

#### 12.2 Рівні зберігання

| Рівень | Де | Серіалізація |
|---|---|---|
| `MEMORY_ONLY` | JVM heap як об'єкти | Ні |
| `MEMORY_AND_DISK` | Heap, надлишок → локальний диск | Ні (за замовчуванням для DataFrame) |
| `MEMORY_ONLY_SER` | Heap, серіалізовано | Так — компактніше, дорожче читати |
| `MEMORY_AND_DISK_SER` | Комбінація | Так |
| `DISK_ONLY` | Тільки диск | Так |
| `*_2` | Той самий рівень із реплікацією ×2 | — |

`cache()` — це `persist(MEMORY_AND_DISK)`.

![Деталі закешованого датасету у Spark UI](https://spark.apache.org/docs/latest/img/webui-storage-detail.png)

*Вкладка Storage показує кожну закешовану партицію: рівень зберігання, розмір у пам'яті й на диску, вузол. Джерело: [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)*

#### 12.3 Правила

- Кешуйте лише те, що **читається більше одного разу різними діями**.
- `cache()` — ліниве оголошення; матеріалізація відбувається при першій дії.
- Завжди викликайте `unpersist()`, коли дані більше не потрібні: кеш конкурує за пам'ять із обчисленнями, і execution має пріоритет — надлишковий кеш просто витісниться, а витрачений на нього час пропаде.
- Перевіряйте вкладку Storage: якщо закешовано не 100 % партицій, кеш може приносити більше шкоди, ніж користі.
- Кешування вихідного DataFrame безпосередньо перед єдиним записом — марна робота.

---

### 13. Партиціонування в пам'яті

#### 13.1 repartition проти coalesce

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Так, повний (`Exchange` у плані) | Ні |
| Напрямок | Збільшити або зменшити | Тільки зменшити |
| Рівномірність | Рівномірний розподіл | Може бути нерівномірним |
| Вплив на попередній stage | Немає | **Є** — знижує паралелізм усього stage |

```python
df.rdd.getNumPartitions()

df.repartition(200)                    # рівномірно, з shuffle
df.repartition("zone_id")              # hash-партиціонування за колонкою
df.repartition(50, "zone_id")          # і кількість, і колонка
df.repartitionByRange(50, "pickup_ts") # діапазонне партиціонування
df.coalesce(10)                        # злиття без shuffle
```

**Прихована пастка `coalesce`.** Оскільки він не створює межу stage, зменшення кількості партицій «піднімається» вгору по плану: `df.map(...).coalesce(1)` означає, що **весь** stage, включно з `map`, виконається в одному task-у. Якщо потрібно зменшити кількість файлів на виході, але зберегти паралелізм обчислень — використовуйте `repartition`, який ставить межу stage.

**`repartition("col")` перед агрегацією або join-ом за тією ж колонкою** дозволяє Spark пропустити наступний shuffle: дані вже розподілені потрібним чином. Перевіряється у плані — зайвий `Exchange` зникає.

---

### 14. Запис даних

#### 14.1 Базовий інтерфейс і режими

```python
(df.write
   .format("parquet")
   .mode("overwrite")
   .option("compression", "snappy")
   .save("s3://lake/out/"))
```

| Режим | Поведінка, якщо шлях існує |
|---|---|
| `errorifexists` (за замовчуванням) | Виняток |
| `append` | Дописати нові файли |
| `overwrite` | Видалити наявні дані й записати заново |
| `ignore` | Нічого не робити |

**`overwrite` небезпечний.** За замовчуванням він видаляє **весь** цільовий каталог до початку запису. Якщо job упаде посередині, ви залишитеся без старих даних і без нових. Часткове перезаписування лише зачеплених партицій вмикається окремо:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

#### 14.2 Кількість вихідних файлів

Правило просте: **одна непорожня партиція DataFrame → один файл**. Звідси:

```python
df.write.parquet(out)              # стільки файлів, скільки партицій (часто 200)
df.coalesce(1).write.parquet(out)  # один файл — і один task на весь запис
df.repartition(20).write.parquet(out)
```

Цільовий розмір файлу для аналітики — **128 МБ – 1 ГБ**. Кілька тисяч файлів по 200 КБ — це деградація читання (кожен файл — окремий запит до сховища й окремий footer) і зростання вартості на object storage.

#### 14.3 partitionBy і small files problem

```python
df.write.partitionBy("year", "month").parquet(out)
```

Створює Hive-style структуру директорій `year=2024/month=01/`. Це дає partition pruning при читанні.

**Проблема:** без попереднього перерозподілу **кожна** партиція DataFrame записує свій шматок у **кожну** директорію. 200 партицій × 12 місяців = 2400 дрібних файлів.

**Рішення** — узгодити партиціонування в пам'яті з партиціонуванням на диску:

```python
(df.repartition("year", "month")
   .write.partitionBy("year", "month")
   .parquet(out))
```

Тепер усі рядки одного місяця опиняються в одній партиції, і на кожну директорію припадає один файл.

**Вибір колонки для partitionBy:** тільки низька кардинальність (дата, регіон, тип події). Партиціонування за `user_id` створить мільйони директорій із файлами по кілька кілобайтів і зробить таблицю непрацездатною.

#### 14.4 Сортування й bucketing

```python
# Сортування всередині кожного файлу — покращує pruning за статистикою
df.sortWithinPartitions("fare_amount").write.parquet(out)

# Bucketing — фіксований розподіл за хешем, тільки для таблиць у каталозі
(df.write
   .bucketBy(64, "user_id")
   .sortBy("user_id")
   .saveAsTable("db.events"))
```

Bucketing призначений для повторюваних join-ів за однією й тією ж колонкою: якщо обидві таблиці розкладені по однакових бакетах, join виконується без shuffle. Ціна — жорсткість схеми й необхідність узгоджувати кількість бакетів між таблицями.

---

## Ключові терміни

| Термін | Визначення |
|---|---|
| **DataFrame** | Розподілена колекція `Row` зі схемою; `Dataset[Row]`. Опис обчислення, а не дані |
| **Schema / StructType / StructField** | Опис колонок: ім'я, тип, nullability |
| **Schema inference** | Автоматичне виведення типів із даних; додатковий прохід і недетермінований результат |
| **Nullable** | Обіцянка оптимізатору, що NULL не буде; під час виконання не перевіряється |
| **PERMISSIVE / DROPMALFORMED / FAILFAST** | Режими обробки записів, що не відповідають схемі |
| **`_corrupt_record`** | Колонка, у яку зберігається сирий текст некоректного рядка |
| **Predicate pushdown** | Передача фільтра в reader джерела; видно як `PushedFilters` у плані |
| **Projection pushdown** | Читання лише потрібних колонок; видно як `ReadSchema` |
| **Column** | Вираз, а не дані; будується операторами й функціями |
| **`F.col` / `F.expr` / `selectExpr`** | Способи побудувати вираз із імені або SQL-рядка |
| **Трьохзначна логіка** | TRUE / FALSE / UNKNOWN; `NULL = NULL` → NULL |
| **`eqNullSafe` (`<=>`)** | Порівняння, у якому два NULL вважаються рівними |
| **`coalesce` (функція)** | Перше не-NULL значення зі списку виразів |
| **`nullif`** | Перетворює задане значення на NULL; захист від ділення на нуль |
| **`na.drop` / `na.fill` / `na.replace`** | Операції обробки відсутніх значень |
| **`dropDuplicates`** | Дедублікація; за підмножиною колонок залишає недетермінований рядок |
| **Partial aggregation** | Часткова агрегація на map-стороні до shuffle |
| **`rollup` / `cube` / `pivot`** | Ієрархічні підсумки / усі комбінації / розворот значень у колонки |
| **`approx_count_distinct`** | HyperLogLog++: наближена кількість унікальних із контрольованою похибкою |
| **`percentile_approx`** | Наближені квантилі за один прохід |
| **`left_semi` / `left_anti`** | Фільтрація за наявністю/відсутністю збігу без додавання колонок і без розмноження рядків |
| **Broadcast hash join** | Broadcast малої сторони на всі executor-и; join без shuffle великої таблиці |
| **Shuffle hash join** | Обидві сторони шафляться; хеш-таблиця будується з меншої |
| **Sort-merge join** | Обидві сторони шафляться і сортуються; варіант за замовчуванням для великих таблиць |
| **`autoBroadcastJoinThreshold`** | Поріг автоматичного broadcast, за замовчуванням 10 МБ |
| **`union` / `unionByName`** | Об'єднання за позицією колонок / за іменами |
| **Window specification** | `partitionBy` + `orderBy` + frame |
| **`rowsBetween` / `rangeBetween`** | Frame за кількістю рядків / за діапазоном значень |
| **`row_number` / `rank` / `dense_rank`** | Ранжуючі функції з різною обробкою нічиїх |
| **`lag` / `lead`** | Доступ до значення попереднього/наступного рядка у вікні |
| **Temp view / global temp view** | Реєстрація DataFrame як таблиці для SQL у межах сесії / застосунку |
| **`df.transform(fn)`** | Застосування власної функції в ланцюжку; основа композиції |
| **Python UDF** | Порядкова функція з серіалізацією в окремий Python-процес; непрозора для Catalyst |
| **pandas UDF** | Векторизована UDF: дані передаються пачками через Arrow |
| **Apache Arrow** | Колонковий формат у пам'яті, що усуває серіалізацію між JVM і Python |
| **`pyspark.pandas`** | pandas-сумісний API поверх Spark |
| **StorageLevel** | Рівень кешування: пам'ять / диск, серіалізація, реплікація |
| **`repartition` / `coalesce`** | Зміна кількості партицій із shuffle / без нього |
| **`repartitionByRange`** | Діапазонне партиціонування за значеннями колонки |
| **Write mode** | `errorifexists`, `append`, `overwrite`, `ignore` |
| **`partitionBy` (запис)** | Hive-style директорії `col=value/` на диску |
| **`partitionOverwriteMode=dynamic`** | Перезапис лише зачеплених партицій замість усього каталогу |
| **`bucketBy`** | Фіксований розподіл за хешем колонки для join-ів без shuffle |
| **Small files problem** | Тисячі дрібних файлів через невідповідність партиціонування в пам'яті та на диску |

---

## Перевір себе

1. DataFrame «не містить даних». Що тоді містить змінна `df` після `spark.read.parquet(...)` і чому `df.schema` відповідає миттєво?

2. Назвіть три причини не покладатися на schema inference у продакшені. Для якого формату inference найдешевший і чому?

3. Чим `PERMISSIVE` без `_corrupt_record` небезпечніший за `FAILFAST`?

4. Як перевірити в плані, що predicate і projection pushdown спрацювали? На які саме рядки `explain("formatted")` треба дивитися?

5. Чому в PySpark треба писати `&` і `|` замість `and` і `or`, і чому дужки навколо порівнянь обов'язкові?

6. Запит `df.where(F.col("payment") != "cash")` не повернув рядки, де `payment IS NULL`. Поясніть причину через трьохзначну логіку і напишіть коректний варіант.

7. У чому різниця між `count("*")` і `count("tip_amount")`? Що поверне `sum("tip")`, якщо всі значення NULL?

8. Чому `dropDuplicates(["trip_id"])` недетермінований? Напишіть детермінований варіант «залишити найсвіжіший запис».

9. Поясніть двофазну агрегацію. Чому `collect_list` не отримує виграшу від часткової агрегації і чим це загрожує?

10. Чому для `pivot` треба явно передавати список значень? Що станеться, якщо цього не зробити?

11. Ви перевіряєте, чи є в lookup-таблиці запис для кожної поїздки. Чому `left_semi` тут кращий за `inner join`?

12. Опишіть чотири фізичні стратегії join і умови, за яких Catalyst обирає кожну.

13. Коли явна підказка `broadcast()` виправдана, і які два обмеження за пам'яттю треба врахувати перед її використанням?

14. Чому `union` у продакшн-коді варто замінити на `unionByName`? Наведіть сценарій, у якому `union` дає некоректний результат без жодної помилки.

15. Що поверне `F.sum("x").over(Window.partitionBy("z").orderBy("t"))` — суму по групі чи наростаючий підсумок? Поясніть через frame за замовчуванням.

16. У чому різниця між `rowsBetween` і `rangeBetween`? Наведіть задачу, для якої правильний лише один із них.

17. Що означає попередження `No Partition Defined for Window operation` і чим воно загрожує?

18. DataFrame API і Spark SQL дають ідентичний фізичний план. Чому тоді для бібліотек трансформацій зазвичай обирають API?

19. Поясніть, чому Python UDF дорога, і назвіть дві причини, не пов'язані з серіалізацією.

20. Що дає Apache Arrow при використанні pandas UDF? Чому виграш вимірюється десятками разів, а не відсотками?

21. Ви викликали `cache()` і одразу `write.parquet()`. Яка користь від кешу в цьому сценарії?

22. `df.map(...).coalesce(1)` виконується в один потік. Поясніть чому і як це виправити.

23. `df.write.partitionBy("year", "month")` породив 2400 файлів. Поясніть механізм і напишіть виправлений код.

24. За якими критеріями обирається колонка для `partitionBy` при записі? Чому `user_id` — погана ідея?
