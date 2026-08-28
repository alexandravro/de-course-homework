# Заняття 07. Контейнери

## Навіщо це заняття

Уяви ситуацію: Data Engineer написав pipeline, протестував його на своєму MacBook, відправив колезі — і в колеги нічого не запускається. «Works on my machine» — одна з найвідоміших фраз у розробці програмного забезпечення, і вона кожного дня зупиняє реальні проєкти. Версія Python інша. Бібліотека є, але з іншою версією. Системна залежність не встановлена. Змінна середовища забута.

**Контейнери** вирішують цю проблему: вони пакують застосунок разом із усіма його залежностями у **відтворюване ізольоване середовище**, яке однаково поводиться на ноутбуці розробника, на CI-сервері і в хмарі. Для Data Engineer-а це — фундаментальна навичка, бо ніякий pipeline «в продакшені» не запускається просто як `python my_script.py` на голому сервері.

Після цього заняття ти зможеш написати Dockerfile від наївного першого кроку до production-ready образу, підняти multi-container стек через Docker Compose, зрозуміти, як саме ізоляція досягається на рівні ядра Linux, і мати концептуальне уявлення про Kubernetes для production.

Це заняття — також міст до наступного: Airflow піднімається через Docker Compose поверх Postgres за тим самим патерном, який ти освоїш тут.

## Що треба знати заздалегідь

З заняття 04 — ти вже запускав `docker run` для ClickHouse. Docker встановлений. З занять 05–06 — у тебе є готовий ELT-pipeline (Path A), який тепер треба контейнеризувати і підготувати до оркестрації. Базове розуміння командного рядка (Linux/macOS) і того, що таке файлова система і процес.

---

## Контейнери vs Віртуальні машини

### Звідки виникли VM

Колись типова практика — **один застосунок на сервер**. Якщо застосунок падав, він міг поламати весь сервер. Якщо потрібен новий застосунок — потрібен новий сервер. Компанії купували залізо «з запасом», і воно простоювало. Це була дорога і нееластична модель.

**VMware** вирішила це через **Virtual Machine (VM)**: технологія дозволила безпечно запускати кілька застосунків на одному сервері через гіпервізор. Гіпервізор (VMware, VirtualBox, KVM) емулює залізо; кожна VM — це повноцінна **Guest OS** зі своїм ядром, файловою системою, мережевим стеком.

Але у VM є суттєвий overhead: кожна займає 2–20 GB дискового простору і 30–90 секунд для старту. Якщо треба підняти 10 мікросервісів — це 10 повних ОС, навіть якщо кожен сервіс важить кілька мегабайтів.

### Контейнери: ізольований процес без Guest OS

Контейнер — це **ізольований UNIX-процес**. Він **не несе власної ОС**: замість цього він використовує ядро хост-ОС через два Linux-механізми — **namespaces** і **cgroups**.

```
VM:                                        Container:
┌──────────────────────────────┐           ┌──────────────────────────────┐
│  App A  │  App B  │  App C   │           │  App A  │  App B  │  App C   │
│  Libs   │  Libs   │  Libs    │           │  Libs   │  Libs   │  Libs    │
│ Guest OS│ Guest OS│ Guest OS │           ├──────────────────────────────┤
├──────────────────────────────┤           │       Docker Engine          │
│         Hypervisor           │           │  Host OS + Kernel (shared)   │
│         Host OS              │           │         Hardware             │
│         Hardware             │           └──────────────────────────────┘
└──────────────────────────────┘
```

**Namespaces** ізолюють те, що процес **бачить**:
- **PID namespace** — контейнер бачить свої процеси з власними ідентифікаторами (всередині контейнера є «process 1», але на хості це зовсім інший PID)
- **NET namespace** — незалежний мережевий стек: свій IP, таблиця маршрутизації, портів
- **MNT namespace** — ізольовані точки монтування: контейнер бачить свою файлову систему

**cgroups (control groups)** обмежують те, що процес **споживає**:
- CPU (максимальна частка процесора)
- Memory (ліміт RAM)
- Block I/O (швидкість читання/запису на диск)
- Кількість процесів (pids)

Результат: образ 10–500 MB замість 2–20 GB, старт менше ніж за секунду замість 30–90 секунд, мінімальний overhead.

**Мотивація для Data Engineer-а:** контейнер вирішує «works on my machine» — відтворюване середовище, яке однаково поводиться локально, на CI і в cloud.

---

## Container Runtimes і Docker

Docker — не єдиний container runtime. Є Podman (без daemon, сумісний за CLI), containerd (вбудований у Kubernetes). Але Docker лишається стандартом де-факто для локальної розробки і навчання, тому далі говоримо про нього.

### Image vs Container: рецепт і страва

- **Image** — пакунок, доступний лише для читання (read-only): містить код застосунку, залежності, мінімальний набір бібліотек ОС і метадані. З одного image можна запустити багато контейнерів.
- **Container** — запущений **instance** image: отримує власний writable-шар поверх read-only layers image.

Аналогія: image — це рецепт страви; container — це страва, приготована за ним. Зі стравою ти щось робиш (їси), але рецепт від цього не змінюється. Можна приготувати десять страв за одним рецептом одночасно.

```bash
docker pull python:3.12-slim                  # завантажити image один раз
docker run --rm python:3.12-slim python -c "print('привіт з контейнера')"
docker ps          # запущені контейнери
docker ps -a       # усі, включно зупиненими
```

### Layers і кешування: чому порядок у Dockerfile важливий

Docker будує image, **стекуючи незалежні шари (layers)**: кожна інструкція Dockerfile (`FROM`, `RUN`, `COPY`, …) = новий незмінний шар. Готовий image — стек цих шарів, але виглядає як єдина файлова система.

Шари **кешуються**: якщо шар не змінився, при наступній збірці Docker бере його з кешу — не перебудовує. Це дає колосальне прискорення розробки.

**Правило:** рідко змінювані шари — вгору, часто змінювані — вниз. Залежності копіюємо й встановлюємо **окремим шаром, перед кодом**. Поки `requirements.txt` не змінився, шар `pip install` береться з кешу — навіть якщо ти щойно змінив `main.py`.

Якби `COPY . .` стояв перед `pip install`: будь-яка правка будь-якого файлу коду тягла б повторну установку всіх залежностей — у великому проєкті це хвилини замість секунд.

### Типи базових образів

- **standard** — повний образ ОС (ubuntu, debian)
- **slim** — мінімальний образ на базі стандартного дистрибутива (python:3.12-slim)
- **alpine / busybox** — образ на базі мінімального дистрибутива Alpine Linux (~5 MB)
- **distroless** — runtime + застосунок, без shell і package manager (максимальна безпека)
- **scratch** — порожній базовий образ (для статично скомпільованих програм)

Для більшості Python-задач достатньо `*-slim`. Чим менший образ, тим швидший pull/build і менша attack surface (менше потенційних вразливостей).

---

## Як влаштований Docker Engine

Розуміти архітектуру Docker Engine корисно, щоб не думати про «Docker» як про один магічний бінарник. Насправді це кілька компонентів:

- **Docker daemon** — API-сервер, що слухає запити від `docker` CLI і клієнтів
- **containerd** — high-level runtime: керує life cycle контейнерів (pull, create, start, stop, delete). Спілкується з daemon через gRPC API.
- **runc** — low-level runtime: безпосередньо взаємодіє з ядром Linux через namespaces і cgroups. Виконує lifecycle-події контейнера.
- **shims** — посередники між containerd і різними low-level runtimes, що дозволяють використовувати альтернативні runtimes

Спрощений потік: `docker run` → daemon → containerd → runc → ядро Linux (namespaces + cgroups).

Ця архітектура важлива для розуміння: контейнери **не прив'язані жорстко до Docker**. Kubernetes використовує containerd напряму, без Docker daemon.

---

## Dockerfile: від наївного до production-ready

Кращий спосіб зрозуміти Dockerfile — бачити, як він еволюціонує і чому кожна зміна важлива.

### Крок 1: Наївно, але працює

```dockerfile
# Dockerfile.step1
FROM python:3.12-slim
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "main.py"]
```

Що не так:
1. `COPY . .` перед `pip install` — будь-яка правка коду тягне повторну установку всіх залежностей
2. В образ потрапляє весь вміст директорії (включно з `__pycache__`, `.env`, README)
3. Процес запускається від **root** — небезпечно

### Крок 2: WORKDIR і порядок шарів

```dockerfile
# Dockerfile.step2
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "main.py"]
```

Тепер залежності — окремий шар перед кодом. При повторній збірці (якщо `requirements.txt` не змінився) шар `pip install` буде `CACHED`. Лише шар `COPY . .` перебудовується.

`WORKDIR /app` встановлює робочу директорію всередині контейнера: усі відносні шляхи будуть відносно `/app`.

### Крок 3: ENTRYPOINT і .dockerignore

```dockerfile
# Dockerfile.step3
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "main.py"]
```

Різниця між `CMD` і `ENTRYPOINT`:
- `CMD ["python", "main.py"]` — повністю замінюється при `docker run <image> bash` → запускається bash
- `ENTRYPOINT ["python", "main.py"]` — фіксує програму; при `docker run <image> --help` аргумент `--help` **передається до** `python main.py --help`

Для застосунку, де `main.py` — єдина точка входу, ENTRYPOINT чіткіше виражає намір: «цей контейнер завжди запускає саме цю програму».

**.dockerignore** — аналог `.gitignore` для Docker. Виключає файли з build context (все, що Docker бачить при збірці):

```
# .dockerignore
__pycache__/
*.pyc
.env
.env.*
README.md
Dockerfile*
docker-compose.yml
.dockerignore
```

Без `.dockerignore` у build context потрапляють `__pycache__`, `.env` зі секретами, README, великі data-файли. Образ більший, збірка повільніша, секрети можуть «просочитися».

### Фінал: non-root і змінні оточення

```dockerfile
# Dockerfile (фінальна версія)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 1000 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py events.csv ./

USER appuser
ENTRYPOINT ["python", "main.py"]
```

**`PYTHONUNBUFFERED=1`** — коли stdout не термінал (а в контейнері він саме такий), Python буферизує вивід і віддає його блоками. Наслідок: `docker logs` мовчить, поки процес не завершиться. Це та сама ситуація «контейнер висить, логів нема».

**`PYTHONDONTWRITEBYTECODE=1`** — `.pyc` нікому перевикористовувати: контейнер живе один запуск.

**`--no-cache-dir`** — pip за замовчуванням лишає завантажені пакети в `~/.cache/pip`. В образі цей кеш не потрібен: другого `pip install` не буде. На маленькому проєкті це десятки мегабайтів, на pandas + pyarrow — сотні.

**non-root (`USER appuser`):** за замовчуванням процес у контейнері запускається від root — і це той самий uid 0, що й на хості. Якщо атакуючий отримає доступ до процесу, він буде root у контейнері. `USER appuser` запускає процес від непривілейованого користувача. Ставиться **в кінці**, після всіх операцій, що потребують root (`useradd`, `pip install`).

Перевірка:

```bash
docker run --rm --entrypoint whoami <image>                      # appuser
docker run --rm --entrypoint sh <image> -c 'touch /app/new.py'   # Permission denied
```

Код лишається власністю root, тому процес може його читати й виконувати, але не переписати. Якщо застосунку треба кудись писати — створіть саме той каталог і віддайте його користувачу: `RUN mkdir /cache && chown appuser /cache`.

### Exec-form: чому ENTRYPOINT пишуть масивом

`ENTRYPOINT python main.py` (shell-form) розгортається в `/bin/sh -c "python main.py"`: PID 1 у контейнері — це `sh`, а Python — його дитина. `docker stop` шле SIGTERM у PID 1, чекає 10 секунд і вбиває через SIGKILL; `sh` сигнал дітям не передає.

Exec-form `ENTRYPOINT ["python", "main.py"]` робить PID 1 самим Python. Але цього мало: **ядро не застосовує дії за замовчуванням до PID 1** — сигнал без явного обробника PID 1 просто ігнорує. Тому довгоживучий процес має сам поставити обробник (`signal.signal(signal.SIGTERM, ...)`) або його запускають із `docker run --init`.

| варіант | PID 1 | `docker stop` |
|---|---|---|
| shell-form | `/bin/sh -c python main.py` | 10 с → SIGKILL |
| exec-form, без обробника | `python main.py` | 10 с → SIGKILL |
| exec-form + обробник SIGTERM | `python main.py` | миттєво |
| exec-form + `docker run --init` | `/sbin/docker-init` | миттєво |

Для батч-скрипта, який відпрацював і вийшов, це неважливо. Для Kafka consumer або Spark driver — це різниця між «коректно завершив роботу» і «вбитий на середині транзакції».

---

## Docker Compose: multi-container стеки

Один контейнер — ще не система. Реальний pipeline: застосунок + база даних + брокер повідомлень. Замість десятка `docker run` з довжелезними флагами — один YAML-файл.

### Стек: app + Postgres

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 10

  app:
    build: .
    image: events-demo:latest
    environment:
      PGHOST: postgres          # звертаємось до БД за іменем сервісу!
      PGPORT: "5432"
      PGUSER: ${POSTGRES_USER}
      PGPASSWORD: ${POSTGRES_PASSWORD}
      PGDATABASE: ${POSTGRES_DB}
    depends_on:
      postgres:
        condition: service_healthy   # стартуємо лише коли БД готова

volumes:
  pgdata:
```

Зверни увагу на кілька важливих деталей:

**`PGHOST: postgres`** — не `localhost`, не IP-адреса. У Compose-мережі сервіси звертаються один до одного **за іменем сервісу**. Це одна з найчастіших помилок новачків.

**`healthcheck`** + **`depends_on: condition: service_healthy`** — без цього `app` може спробувати підключитись до Postgres, поки той ще ініціалізується, і впасти. `pg_isready` перевіряє, що Postgres прийматиме з'єднання. App стартує лише після того, як healthcheck пройде.

### Ключові команди

```bash
docker compose up -d --build       # підняти стек (зібравши образ app)
docker compose ps -a               # стан усіх сервісів
docker compose logs -f app         # стрімити логи сервісу app
docker compose exec postgres psql -U user -d db   # команда в запущеному контейнері
docker compose down                # зупинити стек (volumes лишаються!)
docker compose down -v             # зупинити + знести named volumes
```

### Volumes: persistent дані

Коли контейнер зупиняється — writable-шар знищується. Все, що там було записано (включно з даними Postgres), зникає. Це правильна поведінка для stateless-застосунків, але катастрофа для баз даних.

**Volume** — окремий об'єкт Docker, **не прив'язаний до lifecycle контейнера**. Named volume `pgdata` переживає `docker compose down` і `docker compose up`:

```bash
docker compose down    # без -v: контейнери знесені, pgdata — на місці
docker compose up -d   # Postgres піднявся, дані в базі збережені
```

Відмінності:
- **Named volume** (`pgdata:/var/lib/...`) — керується Docker, зберігається в системному місці
- **Bind mount** (`./data:/app/data`) — монтує конкретну директорію хоста в контейнер; зручно для розробки (зміни в коді відразу видно в контейнері)

### Docker Networking

Compose автоматично створює одну **bridge network** для всіх сервісів стека. Усі сервіси в ній можуть звертатись один до одного за іменем сервісу.

Архітектура Docker networking складається з трьох рівнів:
- **Container Network Model (CNM)** — специфікація (що повинне бути)
- **libnetwork** — реалізація CNM (як це зроблено)
- **Drivers** — конкретні топології:
  - **bridge** (за замовчуванням в Compose): всі контейнери на одному хості, virtual switch
  - **overlay**: layer-2 мережа через кілька хостів (для Swarm/Kubernetes)
  - **macvlan**: контейнер отримує власний IP/MAC на фізичній мережі

Кожен контейнер має **sandbox** (ізольований мережевий стек), **endpoints** (підключення до мережі) і бачить лише **networks**, до яких підключений.

### Environment variables і секрети

Credentials передаємо через змінні середовища. Compose читає їх із файлу `.env`:

```bash
cp .env.example .env    # налаштовуємо локально
```

**Ніколи не хардкодьте секрети у `docker-compose.yml` або Dockerfile.** Файл `.env` — у `.gitignore`. Якщо залити `PGPASSWORD=secret` у репозиторій — це не виправляється видаленням коміту (він залишається в history).

### Compose profiles: запускати лише те, що потрібно

У великому проєктному `docker-compose.yml` можна мати 10+ сервісів. Щоб не піднімати весь стек (8–16 GB RAM), сервіси групують **профілями**:

```bash
docker compose up -d                       # тільки default-сервіси
docker compose --profile spark up -d       # + Spark-кластер
docker compose --profile streaming up -d   # + Kafka + Zookeeper
```

---

## Best practices написання image: чекліст

- **Один процес на контейнер** — не запускати syslog + застосунок в одному образі
- **Мінімальні базові образи** — Alpine, slim, distroless
- **Pin версії тегів** — не `FROM python:latest`, а `FROM python:3.12-slim`
- **Об'єднувати `RUN`-кроки** — менше шарів, менший розмір (`RUN apt update && apt install -y ... && rm -rf /var/lib/apt/lists/*`)
- **`.dockerignore`** — виключати `__pycache__`, логи, data-файли, `.env`
- **Exec-form `ENTRYPOINT`** — `["python","main.py"]`, а не рядком: PID 1 має бути вашим процесом
- **non-root** — `USER appuser` для runtime-процесу
- **Docker Content Trust** — криптографічний підпис образів для верифікації джерела
- **Docker Scout** — сканування образу на вразливості перед деплоєм

---

## Kubernetes: концептуальний огляд

Docker Compose — для локальної розробки і простих сценаріїв. У production, де треба запускати тисячі контейнерів, забезпечити auto-scaling, rolling updates, self-healing — використовують **Kubernetes (K8s)**.

Базові концепти:

- **Pod** — мінімальна одиниця деплою в Kubernetes. Містить один або кілька контейнерів, що ділять мережу і storage.
- **ReplicaSet** — гарантує, що завжди запущена задана кількість однакових pods.
- **Deployment** — декларативне управління pods: `replicas: 3` — і K8s сам підтримує три копії, перезапускає впалі, робить rolling update.
- **Service** — стабільний endpoint для набору pods: DNS-ім'я і балансування навантаження.

**Де Kubernetes зустрічається в Data Engineering:**
- Spark on Kubernetes (Spark Operator) — замість YARN
- Airflow on Kubernetes (KubernetesPodOperator, Kubernetes Executor) — кожна Airflow task у своєму pod
- Kafka on Kubernetes (Strimzi) — managed Kafka-кластер

Kubernetes виходить за рамки цього курсу, але ти маєш знати, що він існує і навіщо — бо він присутній у більшості job descriptions для Data Engineers.

---

## Ключові терміни

| Термін | Визначення |
|---|---|
| **Container** | Ізольований UNIX-процес із власним filesystem, мережею, PID. Використовує ядро хоста через namespaces + cgroups — без власної Guest OS. |
| **Namespaces (PID/NET/MNT)** | Linux-механізм ізоляції того, що процес **бачить**: процеси, мережевий стек, точки монтування. |
| **cgroups** | Linux-механізм обмеження того, що процес **споживає**: CPU, memory, block I/O, pids. |
| **Image** | Read-only пакунок із шарами: код, залежності, конструкції ОС, метадані. Будується Dockerfile-ом. |
| **Layer** | Незмінний шар image; кешується між builds. Кожна інструкція Dockerfile = новий шар. |
| **Dockerfile** | Декларативний опис побудови image: FROM, RUN, COPY, ENTRYPOINT/CMD. |
| **ENTRYPOINT** | Фіксує програму контейнера; аргументи до `docker run` передаються їй. |
| **CMD** | Аргументи за замовчуванням для ENTRYPOINT, або команда, що повністю замінюється при `docker run`. |
| **non-root** | Запуск процесу від непривілейованого користувача (USER appuser) — зменшує наслідки компрометації. |
| **Docker Engine** | daemon (API) + containerd (high-level runtime) + runc (low-level runtime) + shims. |
| **containerd** | High-level container runtime: керує lifecycle контейнерів, pull images. |
| **runc** | Low-level runtime: взаємодіє з ядром Linux, виконує lifecycle-події. |
| **Docker Compose** | Інструмент для визначення і запуску multi-container стеків через YAML. |
| **Volume** | Persistent storage, не прив'язаний до lifecycle контейнера. Named volume або bind mount. |
| **Healthcheck** | Перевірка готовності сервісу; `depends_on: condition: service_healthy` запускає залежний сервіс лише після успішного healthcheck. |
| **Bridge network** | Default-мережа Compose: всі сервіси стека можуть звертатись один до одного за іменем сервісу. |
| **.dockerignore** | Файл, що виключає файли/директорії з build context — аналог .gitignore для Docker. |
| **Profile** | Compose: умовна активація групи сервісів за допомогою `--profile <name>`. |
| **Stateful / stateless** | Застосунок, що керує persistent-даними (Postgres) / не керує (REST API). |
| **Kubernetes** | Платформа оркестрації контейнерів для production: Pod, ReplicaSet, Deployment, Service. |

---

## Перевір себе

1. Чим принципово відрізняється контейнер від віртуальної машини? Поясни конкретні Linux-механізми (namespaces і cgroups), які забезпечують ізоляцію контейнера.
2. Чому порядок інструкцій у Dockerfile впливає на швидкість збірки? Поясни, що таке layer cache і як це пов'язано з розміщенням `COPY requirements.txt` перед `COPY . .`.
3. У чому різниця між `ENTRYPOINT` і `CMD`? Наведи конкретний приклад, коли кожен з них поводиться по-різному при запуску `docker run <image> --help`.
4. Чому `USER appuser` ставлять у кінці Dockerfile, а не одразу після `FROM`? Що зламається, якщо поставити його раніше?
5. Чому у Compose-стеку `app` звертається до Postgres як `PGHOST: postgres`, а не `PGHOST: localhost`?
6. Що відбудеться з даними в Postgres, якщо запустити `docker compose down` без флагу `-v`? А з флагом `-v`? У чому різниця між named volume і bind mount?
7. Навіщо потрібен `healthcheck` у Postgres-сервісі і `depends_on: condition: service_healthy` у app-сервісі? Що трапиться без цієї конструкції?
8. Що таке Kubernetes і в яких сценаріях Data Engineering він використовується? Назви три конкретні приклади.
