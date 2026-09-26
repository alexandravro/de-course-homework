"""Генератор корпусу подій ride-hailing маркетплейсу. ДАНО. Не редагуйте.

Це «система-джерело»: застосунки водія й пасажира та billing шлють події про життєвий
цикл поїздки. Дані напівструктуровані — спільний конверт плюс `payload`, який у кожного
типу події свій.

Корпус ДЕТЕРМІНОВАНИЙ: за тим самим `seed` і `n_rides` виходить той самий набір подій,
ті самі `event_id`, ті самі суми, ті самі `occurred_at` (див. `EVENT_TIME_ANCHOR` у
producer.py). Тому checkpoint-числа зі SPEC.md не залежать від того, коли ви запустили стек.

Бруд у потоці — навмисний, він відтворює реальні збої ingestion:
  * дублікати   — Kafka дає at-least-once, частина подій публікується двічі (той самий event_id);
  * late events — застосунок був офлайн і злив буфер через 5-40 хвилин після події;
  * порядок     — частина подій публікується з дрібним зсувом, і `ride_completed` може
                  прийти раніше за `ride_started`;
  * тестовий трафік — навантажувальні поїздки з `source = "loadtest"` (усі їхні події);
  * невідомі зони — 264/265 у довіднику зон ("Unknown" / "Outside of NYC");
  * NULL-и      — готівкові поїздки приходять без чайових (NULL, а не 0);
  * факт ≠ намір — у ~5% поїздок фактична зона посадки/висадки відрізняється від заявленої.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass

# --- параметри світу ---------------------------------------------------------
N_RIDERS = 300
N_DRIVERS = 80
VEHICLE_TYPES = ["standard", "standard", "xl", "lux"]  # ~50/25/25 серед водіїв
PAYMENT_METHODS = ["card", "card", "card", "cash", "wallet"]  # ~60/20/20
APP_VERSIONS = ["5.1.0", "5.2.1", "5.3.0", "6.0.0-beta"]
PLATFORMS = ["ios", "android"]
CANCEL_REASONS = [
    "rider_changed_mind",
    "driver_too_far",
    "wrong_pickup",
    "no_show",
    "price_too_high",
]
# Популярні зони Мангеттена + хвіст решти довідника (location_id 1..263).
HOT_ZONES = [132, 138, 161, 162, 163, 170, 186, 230, 234, 236, 237, 239, 249]
UNKNOWN_ZONES = [264, 265]

# Частки бруду (частка від подій / поїздок).
DUPLICATE_RATE = 0.03
LATE_RATE = 0.04
OUT_OF_ORDER_RATE = 0.01
LOADTEST_RIDE_RATE = 0.02
UNKNOWN_ZONE_RATE = 0.015
ZONE_CHANGE_RATE = 0.05

# Тариф: база + за кілометр; множник попиту застосовується до суми.
FARE_BASE = 3.00
FARE_PER_KM = 2.50
TOLL_AMOUNT = 6.55
SURGE_CHOICES = [1.0, 1.0, 1.0, 1.2, 1.5, 2.0]


@dataclass(frozen=True, slots=True)
class Driver:
    driver_id: str
    vehicle_type: str
    medallion: str
    base_rating: float


@dataclass(frozen=True, slots=True)
class PlannedEvent:
    """Подія з двома незалежними часовими координатами.

    `event_offset_s` — коли подія СТАЛАСЯ (event time), секунди від початку корпусу.
    `emit_offset_s`  — коли її ОПУБЛІКОВАНО (processing time). Для late events більший за
                       `event_offset_s`, для дублікатів — ще більший.
    Обидві координати логічні; producer ділить emit на SPEED і прив'язує до wall clock.
    """

    event_offset_s: float
    emit_offset_s: float
    event: dict


def build_drivers(rng: random.Random) -> list[Driver]:
    """Пул водіїв. Автомобіль у водія ОДИН і той самий; рейтинг у кожній поїздці «дрейфує»."""
    return [
        Driver(
            driver_id=f"d-{i:03d}",
            vehicle_type=rng.choice(VEHICLE_TYPES),
            medallion=f"M{rng.randint(1000, 9999)}",
            base_rating=round(rng.uniform(4.3, 4.9), 2),
        )
        for i in range(1, N_DRIVERS + 1)
    ]


def _zone(rng: random.Random) -> int:
    if rng.random() < UNKNOWN_ZONE_RATE:
        return rng.choice(UNKNOWN_ZONES)
    if rng.random() < 0.55:
        return rng.choice(HOT_ZONES)
    return rng.randint(1, 263)


def _money(value: float) -> float:
    return round(value + 1e-9, 2)


def _ride_plan(
    rng: random.Random,
    ride_no: int,
    t_start: float,
    drivers_by_type: dict[str, list[Driver]],
) -> list[PlannedEvent]:
    """Події однієї поїздки. Час усередині поїздки — секунди від її початку."""
    ride_id = f"r-{ride_no:05d}"
    is_loadtest = rng.random() < LOADTEST_RIDE_RATE
    rider_id = f"test-{rng.randint(1, 9)}" if is_loadtest else f"u-{rng.randint(1, N_RIDERS):04d}"
    src_rider = "loadtest" if is_loadtest else "rider_app"
    src_driver = "loadtest" if is_loadtest else "driver_app"
    src_billing = "loadtest" if is_loadtest else "billing"

    requested_pu, requested_do = _zone(rng), _zone(rng)
    # Фактичні зони: у ~5% поїздок пасажир зсунув піну або змінив пункт призначення.
    actual_pu = _zone(rng) if rng.random() < ZONE_CHANGE_RATE else requested_pu
    actual_do = _zone(rng) if rng.random() < ZONE_CHANGE_RATE else requested_do
    vehicle = rng.choice(VEHICLE_TYPES)
    surge_estimate = rng.choice(SURGE_CHOICES)
    distance_km = _money(rng.uniform(0.8, 24.0))

    events: list[tuple[float, dict]] = []

    def add(offset: float, event_type: str, source: str, payload: dict) -> None:
        events.append(
            (
                offset,
                {
                    "event_type": event_type,
                    "ride_id": ride_id,
                    "source": source,
                    "payload": payload,
                },
            )
        )

    add(
        0.0,
        "ride_requested",
        src_rider,
        {
            "rider": {
                "id": rider_id,
                "platform": rng.choice(PLATFORMS),
                "app_version": rng.choice(APP_VERSIONS),
            },
            "pickup": {"zone_id": requested_pu},
            "dropoff": {"zone_id": requested_do},
            "requested_vehicle": vehicle,
            # Оцінка множника попиту на момент замовлення. НЕ фактична —
            # фактична приїде у ride_completed. Плутати їх не можна.
            "surge_estimate": surge_estimate,
        },
    )

    roll = rng.random()
    # 8% скасовано до підтвердження, 7% — після, решта доїхала.
    if roll < 0.08:
        add(
            rng.uniform(20, 120),
            "ride_cancelled",
            src_rider,
            {
                "cancelled_by": "rider",
                "reason": rng.choice(CANCEL_REASONS),
                "stage": "requested",
            },
        )
        return _finalize(rng, events, t_start)

    driver = rng.choice(drivers_by_type[vehicle])
    t_accept = rng.uniform(4, 45)
    rating = round(min(5.0, max(1.0, driver.base_rating + rng.uniform(-0.25, 0.25))), 2)
    add(
        t_accept,
        "ride_accepted",
        src_driver,
        {
            "driver": {
                "id": driver.driver_id,
                "rating": rating,
                "vehicle": {"type": driver.vehicle_type, "medallion": driver.medallion},
            },
            "eta_seconds": rng.randint(60, 900),
        },
    )

    if roll < 0.15:
        add(
            t_accept + rng.uniform(10, 240),
            "ride_cancelled",
            src_driver,
            {
                "cancelled_by": rng.choice(["rider", "driver"]),
                "reason": rng.choice(CANCEL_REASONS),
                "stage": "accepted",
            },
        )
        return _finalize(rng, events, t_start)

    t_start_ride = t_accept + rng.uniform(60, 600)
    add(
        t_start_ride,
        "ride_started",
        src_driver,
        {
            "driver": {"id": driver.driver_id},
            "pickup": {"zone_id": actual_pu},
            "odometer_km": round(rng.uniform(1000, 90000), 1),
        },
    )

    surge_actual = rng.choice(SURGE_CHOICES)
    tolls = TOLL_AMOUNT if rng.random() < 0.05 else 0.0
    method = rng.choice(PAYMENT_METHODS)
    fare_amount = _money(FARE_BASE + FARE_PER_KM * distance_km)
    # Готівкові поїздки приїжджають БЕЗ чайових: у касі їх не видно (NULL, не 0).
    tip = None if method == "cash" else _money(fare_amount * surge_actual * rng.uniform(0, 0.25))
    total = _money(fare_amount * surge_actual + tolls + (tip or 0.0))

    t_complete = t_start_ride + rng.uniform(180, 2400)
    add(
        t_complete,
        "ride_completed",
        src_driver,
        {
            "driver": {"id": driver.driver_id},
            "dropoff": {"zone_id": actual_do},
            "distance_km": distance_km,
            "fare": {
                "amount": fare_amount,
                "surge_multiplier": surge_actual,
                "tolls": tolls,
                "tip": tip,
                "total": total,
                "currency": "USD",
            },
        },
    )

    add(
        t_complete + rng.uniform(1, 90),
        "payment_captured",
        src_billing,
        {
            "payment": {
                "method": method,
                "amount": total,
                "currency": "USD",
                "psp_reference": f"psp_{rng.randrange(16**12):012x}",
            },
        },
    )
    return _finalize(rng, events, t_start)


def _finalize(
    rng: random.Random, events: list[tuple[float, dict]], t_start: float
) -> list[PlannedEvent]:
    """Зсуває події поїздки на її початок і накидає late events та дублікати."""
    planned: list[PlannedEvent] = []
    for offset, event in events:
        event_offset = t_start + offset
        emit_offset = event_offset
        if rng.random() < LATE_RATE:
            # Застосунок був офлайн: подія публікується через 5-40 хвилин.
            emit_offset += rng.uniform(300, 2400)
        elif rng.random() < OUT_OF_ORDER_RATE:
            # Дрібний зсув публікації ламає порядок у межах поїздки.
            emit_offset += rng.uniform(30, 200)
        planned.append(PlannedEvent(event_offset, emit_offset, event))
        if rng.random() < DUPLICATE_RATE:
            # At-least-once: той самий event_id публікується вдруге.
            planned.append(
                PlannedEvent(event_offset, emit_offset + rng.uniform(0.5, 30), dict(event))
            )
    return planned


def plan_corpus(seed: int, n_rides: int, ride_gap_s: float = 6.0) -> list[PlannedEvent]:
    """Повний корпус подій, відсортований у порядку публікації.

    `event_id` — детермінований хеш (ride_id, тип, час події): у дубліката він той самий,
    що й в оригіналу (дублікат — це та сама подія, а не нова).
    """
    rng = random.Random(seed)
    drivers = build_drivers(rng)
    drivers_by_type: dict[str, list[Driver]] = {}
    for driver in drivers:
        drivers_by_type.setdefault(driver.vehicle_type, []).append(driver)
    # Гарантуємо, що для кожного типу авто знайдеться хоча б один водій.
    for vehicle_type in ("standard", "xl", "lux"):
        drivers_by_type.setdefault(vehicle_type, [drivers[0]])

    planned: list[PlannedEvent] = []
    t = 0.0
    for ride_no in range(1, n_rides + 1):
        t += rng.uniform(0.2 * ride_gap_s, 1.8 * ride_gap_s)
        planned.extend(_ride_plan(rng, ride_no, t, drivers_by_type))

    planned.sort(
        key=lambda p: (p.emit_offset_s, p.event_offset_s, p.event["ride_id"], p.event["event_type"])
    )

    for p in planned:
        # blake2b, а не вбудований hash(): той рандомізований між процесами
        # (PYTHONHASHSEED), і корпус перестав би бути відтворюваним.
        key = f"{p.event['ride_id']}|{p.event['event_type']}|{p.event_offset_s:.3f}"
        p.event["event_id"] = "ev-" + hashlib.blake2b(key.encode(), digest_size=6).hexdigest()
    return planned


def corpus_span_seconds(planned: list[PlannedEvent]) -> float:
    """Скільки секунд event time укладається в корпус."""
    return max(p.event_offset_s for p in planned) if planned else 0.0
