"""Розкладає корпус подій на батчі у форматі landing-зони. ДАНО. Не редагуйте.

    uv run python -m scripts.make_fixtures        # -> data/fixtures/batch_1 .. batch_4

Кожен батч — це те, що consumer записав би за один проміжок часу: файли
`dt=…/hour=…/part-p{partition}-o{first}-o{last}.ndjson`. Корпус ділиться за ПОРЯДКОМ
ПУБЛІКАЦІЇ, тому late events і дублікати опиняються в пізніших батчах, ніж решта подій їхньої
поїздки — саме на цьому перевіряється інкрементальність.

Партиція = хеш ride_id (як ключ у Kafka: одна поїздка — одна партиція), офсети
продовжуються від батча до батча. Вміст подій ідентичний тому, що шле producer.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from stream.events import plan_corpus

SEED = 42
TOTAL_RIDES = 1200
N_BATCHES = 4
N_PARTITIONS = 3
ANCHOR = datetime(2024, 1, 15, 8, 0, tzinfo=UTC)
OUT = Path("data/fixtures")


def partition_of(ride_id: str) -> int:
    digest = hashlib.blake2b(ride_id.encode(), digest_size=4).digest()
    return int.from_bytes(digest, "big") % N_PARTITIONS


def main() -> None:
    planned = plan_corpus(SEED, TOTAL_RIDES)
    chunk = -(-len(planned) // N_BATCHES)  # ceil
    shutil.rmtree(OUT, ignore_errors=True)
    next_offset: dict[int, int] = defaultdict(int)

    for batch in range(N_BATCHES):
        per_partition: dict[int, list[tuple[int, dict]]] = defaultdict(list)
        for item in planned[batch * chunk : (batch + 1) * chunk]:
            event = dict(item.event)
            occurred_at = ANCHOR + timedelta(seconds=item.event_offset_s)
            event["occurred_at"] = occurred_at.isoformat().replace("+00:00", "Z")
            part = partition_of(event["ride_id"])
            per_partition[part].append((next_offset[part], event))
            next_offset[part] += 1

        for part, rows in sorted(per_partition.items()):
            first, last = rows[0][0], rows[-1][0]
            path = (
                OUT
                / f"batch_{batch + 1}"
                / "dt=2024-01-15"
                / f"hour={9 + batch:02d}"
                / f"part-p{part}-o{first:012d}-o{last:012d}.ndjson"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w") as fh:
                for _, event in rows:
                    fh.write(json.dumps(event, separators=(",", ":")) + "\n")
        n_events = sum(len(v) for v in per_partition.values())
        print(f"batch_{batch + 1}: {n_events} подій, {len(per_partition)} файли")


if __name__ == "__main__":
    main()
