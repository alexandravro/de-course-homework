"""GHArchiveSensor — ВАШ custom sensor. Специфікація: ../../SPEC.md → «Sensor».

Сенсор чекає, поки годинний файл GitHub Archive за logical date стане доступним,
і лише тоді пропускає DAG далі.

Підказки:
  * успадкуйте `airflow.sensors.base.BaseSensorOperator`;
  * у __init__ прийміть параметр `hour` (година доби, яку перевіряємо);
  * реалізуйте `poke(self, context) -> bool`: візьміть дату з context["ds"],
    зберіть URL https://data.gharchive.org/<ds>-<hour>.json.gz і зробіть HTTP HEAD —
    поверніть True на 200, інакше False (або при винятку);
  * у DAG додайте сенсор першою задачею з timeout=600, poke_interval=60,
    mode="reschedule".
"""

from __future__ import annotations
import requests
from airflow.sensors.base import BaseSensorOperator


class GHArchiveSensor(BaseSensorOperator):
    def __init__(self, hour: int = 14, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        ds = context["ds"]
        url = f"https://data.gharchive.org/{ds}-{self.hour:02d}.json.gz"
        try:
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False

