"""Methods pertaining to weather data"""
from enum import IntEnum
from json import dumps as json_dumps, load as json_load
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)

REST_PROXY_URL = "http://localhost:8082"
class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        super().__init__(
            "weather",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=3,
            num_replicas=1
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json_load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json_load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        headers = {
            "Content-Type": "application/vnd.kafka.json.v2+json"
        }
        data = {     
            "key_schema": json_dumps(Weather.key_schema),
            "value_schema": json_dumps(Weather.value_schema),
            "records": [{
                "value": {
                    "temperature": int(self.temp),
                    "status": self.status.name
                },
                "key": {
                    "timestamp": self.time_millis()
                }
            }]
        }
        resp = requests.post(
            f"{REST_PROXY_URL}/topics/weather", 
            data=json_dumps(data), 
            headers=headers
        )
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
