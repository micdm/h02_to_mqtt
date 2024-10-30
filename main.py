import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from functools import cache
import logging
from typing import Any, Annotated, Literal

import uvicorn
from fastapi import FastAPI, Depends
from paho.mqtt.client import MQTTv5, Client
from paho.mqtt.enums import CallbackAPIVersion
from pydantic import BaseModel, Field, field_serializer
from pydantic.functional_validators import model_validator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Config(BaseModel):
    MQTTHost: str = Field(alias="MQTT_HOST")
    MQTTPort: int = Field(alias="MQTT_PORT")
    MQTTUser: str = Field(alias="MQTT_USER")
    MQTTPassword: str = Field(alias="MQTT_PASSWORD")


def _on_mqtt_connect(*args) -> None:
    logger.info("Connected to MQTT")


def _on_mqtt_connect_fail(*args) -> None:
    logger.info("Cannot connect to MQTT")


@cache
def setup_mqtt_client() -> Client:
    client = Client(
        CallbackAPIVersion.VERSION2,
        protocol=MQTTv5,
    )
    client.on_connect = _on_mqtt_connect
    client.on_connect_fail = _on_mqtt_connect_fail
    return client


class RequestBody(BaseModel):
    device_id: str
    latitude: float
    longitude: float
    velocity: float
    timestamp: datetime

    @model_validator(mode="before")
    @classmethod
    def split_input(cls, data: bytes) -> dict[str, Any]:
        def fix_coord(value: str) -> float:
            return round(int(value[:2]) + float(value[2:]) / 60, 6)

        parts = data.decode().split(",")
        return {
            "device_id": parts[1],
            "latitude": fix_coord(parts[5]),
            "longitude": fix_coord(parts[7]),
            "velocity": round(float(parts[9]) * 1.852),
            "timestamp": datetime.strptime(f"{parts[11]}{parts[3]}", "%d%m%y%H%M%S"),
        }


class MQTTPayload(BaseModel):
    _type: Literal["location"] = "location"
    lat: float
    lon: float
    acc: Literal[3] = 3
    vel: float
    tst: datetime
    created_at: datetime
    conn: Literal["m"] = "m"
    tid: Literal["CA"] = "CA"
    t: Literal["I"] = "I"

    @field_serializer("tst")
    def serialize_timestamp(self, timestamp: datetime) -> int:
        return int(timestamp.timestamp())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    mqtt_client = setup_mqtt_client()
    config: Config = Config.model_validate(os.environ)
    mqtt_client.username_pw_set(config.MQTTUser, config.MQTTPassword)
    mqtt_client.connect(config.MQTTHost, config.MQTTPort, keepalive=60)
    mqtt_client.loop_start()
    yield
    mqtt_client.loop_stop()


app = FastAPI(lifespan=lifespan)


@app.post("/")
def index(
    body: RequestBody,
    mqtt_client: Annotated[Client, Depends(setup_mqtt_client)],
):
    logger.info("Request received: %s", body)
    payload = MQTTPayload(
        lat=body.latitude,
        lon=body.longitude,
        vel=body.velocity,
        tst=body.timestamp,
        created_at=datetime.now(UTC),
    )
    mqtt_client.publish("owntracks/car/gps", payload.model_dump_json())
    return "OK"


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        ssl_certfile="./cert.pem",
        ssl_keyfile="./key.pem",
    )
