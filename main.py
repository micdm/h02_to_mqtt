import logging
import os
import socketserver
from collections.abc import Generator
from datetime import datetime, UTC
from functools import cache
from socket import socket
from typing import Any, Literal

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


@cache
def setup_mqtt_client() -> Client:
    client = Client(
        CallbackAPIVersion.VERSION2,
        protocol=MQTTv5,
    )
    client.on_connect = lambda *args: logger.info("Connected to MQTT")
    client.on_connect_fail = lambda *args: logger.info("Cannot connect to MQTT")
    return client


class TCPHandler(socketserver.BaseRequestHandler):
    def setup(self) -> None:
        logger.info(
            "New connection: connection=%s, address=%s", self, self.client_address
        )

    def finish(self) -> None:
        logger.info("Connection closed: connection=%s", self)

    def handle(self) -> None:
        try:
            for payload in handle_request(self.request):
                process_payload(payload)
        except BadRequest as e:
            logger.info("Bad request: e=%s", e)


class BadRequest(Exception):
    pass


def handle_request(request: socket) -> Generator[str, None, None]:
    buffer = ""
    while chunk := request.recv(4096):
        try:
            buffer += chunk.decode()
        except UnicodeDecodeError as e:
            raise BadRequest("cannot read chunk") from e
        for message, end in process_buffer(buffer):
            yield message
            buffer = buffer[end + 1 :]


def process_buffer(buffer: str) -> Generator[tuple[str, int], None, None]:
    cursor = 0
    while cursor < len(buffer):
        if buffer[cursor] != "*":
            raise BadRequest(f"unexpected data: buffer={buffer[cursor : cursor + 20]}")
        end = buffer.find("#", cursor)
        if end == -1:
            return
        yield buffer[cursor + 1 : end], end
        cursor = end + 1


class H02Payload(BaseModel):
    device_id: str
    latitude: float
    longitude: float
    velocity: float
    timestamp: datetime

    @model_validator(mode="before")
    @classmethod
    def split_input(cls, data: str) -> dict[str, Any]:
        def fix_coord(value: str) -> float:
            return round(int(value[:2]) + float(value[2:]) / 60, 6)

        parts = data.split(",")
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


def process_payload(value: str) -> None:
    h02_payload = H02Payload.model_validate(value)
    logger.info("H02 message received: payload=%s", h02_payload)
    mqtt_payload = MQTTPayload(
        lat=h02_payload.latitude,
        lon=h02_payload.longitude,
        vel=h02_payload.velocity,
        tst=h02_payload.timestamp,
        created_at=datetime.now(UTC),
    )
    mqtt_client.publish("owntracks/car/gps", mqtt_payload.model_dump_json())


if __name__ == "__main__":
    with socketserver.TCPServer(("0.0.0.0", 11220), TCPHandler) as server:
        mqtt_client = setup_mqtt_client()
        config: Config = Config.model_validate(os.environ)
        mqtt_client.username_pw_set(config.MQTTUser, config.MQTTPassword)
        mqtt_client.connect(config.MQTTHost, config.MQTTPort)
        mqtt_client.loop_start()
        server.serve_forever()
        mqtt_client.loop_stop()
