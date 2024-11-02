import logging
import os
import socketserver
from datetime import datetime, UTC
from functools import cache
from socket import socket
from typing import Any, Literal

from paho.mqtt.client import MQTTv5, Client
from paho.mqtt.enums import CallbackAPIVersion
from pydantic import BaseModel, Field, field_serializer
from pydantic.functional_validators import model_validator

logger = logging.getLogger(__name__)


class Config(BaseModel):
    MQTTHost: str = Field(alias="MQTT_HOST")
    MQTTPort: int = Field(alias="MQTT_PORT")
    MQTTUser: str = Field(alias="MQTT_USER")
    MQTTPassword: str = Field(alias="MQTT_PASSWORD")
    log_level: Literal["DEBUG", "INFO"] = Field("INFO", alias="LOG_LEVEL")


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
    def __str__(self) -> str:
        return f"id={id(self)}, client={self.client_address}"

    def setup(self) -> None:
        logger.info("New connection: connection=%s", self)

    def finish(self) -> None:
        logger.info("Connection closed: connection=%s", self)

    def handle(self) -> None:
        result = handle_request(self.request)
        if result:
            process_h02_message(result)


def handle_request(request: socket) -> bytes | None:
    buffer = b""
    while chunk := request.recv(4096):
        logger.debug("Chunk: %s", chunk)
        buffer += chunk
        message = search_for_message(buffer)
        if message:
            return message
        if len(buffer) > 10000:
            logger.info("Buffer overflow")
            break
    return None


def search_for_message(buffer: bytes) -> bytes | None:
    begin = buffer.find(b"*")
    if begin == -1:
        return None
    end = buffer.find(b"#", begin)
    if end == -1:
        return None
    return buffer[begin + 1 : end]  # Без звёздочки и решётки


class H02Message(BaseModel):
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


class MQTTMessage(BaseModel):
    type: str = Field(..., serialization_alias="_type")
    lat: float
    lon: float
    vel: float
    tst: datetime
    created_at: datetime
    tid: str
    acc: Literal[3] = 3
    conn: Literal["m"] = "m"
    t: Literal["I"] = "I"

    @field_serializer("tst", "created_at")
    def serialize_timestamp(self, timestamp: datetime) -> int:
        return int(timestamp.timestamp())


def process_h02_message(raw: bytes) -> None:
    h02_msg = H02Message.model_validate(raw)
    logger.info("H02 message received: msg=%s", h02_msg)
    mqtt_msg = MQTTMessage(
        type="location",
        lat=h02_msg.latitude,
        lon=h02_msg.longitude,
        vel=h02_msg.velocity,
        tst=h02_msg.timestamp,
        created_at=datetime.now(UTC),
        tid=h02_msg.device_id,
    )
    mqtt_client.publish("owntracks/car/gps", mqtt_msg.model_dump_json())
    logger.info("MQTT message published: msg=%s", mqtt_msg)


if __name__ == "__main__":
    with socketserver.TCPServer(("0.0.0.0", 11220), TCPHandler) as server:
        mqtt_client = setup_mqtt_client()
        config: Config = Config.model_validate(os.environ)
        logging.basicConfig(
            level=config.log_level, format="{asctime} | {message}", style="{"
        )
        mqtt_client.username_pw_set(config.MQTTUser, config.MQTTPassword)
        mqtt_client.connect(config.MQTTHost, config.MQTTPort)
        mqtt_client.loop_start()
        server.serve_forever()
        mqtt_client.loop_stop()
