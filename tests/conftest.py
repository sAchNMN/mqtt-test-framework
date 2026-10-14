"""pytest 全局 fixture。

broker 后端的选择顺序（按优先级）：

1. 设置了 ``MQTT_HOST`` → 连接该外部 broker（可以是远程实例）
2. ``MQTT_BACKEND=docker`` → 连接 docker compose 起的 Mosquitto
3. 其他情况 → 内嵌 amqtt broker（默认，零外部依赖）

这样同一套用例既能在本地无 Docker 环境跑，也能在 CI 里换成真实
生产级 broker（Mosquitto / EMQX）做横向对比。
"""

from __future__ import annotations

import os
from uuid import uuid4

import pytest

from framework.broker import (
    DEFAULT_EMBEDDED_PORT,
    BrokerEndpoint,
    EmbeddedBroker,
    ExternalBroker,
)
from framework.client import MqttTestClient


def _build_backend() -> EmbeddedBroker | ExternalBroker:
    external_host = os.getenv("MQTT_HOST")
    if external_host:
        return ExternalBroker(
            host=external_host,
            port=int(os.getenv("MQTT_PORT", "1883")),
            name=os.getenv("MQTT_BROKER_NAME", "external"),
        )

    backend = os.getenv("MQTT_BACKEND", "embedded").strip().lower()
    if backend == "embedded":
        return EmbeddedBroker(
            port=int(os.getenv("MQTT_EMBEDDED_PORT", str(DEFAULT_EMBEDDED_PORT)))
        )
    if backend == "docker":
        return ExternalBroker(
            host="127.0.0.1",
            port=int(os.getenv("MQTT_PORT", "1883")),
            name="mosquitto (docker)",
        )
    raise ValueError(
        f"未知的 MQTT_BACKEND={backend!r}，可选值：embedded / docker"
    )


def pytest_report_header(config) -> str:  # noqa: ARG001
    return (
        f"MQTT 测试后端: {os.getenv('MQTT_BACKEND', 'embedded')}"
        f" | MQTT_HOST={os.getenv('MQTT_HOST', '(未设置)')}"
    )


@pytest.fixture(scope="session")
def broker():
    """被测 broker（整个测试会话共用）。"""
    backend = _build_backend()
    backend.start()
    yield backend
    backend.stop()


@pytest.fixture(scope="session")
def broker_endpoint(broker) -> BrokerEndpoint:
    return broker.endpoint


@pytest.fixture
def topic() -> str:
    """每个用例独占的 topic 前缀，避免用例之间互相干扰。"""
    return f"pytest/{uuid4().hex[:12]}"


@pytest.fixture
def client_factory(broker_endpoint):
    """按需创建客户端的工厂，用例结束后统一断开。"""
    created: list[MqttTestClient] = []

    def _make(**kwargs) -> MqttTestClient:
        client = MqttTestClient(broker_endpoint, **kwargs).connect()
        created.append(client)
        return client

    yield _make

    for client in created:
        try:
            client.disconnect()
        except Exception:  # noqa: BLE001 - 清理阶段不应影响测试结论
            pass


@pytest.fixture
def subscriber(client_factory) -> MqttTestClient:
    return client_factory(client_id=f"sub-{uuid4().hex[:8]}")


@pytest.fixture
def publisher(client_factory) -> MqttTestClient:
    return client_factory(client_id=f"pub-{uuid4().hex[:8]}")
