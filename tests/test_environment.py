"""环境自检层。

这一层的存在意义：把"环境问题"和"被测对象问题"分开。
这里的用例一旦失败，说明测试环境本身有问题——后面所有失败都不必深究，
先修环境。这是真实测试体系里的常规分层做法。
"""

from __future__ import annotations

import pytest

from framework.broker import wait_port_open
from framework.client import MqttTestClient

pytestmark = [pytest.mark.smoke]


def test_broker_port_is_reachable(broker):
    """broker 端口可连接。"""
    assert wait_port_open(broker.endpoint.host, broker.endpoint.port, timeout=5), (
        f"broker 不可达：{broker.endpoint.url}"
    )


def test_client_can_connect_and_disconnect(broker_endpoint):
    """客户端能完成 CONNECT / CONNACK 握手，并能正常断开。"""
    client = MqttTestClient(broker_endpoint, client_id="env-probe")
    client.connect(timeout=10)
    assert client.is_connected, "connect() 返回后应处于已连接状态"
    client.disconnect()
    assert not client.is_connected, "disconnect() 之后应处于断开状态"


def test_subscribe_returns_suback(subscriber, topic):
    """subscribe() 内部等待 SUBACK，返回即代表订阅已被 broker 接受。"""
    subject = f"{topic}/env-probe"
    subscriber.subscribe(subject, qos=1)
    assert subscriber.subscriptions == {subject: 1}


def test_cleanup_isolated_topics(subscriber, publisher, topic):
    """同一 broker 上的不同 topic 前缀互不干扰（用例隔离的有效性验证）。"""
    from framework.assertions import assert_exactly

    subscribed = f"{topic}/mine"
    others = f"{topic}/others"

    subscriber.subscribe(subscribed, qos=1)
    publisher.publish(others, b"noise", qos=1)
    publisher.publish(subscribed, b"signal", qos=1)

    messages = assert_exactly(subscriber.messages, 1, timeout=10)
    assert messages[0].payload == b"signal"
