"""面向测试的 MQTT 客户端封装。

直接用 paho-mqtt 写测试会踩四个坑，全部在这里统一解决：

1. ``connect()`` 只负责发出 CONNECT 报文，**不等待 CONNACK**。
   不等就调用 ``publish()`` / ``subscribe()`` 会直接报
   "The client is not currently connected"。
2. ``subscribe()`` 同样是异步的，SUBACK 回来之前发布的消息会被丢掉，
   表现为"刚订阅完就发消息，结果一条收不到"。
3. 消息只能通过回调拿到，测试线程需要线程安全的收集与等待原语，
   否则会出现竞态或忙等。
4. ``keepalive`` 的默认值对小众 broker 偏紧。amqtt 在高频 PINGREQ 下
   PINGRESP 响应不及时，客户端会被判定 keepalive 超时并断开——实测
   把 keepalive 设为 60 秒即可规避。
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from dataclasses import dataclass
from typing import Iterable
from uuid import uuid4

import paho.mqtt.client as mqtt

from framework.broker import BrokerEndpoint

logger = logging.getLogger(__name__)

DEFAULT_KEEPALIVE = 60
DEFAULT_TIMEOUT = 10.0


@dataclass(frozen=True)
class ReceivedMessage:
    """一条已到达订阅端的消息快照。"""

    topic: str
    payload: bytes
    qos: int
    retain: bool
    mid: int
    received_at: float

    @property
    def text(self) -> str:
        return self.payload.decode("utf-8", errors="replace")


class MessageCollector:
    """线程安全的消息收集器。

    paho 的 ``on_message`` 回调运行在客户端的网络线程上，测试线程只读取。
    用条件变量而不是简单 list，才能既避免忙等，又能准确判断"消息流是否
    已经安静下来"——后者是断言"没有多余消息"的正确姿势，固定 sleep 不行。
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._messages: list[ReceivedMessage] = []

    # ------------------------------------------------------------ 写入（回调线程）

    def add(self, message: ReceivedMessage) -> None:
        with self._cond:
            self._messages.append(message)
            self._cond.notify_all()

    # ------------------------------------------------------------ 读取（测试线程）

    def snapshot(self) -> list[ReceivedMessage]:
        with self._cond:
            return list(self._messages)

    def count(self) -> int:
        with self._cond:
            return len(self._messages)

    def clear(self) -> None:
        with self._cond:
            self._messages.clear()

    @property
    def payloads(self) -> list[bytes]:
        return [m.payload for m in self.snapshot()]

    def wait_for_count(self, expected: int, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """等到至少 ``expected`` 条消息到达。"""
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self._messages) < expected:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return True

    def wait_quiet(self, quiet_period: float = 0.5, timeout: float = DEFAULT_TIMEOUT) -> None:
        """等到消息流安静下来（连续 ``quiet_period`` 秒没有新消息）。

        断言"就这些了，没有多余的"必须用这个方法，而不是固定 sleep——
        固定 sleep 在慢机器上会误判，在快机器上白等。
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                before = len(self._messages)
                self._cond.wait(min(quiet_period, remaining))
                if len(self._messages) == before:
                    return


class MqttTestClient:
    """测试用 MQTT 客户端。

    典型用法::

        with MqttTestClient(broker.endpoint) as sub, \\
             MqttTestClient(broker.endpoint) as pub:
            sub.subscribe("a/b", qos=1)
            pub.publish("a/b", b"hi", qos=1)
            assert sub.messages.wait_for_count(1)
    """

    def __init__(
        self,
        endpoint: BrokerEndpoint,
        client_id: str | None = None,
        keepalive: int = DEFAULT_KEEPALIVE,
        clean_session: bool = True,
        protocol: int | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.client_id = client_id or f"test-{uuid4().hex[:10]}"
        self.keepalive = keepalive
        self.clean_session = clean_session
        self.messages = MessageCollector()

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            clean_session=clean_session,
            protocol=protocol or mqtt.MQTTv311,
        )
        if username is not None:
            self._client.username_pw_set(username, password)

        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect
        self._disconnected = threading.Event()

        self._sub_cond = threading.Condition()
        self._acked_mids: set[int] = set()
        self._subscribed: dict[str, int] = {}

    # ------------------------------------------------------------ 连接与断开

    def connect(self, timeout: float = DEFAULT_TIMEOUT) -> "MqttTestClient":
        self._disconnected.clear()
        rc = self._client.connect(self.endpoint.host, self.endpoint.port, self.keepalive)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError(
                f"连接到 {self.endpoint.url} 失败，rc={rc}（{mqtt.error_string(rc)}）"
            )

        self._client.loop_start()

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._client.is_connected():
                logger.debug("客户端已连接 [%s] %s", self.client_id, self.endpoint.url)
                return self
            time.sleep(0.02)

        self._client.loop_stop()
        raise TimeoutError(
            f"等待 CONNACK 超时（{timeout}s）：{self.endpoint.url} client_id={self.client_id}"
        )

    def disconnect(self, timeout: float = 5.0) -> None:
        """正常断开（发送 DISCONNECT），不会触发遗嘱消息。"""
        try:
            if self._client.is_connected():
                self._client.disconnect()
                self._disconnected.wait(timeout)
        finally:
            self._client.loop_stop()

    def kill(self) -> None:
        """粗暴掐断 TCP 连接，用于模拟设备异常掉线（触发遗嘱消息）。

        与 ``disconnect()`` 的区别：不发 DISCONNECT 报文，broker 只能通过
        连接中断察觉，因此会按约定发布遗嘱消息。
        """
        sock = self._client.socket()
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        self._client.loop_stop()

    # ------------------------------------------------------------ 订阅与发布

    def subscribe(
        self, topic: str, qos: int = 0, timeout: float = DEFAULT_TIMEOUT
    ) -> "MqttTestClient":
        """订阅并**等待 SUBACK** 后才返回。

        不等 SUBACK 就发布是新手最常见的坑：SUBSCRIBE 与 PUBLISH 是两个
        独立的报文流程，broker 收到 SUBSCRIBE 之前的消息不会回投。
        """
        rc, mid = self._client.subscribe(topic, qos=qos)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"subscribe({topic!r}) 提交失败：rc={rc}")
        self._await_suback(mid, topic, timeout)
        self._subscribed[topic] = qos
        return self

    def unsubscribe(self, topic: str, timeout: float = DEFAULT_TIMEOUT) -> "MqttTestClient":
        rc, mid = self._client.unsubscribe(topic)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"unsubscribe({topic!r}) 提交失败：rc={rc}")
        self._await_suback(mid, topic, timeout)
        self._subscribed.pop(topic, None)
        return self

    def publish(
        self,
        topic: str,
        payload: bytes | str = b"",
        qos: int = 0,
        retain: bool = False,
        wait: bool = True,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> mqtt.MQTTMessageInfo:
        """发布一条消息。

        ``wait=True`` 时等到 broker 完成对应 QoS 的确认流程（QoS 1 等 PUBACK、
        QoS 2 等 PUBCOMP）才返回。测试里应当保持默认值，否则会引入时序竞态。
        """
        if isinstance(payload, str):
            payload = payload.encode("utf-8")

        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"publish({topic!r}) 失败：rc={info.rc}")

        if wait:
            info.wait_for_publish(timeout=timeout)
        return info

    @property
    def is_connected(self) -> bool:
        return bool(self._client.is_connected())

    @property
    def subscriptions(self) -> dict[str, int]:
        return dict(self._subscribed)

    # ---------------------------------------------------------------- 内部回调

    def _on_message(self, client, userdata, msg) -> None:  # noqa: ANN001
        self.messages.add(
            ReceivedMessage(
                topic=msg.topic,
                payload=bytes(msg.payload or b""),
                qos=msg.qos,
                retain=bool(msg.retain),
                mid=msg.mid,
                received_at=time.monotonic(),
            )
        )

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:  # noqa: ANN001, E501
        # 必须注册这个回调：disconnect() 靠它唤醒，否则每次断开都会空等满超时。
        self._disconnected.set()

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> None:  # noqa: ANN001, E501
        with self._sub_cond:
            self._acked_mids.add(mid)
            self._sub_cond.notify_all()

    def _await_suback(self, mid: int, topic: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        with self._sub_cond:
            while mid not in self._acked_mids:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"等待 SUBACK 超时（{timeout}s）：topic={topic!r} mid={mid}"
                    )
                self._sub_cond.wait(remaining)

    # ------------------------------------------------------------ 上下文管理

    def __enter__(self) -> "MqttTestClient":
        return self.connect()

    def __exit__(self, *exc_info) -> None:
        self.disconnect()

    def __repr__(self) -> str:
        return f"<MqttTestClient {self.client_id} -> {self.endpoint.url}>"


def collect_payloads(messages: Iterable[ReceivedMessage]) -> list[bytes]:
    return [m.payload for m in messages]
