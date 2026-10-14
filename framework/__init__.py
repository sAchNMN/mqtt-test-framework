"""MQTT 协议自动化测试框架。

对外暴露三类核心构件：

- ``EmbeddedBroker`` / ``ExternalBroker``：被测 broker 的生命周期管理
- ``MqttTestClient``：面向测试的 MQTT 客户端（封装就绪等待与消息收集）
- ``assertions``：断言工具集
"""

from framework.broker import (
    BrokerEndpoint,
    EmbeddedBroker,
    ExternalBroker,
    wait_port_open,
)
from framework.client import MessageCollector, MqttTestClient, ReceivedMessage

__all__ = [
    "BrokerEndpoint",
    "EmbeddedBroker",
    "ExternalBroker",
    "wait_port_open",
    "MessageCollector",
    "MqttTestClient",
    "ReceivedMessage",
]

__version__ = "0.1.0"
