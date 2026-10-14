"""遗嘱消息（Last Will and Testament）测试 —— 骨架，待实现。

MQTT 的遗嘱机制：客户端在 CONNECT 时预先声明一条消息，当 broker 检测到
该客户端**异常掉线**（未发送 DISCONNECT 报文）时，代为发布这条消息。

这是物联网设备"失联告警"的协议级实现——设备断开这件事本身就是信号。

待实现用例清单：

  1. 异常掉线（TCP 被掐断）→ 遗嘱消息应被发布
  2. 正常 DISCONNECT → 遗嘱消息不应发布
  3. 遗嘱消息的 topic / payload 应与 CONNECT 时声明的一致
  4. 遗嘱消息的 QoS 与 retain 标志应被尊重
  5. 客户端在遗嘱延迟期内重连成功 → 遗嘱应被取消（MQTT 5）
  6. 遗嘱消息在 broker 重启后的行为（持久化相关）

实现提示：``MqttTestClient.kill()`` 已提供"不发 DISCONNECT 直接掐断 TCP"
的能力，用例 1 可以直接使用。
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.lwt]


@pytest.mark.skip(reason="骨架占位：实现清单见模块 docstring")
def test_will_message_published_on_abnormal_disconnect():
    """异常掉线时 broker 应代为发布遗嘱消息。"""
    raise NotImplementedError


@pytest.mark.skip(reason="骨架占位：实现清单见模块 docstring")
def test_will_message_not_published_on_clean_disconnect():
    """正常 DISCONNECT 时不应发布遗嘱消息。"""
    raise NotImplementedError
