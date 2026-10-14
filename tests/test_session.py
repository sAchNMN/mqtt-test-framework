"""会话管理测试 —— 骨架，待实现。

MQTT 的会话（Session）指 broker 为客户端保存的订阅关系与未确认消息。
Clean Session 标志决定重连后能否延续：

  Clean Session = True   每次连接都是全新的，broker 不保存任何状态
  Clean Session = False  持久会话，broker 保存订阅关系与离线期间的消息

这是物联网场景的关键取舍：设备断网重连后，要不要补收离线期间的消息。

待实现用例清单：

  1. Clean Session=True 重连后，原订阅关系应丢失
  2. Clean Session=False 重连后，原订阅关系应保留
  3. Clean Session=False 时，离线期间的 QoS 1/2 消息应被补发
  4. Clean Session=False 时，离线期间的 QoS 0 消息不应被补发（协议不保证）
  5. 同一 client_id 重复连接，旧连接应被踢掉（session takeover）
  6. 客户端显式清除会话后，broker 不应再保留任何状态

实现提示：需要能指定 ``client_id`` 与 ``clean_session`` 手动创建客户端，
使用 ``client_factory(client_id="...", clean_session=False)``。
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.session]


@pytest.mark.skip(reason="骨架占位：实现清单见模块 docstring")
def test_persistent_session_keeps_subscription():
    """Clean Session=False 的客户端重连后应保留原订阅关系。"""
    raise NotImplementedError


@pytest.mark.skip(reason="骨架占位：实现清单见模块 docstring")
def test_offline_messages_queued_for_persistent_session():
    """持久会话的客户端离线期间，QoS 1/2 消息应在重连后补发。"""
    raise NotImplementedError
