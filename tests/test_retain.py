"""保留消息（Retained Message）测试 —— 骨架，待实现。

broker 为每个 topic 保存最后一条 ``retain=True`` 的消息。新订阅者订阅到该
topic 时会**立刻**收到这条保留消息，不必等下一次发布。

这是设备状态上报的标准做法：设备上线时上报一次状态，此后任何时候接入的
观察者都能马上看到"当前状态"，而不是干等到下次变化。

待实现用例清单：

  1. 发布 retain=True 后，新订阅者应立即收到该消息
  2. 发布 retain=True 后，原有订阅者不应重复收到（不是广播加成）
  3. retain=False 的消息不产生保留效果
  4. 空载荷 + retain=True 应清除保留消息（规范行为）
  5. 保留消息的投递 QoS 应遵循 min(发布 QoS, 订阅 QoS)
  6. 通配符订阅应收到匹配到的多个 topic 的保留消息
  7. 新订阅者收到的保留消息，retain 标志应为 True
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.retain]


@pytest.mark.skip(reason="骨架占位：实现清单见模块 docstring")
def test_retained_message_delivered_to_new_subscriber():
    """保留消息应在新的订阅者接入时立即投递。"""
    raise NotImplementedError


@pytest.mark.skip(reason="骨架占位：实现清单见模块 docstring")
def test_empty_payload_clears_retained_message():
    """空载荷 + retain=True 应清除该 topic 上的保留消息。"""
    raise NotImplementedError
