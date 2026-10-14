"""MQTT QoS 投递语义测试。

MQTT 3.1.1 定义三档服务质量：

  QoS 0  至多一次（at most once）—— 发出去就不管，可能丢
  QoS 1  至少一次（at least once）—— 保证到达，但可能重复
  QoS 2  恰好一次（exactly once）—— 不丢不重，代价是四步握手

以及一条最容易被误解的规则（规范 4.3.5）：

  投递给订阅者的 QoS = min(发布 QoS, 订阅 QoS)

也就是说，发布端用 QoS 2 并不能"强行"让订阅端收到 QoS 2——订阅端的降级
会把整条链路拉低，发布端的高 QoS 是白花的。
"""

from __future__ import annotations

import pytest

from framework.assertions import (
    assert_delivery_qos,
    assert_exactly,
    assert_no_duplicate_payloads,
    assert_payloads,
)

pytestmark = [pytest.mark.qos]


@pytest.mark.smoke
@pytest.mark.parametrize("qos", [0, 1, 2], ids=["qos0", "qos1", "qos2"])
def test_all_messages_delivered_at_each_qos(publisher, subscriber, topic, qos):
    """三档 QoS 下，发布的消息都应完整到达订阅端。

    这是链路层最基础的保证。它失败说明框架或 broker 的连接/订阅环节有问题，
    后面更细的语义用例都不必看。

    关于 QoS 0：协议上不保证投递，本断言成立的前提是"本地 broker、无丢包"。
    这条用例因此兼作环境健壮性探针——它一旦变得 flaky，大概率是环境出了问题，
    而不是被测对象有 bug。
    """
    subject = f"{topic}/delivery"
    subscriber.subscribe(subject, qos=qos)

    payloads = [f"msg-{i:03d}".encode() for i in range(10)]
    for payload in payloads:
        publisher.publish(subject, payload, qos=qos)

    messages = assert_exactly(subscriber.messages, len(payloads), timeout=15)
    assert_payloads(messages, payloads)


def test_qos2_delivers_exactly_once(publisher, subscriber, topic):
    """QoS 2 的核心承诺：不丢、不重。

    QoS 2 通过 PUBLISH → PUBREC → PUBREL → PUBCOMP 四步握手保证消息恰好
    投递一次。broker 若在去重或状态机上存在缺陷，这里就会出现重复投递——
    那是一个真实的、值得上报的 bug。
    """
    subject = f"{topic}/exactly-once"
    subscriber.subscribe(subject, qos=2)

    count = 30
    payloads = [f"seq-{i:03d}".encode() for i in range(count)]
    for payload in payloads:
        publisher.publish(subject, payload, qos=2)

    messages = assert_exactly(subscriber.messages, count, timeout=20)
    assert_payloads(messages, payloads)
    assert_no_duplicate_payloads(messages)


@pytest.mark.parametrize(
    ("publish_qos", "subscribe_qos", "expected_qos"),
    [
        (0, 0, 0),
        (1, 0, 0),
        (2, 0, 0),
        (1, 1, 1),
        (2, 1, 1),
        (2, 2, 2),
    ],
    ids=[
        "pub0-sub0",
        "pub1-sub0",
        "pub2-sub0",
        "pub1-sub1",
        "pub2-sub1",
        "pub2-sub2",
    ],
)
def test_delivery_qos_is_min_of_publish_and_subscribe(
    publisher, subscriber, topic, publish_qos, subscribe_qos, expected_qos
):
    """投递 QoS = min(发布 QoS, 订阅 QoS)（MQTT 3.1.1 规范 4.3.5）。

    这六种组合覆盖完整的降级矩阵。其中 pub2-sub0 与 pub2-sub1 两行说明：
    发布端提高 QoS 并不能提升订阅端实际收到的服务质量。
    """
    subject = f"{topic}/min-rule"
    subscriber.subscribe(subject, qos=subscribe_qos)
    publisher.publish(subject, b"probe", qos=publish_qos)

    messages = assert_exactly(subscriber.messages, 1, timeout=10)
    assert_delivery_qos(messages, expected_qos)


def test_payload_integrity_for_tricky_payloads(publisher, subscriber, topic):
    """报文载荷必须原样透传，包括各类边界数据。

    真实设备会上报二进制帧、含 0x00 的结构体、UTF-8 文本、空载荷。
    broker 若做了不当的编码转换或截断，设备侧解析就会出错——这类问题
    在只测 ASCII 字符串时完全发现不了。
    """
    subject = f"{topic}/payload"
    subscriber.subscribe(subject, qos=1)

    payloads = [
        b"",
        b"\x00\x01\x02\xff\xfe",
        "温度=23.5C 湿度=61%".encode("utf-8"),
        bytes(range(256)),
        b"A" * 4096,
    ]
    for payload in payloads:
        publisher.publish(subject, payload, qos=1)

    messages = assert_exactly(subscriber.messages, len(payloads), timeout=15)
    assert_payloads(messages, payloads)


def test_qos1_at_least_once_under_sustained_load(publisher, subscriber, topic):
    """持续负载下 QoS 1 不丢消息。

    与 QoS 2 用例的区别：这里关注"量大时不丢"，不关注"不重复"。QoS 1 协议上
    允许重复，因此只断言集合相等、不断言序列相等。本地无丢包环境下不应触发
    重传，所以"恰好 100 条"依然成立。
    """
    subject = f"{topic}/sustained"
    subscriber.subscribe(subject, qos=1)

    count = 100
    payloads = [f"load-{i:04d}".encode() for i in range(count)]
    for payload in payloads:
        publisher.publish(subject, payload, qos=1)

    messages = assert_exactly(subscriber.messages, count, timeout=30)
    assert_payloads(messages, payloads, ordered=False)
