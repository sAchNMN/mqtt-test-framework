"""断言工具。

把"等消息 / 查消息 / 验语义"这类重复逻辑集中在这里，让测试用例本身
只表达业务意图。所有断言失败都带可诊断的上下文（期望值、实际值、
收到的原始消息），避免出现"assert False"这种查不出原因的失败。
"""

from __future__ import annotations

from collections import Counter

from framework.client import MessageCollector, ReceivedMessage

DEFAULT_TIMEOUT = 10.0
DEFAULT_QUIET_PERIOD = 0.5


def await_messages(
    collector: MessageCollector,
    count: int,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[ReceivedMessage]:
    """等到至少 ``count`` 条消息到达，返回当前全部消息。"""
    if not collector.wait_for_count(count, timeout):
        raise AssertionError(
            f"期望至少收到 {count} 条消息，实际只收到 {collector.count()} 条"
            f"（超时 {timeout}s）。已收到：{[m.text for m in collector.snapshot()]}"
        )
    return collector.snapshot()


def await_stable(
    collector: MessageCollector,
    quiet_period: float = DEFAULT_QUIET_PERIOD,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[ReceivedMessage]:
    """等到消息流安静下来再返回全部消息。

    断言"没有多余消息"必须走这里——先等到没有新消息到达，再数总数。
    """
    collector.wait_quiet(quiet_period, timeout)
    return collector.snapshot()


def assert_exactly(
    collector: MessageCollector,
    count: int,
    timeout: float = DEFAULT_TIMEOUT,
    quiet_period: float = DEFAULT_QUIET_PERIOD,
) -> list[ReceivedMessage]:
    """断言收到的消息**恰好**是 ``count`` 条。

    分两步：先等到至少 ``count`` 条（此时若失败能立刻给出已收到的消息），
    再等消息流安静下来后核对总数（捕捉"多收"的情况）。
    """
    if count > 0:
        await_messages(collector, count, timeout)

    messages = await_stable(collector, quiet_period, timeout)
    actual = len(messages)
    assert actual == count, (
        f"期望恰好 {count} 条消息，实际 {actual} 条。"
        f"已收到：{[m.text for m in messages]}"
    )
    return messages


def assert_payloads(
    messages: list[ReceivedMessage],
    expected: list[bytes],
    ordered: bool = True,
) -> None:
    """断言消息载荷与期望一致。"""
    actual = [m.payload for m in messages]
    if not ordered:
        assert Counter(actual) == Counter(expected), (
            "载荷集合不匹配。\n"
            f"期望：{expected!r}\n"
            f"实际：{actual!r}"
        )
        return

    assert actual == expected, (
        "载荷序列不匹配。\n"
        f"期望（{len(expected)} 条）：{expected!r}\n"
        f"实际（{len(actual)} 条）：{actual!r}"
    )


def assert_no_duplicate_payloads(messages: list[ReceivedMessage]) -> None:
    """断言没有重复投递（QoS 2 的核心承诺）。"""
    counter = Counter(m.payload for m in messages)
    duplicated = {payload: n for payload, n in counter.items() if n > 1}
    assert not duplicated, (
        f"检测到重复投递：{ {p.decode('utf-8', 'replace'): n for p, n in duplicated.items()} }"
    )


def assert_delivery_qos(messages: list[ReceivedMessage], expected_qos: int) -> None:
    """断言实际投递使用的 QoS。

    MQTT 3.1.1 规范 4.3.5：投递 QoS = min(发布 QoS, 订阅 QoS)。
    """
    actual = sorted({m.qos for m in messages})
    assert actual == [expected_qos], (
        f"期望投递 QoS 为 {expected_qos}，实际出现 {actual}。"
        f"提示：投递 QoS 由 min(发布 QoS, 订阅 QoS) 决定。"
    )


def assert_topic_suffixes(messages: list[ReceivedMessage], expected: list[str]) -> None:
    """断言收到消息的 topic 列表。"""
    actual = [m.topic for m in messages]
    assert actual == expected, f"topic 不匹配。\n期望：{expected!r}\n实际：{actual!r}"
