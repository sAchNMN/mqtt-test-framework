"""被测 broker 的生命周期管理。

框架支持两种后端，通过同一个接口使用：

- ``EmbeddedBroker``：在测试进程内用 amqtt 起一个 broker，零外部依赖，
  本地和 CI 都能直接跑。
- ``ExternalBroker``：连接已经在运行的外部 broker（Docker Compose 起的
  Mosquitto / EMQX，或任意远程实例）。用于做"同一套用例、多个 broker
  实现横向对比"。

两者都暴露 ``endpoint``，测试代码不需要关心当前用的是哪一种后端。
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

DEFAULT_EMBEDDED_HOST = "127.0.0.1"
DEFAULT_EMBEDDED_PORT = 11883


@dataclass(frozen=True)
class BrokerEndpoint:
    """broker 的网络地址。"""

    host: str
    port: int

    @property
    def url(self) -> str:
        return f"mqtt://{self.host}:{self.port}"


def wait_port_open(host: str, port: int, timeout: float = 10.0) -> bool:
    """轮询等待端口可连接。

    比"启动后固定 sleep 几秒"可靠：快机器上不用白等，慢机器上不会误判。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.1)
    return False


def _import_amqtt_broker():
    """延迟导入 amqtt：只用外部 broker 的环境不必安装它。"""
    try:
        from amqtt.broker import Broker
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "使用内嵌 broker 需要 amqtt，请先安装：pip install amqtt"
        ) from exc
    return Broker


class EmbeddedBroker:
    """在独立线程 + 独立事件循环里运行 amqtt broker。

    为什么要独立线程：
    amqtt 是 asyncio 实现，而 paho-mqtt 客户端是线程驱动的。如果 broker
    和客户端跑在同一个事件循环 / 同一个线程上，客户端的同步等待会把事件
    循环堵死，broker 就无法处理任何包——表现为"连上了但收不到消息"。
    隔离到独立线程后两边互不干扰。

    暴露同步的 ``start()`` / ``stop()``，方便 pytest fixture 使用。
    """

    name = "amqtt (embedded)"

    def __init__(
        self,
        host: str = DEFAULT_EMBEDDED_HOST,
        port: int = DEFAULT_EMBEDDED_PORT,
    ) -> None:
        self.endpoint = BrokerEndpoint(host, port)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._broker = None
        self._started = False

    # ---------------------------------------------------------------- 生命周期

    def start(self, timeout: float = 15.0) -> "EmbeddedBroker":
        if self._started:
            return self

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop, name="embedded-mqtt-broker", daemon=True
        )
        self._thread.start()

        if not wait_port_open(self.endpoint.host, self.endpoint.port, timeout):
            self.stop()
            raise RuntimeError(
                f"内嵌 broker 未在 {timeout}s 内就绪：{self.endpoint.url}"
            )

        self._started = True
        logger.info("内嵌 broker 已就绪：%s", self.endpoint.url)
        return self

    def stop(self, timeout: float = 10.0) -> None:
        if self._loop is None:
            return

        try:
            if self._broker is not None:
                future = asyncio.run_coroutine_threadsafe(
                    self._broker.shutdown(), self._loop
                )
                try:
                    future.result(timeout=timeout)
                except Exception as exc:  # noqa: BLE001 - 关闭失败不应掩盖测试结果
                    logger.warning("内嵌 broker 关闭时出现异常：%s", exc)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=timeout)
            if not self._loop.is_closed():
                self._loop.close()
            self._loop = None
            self._thread = None
            self._broker = None
            self._started = False
            logger.info("内嵌 broker 已停止")

    # ------------------------------------------------------------------ 内部

    def _run_loop(self) -> None:
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start_broker())
        self._loop.run_forever()

    async def _start_broker(self) -> None:
        broker_cls = _import_amqtt_broker()
        self._broker = broker_cls(self._build_config())
        await self._broker.start()

    def _build_config(self) -> dict:
        """构造 amqtt broker 配置。

        显式声明插件，而不是依赖 amqtt 的 ``default_broker_plugins()``。
        理由：默认值会随版本变化，框架应当明确声明自己的依赖。

        只加载 ``AnonymousAuthPlugin``（允许匿名连接），并刻意不加载：

        - ``TopicAccessControlListPlugin``：不启用 ACL，所有 topic 可读写。
          测试认证/ACL 应当是与投递语义正交的独立用例集。
        - ``PacketLoggerPlugin``：会把每个报文都打进日志，产生大量噪声。
        - ``BrokerSysPlugin``：负责 $SYS 主题刷新，测试用不上，且会引入
          一条与 psutil 相关的 DeprecationWarning。

        注意：插件配置走 dacite 严格校验，键名必须与插件 ``Config``
        dataclass 的字段名**完全一致**——例如 ``allow_anonymous``（下划线），
        而不是旧顶层 ``auth`` 段用的 ``allow-anonymous``（连字符）。
        写错会抛 ``PluginLoadError``，而且报错信息里看不到具体原因。
        """
        return {
            "listeners": {
                "default": {
                    "type": "tcp",
                    "bind": f"{self.endpoint.host}:{self.endpoint.port}",
                    "max_connections": 0,
                }
            },
            "plugins": {
                "amqtt.plugins.authentication.AnonymousAuthPlugin": {
                    "allow_anonymous": True,
                },
            },
        }


class ExternalBroker:
    """指向已经运行的外部 broker。

    生命周期由外部管理（docker compose up / 远程服务），本类只做可达性
    检查，方便在测试开始前尽早失败，而不是等到第一个用例超时才报错。
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 1883,
        name: str = "external",
    ) -> None:
        self.endpoint = BrokerEndpoint(host, port)
        self.name = name

    def start(self, timeout: float = 5.0) -> "ExternalBroker":
        if not wait_port_open(self.endpoint.host, self.endpoint.port, timeout):
            raise RuntimeError(
                f"外部 broker 不可达：{self.endpoint.url}\n"
                f"请先启动：docker compose up -d"
            )
        logger.info("已连接外部 broker [%s]：%s", self.name, self.endpoint.url)
        return self

    def stop(self) -> None:
        # 外部 broker 的生命周期不归测试管
        logger.info("外部 broker [%s] 保持运行", self.name)
