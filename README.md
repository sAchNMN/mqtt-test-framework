# MQTT 协议自动化测试框架

针对 MQTT broker 的协议一致性自动化测试框架。被测对象是**真实的 broker
实现**（Mosquitto、EMQX、amqtt），不是自己写的玩具——因为测试的价值来自
被测对象有多复杂、多容易出错，而不是测试脚本本身有多花哨。

覆盖范围：投递语义（QoS 0/1/2）、遗嘱消息、保留消息、会话管理、协议边界、
并发故障注入。

---

## 当前状态

| 测试域 | 状态 | 用例数 |
|---|---|---|
| 环境自检 | ✅ 完成 | 4 |
| QoS 投递语义 | ✅ 完成 | 12 |
| 遗嘱消息 LWT | 🚧 骨架（含实现清单） | 2 skipped |
| 保留消息 Retain | 🚧 骨架（含实现清单） | 2 skipped |
| 会话管理 Session | 🚧 骨架（含实现清单） | 2 skipped |
| 协议边界异常 | 📋 规划中 | - |
| 并发与压力（1000 连接） | 📋 规划中 | - |

本地实测（内嵌 broker，Windows / Python 3.13）：

```
16 passed, 6 skipped in 8.30s
```

---

## 快速开始

```bash
pip install -r requirements.txt
pytest
```

默认使用**内嵌 amqtt broker**，零外部依赖，本地和 CI 都能直接跑。

### 换成真实 broker

```bash
# Mosquitto（docker compose）
docker compose up -d mosquitto
MQTT_BACKEND=docker pytest

# EMQX（docker compose，需要用 profile 显式启用）
docker compose --profile emqx up -d
MQTT_HOST=127.0.0.1 MQTT_PORT=1885 MQTT_BROKER_NAME=emqx pytest

# 任意远程 broker
MQTT_HOST=mqtt.example.com MQTT_PORT=1883 pytest
```

同一套用例可以在多个 broker 上跑，用于做**实现差异的横向对比**——
这是框架的设计目标之一。

### 后端选择规则

| 环境变量 | 效果 |
|---|---|
| `MQTT_HOST` 已设置 | 连接该外部 broker（优先级最高） |
| `MQTT_BACKEND=docker` | 连接 docker compose 的 Mosquitto |
| 其他 / 未设置 | 内嵌 amqtt broker（默认） |

---

## 目录结构

```
mqtt-test-framework/
├── framework/                  # 框架本体（与被测用例解耦）
│   ├── broker.py               # broker 后端抽象：内嵌 / 外部
│   ├── client.py               # 测试用 MQTT 客户端 + 消息收集器
│   └── assertions.py           # 断言工具集
├── tests/
│   ├── conftest.py             # 全局 fixture（broker / 客户端工厂 / topic 隔离）
│   ├── test_environment.py     # 环境自检层
│   ├── test_qos.py             # ★ QoS 投递语义
│   ├── test_lwt.py             # 遗嘱消息
│   ├── test_retain.py          # 保留消息
│   └── test_session.py         # 会话管理
├── docker/mosquitto.conf
├── docker-compose.yml
├── pytest.ini
└── .github/workflows/ci.yml
```

---

## 设计要点

### 1. broker 后端抽象

`EmbeddedBroker` 与 `ExternalBroker` 暴露同一个 `endpoint` 接口，测试代码
不关心后端是哪一种。这让"同一套用例跑多个 broker 实现"成为零成本的事。

### 2. 客户端就绪等待（框架最核心的价值）

直接用 paho-mqtt 写测试会踩四个坑，框架全部封装掉了：

- `connect()` 只发 CONNECT 报文，**不等待 CONNACK**
- `subscribe()` 是异步的，SUBACK 回来之前发布的消息会丢
- 消息只能通过回调获取，测试线程需要线程安全的等待原语
- keepalive 默认值对小众 broker 偏紧

### 3. 消息收集器：`wait_quiet()` 而不是固定 sleep

断言"就这些了、没有多余的"必须等到消息流安静下来。固定 `sleep` 在慢机器上
会误判、在快机器上白等。`MessageCollector.wait_quiet()` 用条件变量实现，
连续 `quiet_period` 秒没有新消息才认为流已结束。

### 4. 环境自检层

`test_environment.py` 把"环境问题"和"被测对象问题"分开。这一层失败时，
后面所有失败都不必深究——先修环境。这是真实测试体系里的常规分层。

### 5. 用例隔离

每个用例通过 `topic` fixture 拿到独占的 topic 前缀（`pytest/<随机>`），
避免并发或残留状态互相污染。

---

## 实测踩过的坑

这四条都是开发过程中真实遇到并解决的，不是教科书摘抄：

| 现象 | 根因 | 解法 |
|---|---|---|
| 客户端"连上了"但一条消息都收不到 | `connect()` 返回时 TCP 还没建好，`subscribe()` 报 `MQTT_ERR_NO_CONN` 静默失败 | 轮询 `is_connected()` 等待 CONNACK |
| 发 20 条 QoS 2 消息后客户端被踢，报 `Keep alive timeout` | 测试里把 keepalive 传成了 5 秒，amqtt 的 PINGRESP 响应跟不上 | keepalive 提到 60 秒 |
| 12 个用例跑了 127 秒 | 注册了 `on_subscribe` 却忘了 `on_disconnect`，每次断开都空等满 5 秒超时 | 补上回调，**降到 8.3 秒** |
| broker 和客户端互相阻塞，完全跑不通 | amqtt 是 asyncio 实现，paho 是线程驱动，放在同一个事件循环里会互相堵死 | broker 隔离到独立线程 + 独立事件循环 |

最后一条尤其值得记住：**asyncio 服务端 + 线程驱动客户端必须在不同线程**。

---

## QoS 用例覆盖了什么

| 用例 | 验证的规范行为 |
|---|---|
| `test_all_messages_delivered_at_each_qos[qos0/1/2]` | 三档 QoS 下的基础投递链路 |
| `test_qos2_delivers_exactly_once` | QoS 2 四步握手的"恰好一次"承诺，30 条零重复 |
| `test_delivery_qos_is_min_of_publish_and_subscribe` | **规范 4.3.5：投递 QoS = min(发布 QoS, 订阅 QoS)**，覆盖 6 种降级组合 |
| `test_payload_integrity_for_tricky_payloads` | 二进制帧、`0x00`、UTF-8、空载荷、4KB 大报文的原样透传 |
| `test_qos1_at_least_once_under_sustained_load` | 持续负载（100 条）下的不丢消息 |

其中 min 规则那一条最容易被误解：**发布端用 QoS 2 并不能让订阅端收到 QoS 2**，
订阅端降级会把整条链路拉低。`pub2-sub0` 和 `pub2-sub1` 两个组合就是在演示这件事。

---

## CI

`.github/workflows/ci.yml` 跑两个 job：

1. **内嵌 broker**：Python 3.11 / 3.12 / 3.13 三版本矩阵，零外部依赖
2. **真实 Mosquitto**：用 GitHub Actions 起容器，对生产级 broker 跑同一套用例

两个 job 都产出 JUnit XML 并上传为 artifact。

---

## 路线图

- [x] 框架核心：broker 后端抽象、客户端就绪等待、消息收集器、断言工具
- [x] 环境自检层
- [x] QoS 投递语义（12 用例）
- [ ] 遗嘱消息 LWT（异常掉线告警的协议级基础）
- [ ] 保留消息 Retain
- [ ] 会话管理（Clean Session / 持久会话 / 离线补发 / takeover）
- [ ] 协议边界异常（超长 topic、非法报文、超大报文的拒绝行为）
- [ ] 并发压测（asyncio，目标 1000 并发连接 + 重连风暴）
- [ ] 多 broker 横向对比报告（amqtt vs Mosquitto vs EMQX）
