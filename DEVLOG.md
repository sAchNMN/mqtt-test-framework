# 开发日志

记录**决策、踩坑与量化结果**。目标读者有两个：三个月后的我，以及任何想了解这个项目是怎么做出来的人。

写作原则：

- 只写**为什么**和**数据**，不写"我改了哪一行"——git diff 已经说了
- **失败的尝试同样记录**，它比成功更能说明问题
- 每条都带一个可复用的结论，而不只是"我遇到并解决了"

---

## 2026-10-14 · 搭建框架骨架

### 决策

**为什么默认用内嵌 broker，而不是直接要求 Docker**

本机没装 Docker（Windows 侧与 WSL 内均无）。如果默认依赖 Docker，这个项目在
"随手 clone 下来跑一遍"的场景下会直接失败——而 README 里那句"一键跑通"是
面试官最看重的东西之一。

于是引入 `amqtt` 作为进程内 broker 作为默认后端，Docker 的 Mosquitto / EMQX
作为可选的"真实 broker"后端，用 `MQTT_BACKEND` 环境变量切换。

代价：amqtt 不是生产级实现，某些行为可能与 EMQX 存在差异。**这恰好是后续
做横向对比测试的素材**，不是缺陷。

**为什么显式声明 plugins，而不依赖 amqtt 的默认配置**

默认值会随库版本变化。框架应当明确声明自己依赖什么，而不是把行为交给别人的
默认值。见下文踩坑 5。

### 踩坑

#### 1. 客户端"连上了"，却收不到任何消息

- **现象**：TCP 连上了，`is_connected()` 返回 `True`，但 `publish()` 报
  `The client is not currently connected`，测试里一条消息都收不到。
- **定位**：打印 `subscribe()` 的返回码，发现是 `MQTT_ERR_NO_CONN`——
  订阅其实从未成功。
- **原因**：paho 的 `connect()` 只发出 CONNECT 报文，**不等待 CONNACK**；
  `subscribe()` 同样不等 SUBACK。在握手完成前发起的操作会静默失败。
- **解法**：`connect()` 后轮询 `is_connected()` 直到真正确认；`subscribe()`
  内部等待 SUBACK 回调后才返回。
- **结论**：**任何"异步握手"型 API，返回不等于就绪。** 必须找到一个可验证的
  就绪信号，而不是靠 `sleep` 猜。

#### 2. asyncio broker 与线程驱动客户端互相阻塞

- **现象**：把 broker 和客户端放在同一个事件循环里，完全跑不通。
- **原因**：amqtt 是 asyncio 实现，paho 是线程驱动。客户端的同步等待会
  堵死事件循环，broker 因而无法处理任何网络包——表现为"连接建立后一片死寂"。
- **解法**：把 broker 隔离到独立线程 + 独立事件循环。
- **结论**：**asyncio 服务端 + 线程驱动客户端必须运行在不同线程。**
  这条对任何"用 Python 测试 asyncio 服务"的场景都适用。

#### 3. keepalive 设成 5 秒，导致客户端被误判掉线

- **现象**：连续发 20 条 QoS 2 消息后客户端被踢掉，日志报 `Keep alive timeout`。
- **原因**：测试里把 keepalive 写成了 5 秒（本能地当成"超时时间"了）。
  实际语义是"客户端承诺的心跳间隔"，broker 需在此周期内响应 PINGREQ。
  amqtt 的 PINGRESP 响应不够及时，客户端在 1.5 倍 keepalive 后自动断开。
- **解法**：keepalive 提到 60 秒。
- **结论**：keepalive 是"我容忍对方多慢"的参数，不是"我等多久算超时"。
  对接慢速实现时要放宽，否则会把自己的客户端踢下线。

#### 4. 测试套件跑 127 秒 —— 性能问题

- **现象**：12 个用例跑了 127 秒，比被测逻辑本身的耗时长两个数量级。
- **定位**：在每个 fixture 的建立/销毁处加计时打点，发现**每个用例的
  `disconnect()` 都恰好耗时 5 秒**——正好是我设的等待超时值。
- **原因**：`disconnect()` 内部等待一个事件被唤醒，但我只注册了
  `on_subscribe` 回调，**忘了注册 `on_disconnect`**。等待的事件永远不会被 set，
  于是每次都等满超时。
- **解法**：补上 `on_disconnect` 回调；并在 `connect()` 时重置该事件，
  使同一个客户端实例可以被复用。
- **数据**：**127s → 8.3s，提速 15 倍。**
- **结论**：**每一个"等待"都必须验证"唤醒源真的存在"。** 写等待逻辑时先问
  一句：谁会来 set 这个事件？这个教训适用于任何条件变量 / 事件驱动的代码。

#### 5. amqtt 插件配置走严格校验，报错不友好

- **现象**：改用官方推荐的 `plugins` 段配置后抛 `PluginLoadError`，
  而异常信息里**看不到任何具体原因**。
- **原因**：插件配置经 dacite 以 `strict=True` 反序列化，键名必须与插件
  `Config` dataclass 的字段名**完全一致**。旧顶层 `auth` 段用的是
  `allow-anonymous`（连字符），而 dataclass 字段实际叫 `allow_anonymous`
  （下划线）。
- **解法**：改用下划线；顺带显式只加载 `AnonymousAuthPlugin`，排除
  `PacketLoggerPlugin`（报文级日志噪声）与 `BrokerSysPlugin`（引入一条
  与 psutil 相关的废弃警告）。
- **结论**：**报错信息不友好时，直接去读源码里的配置解析逻辑，比反复试错快。**
  这里读 `plugins/manager.py` 的 `_load_str_plugin` 五分钟就定位了。

#### 6. CI 交付时从未真正运行过

- **现象**：CI 中"真实 Mosquitto 对比测试"job 失败，报 `mosquitto 启动超时`。
- **定位**：看容器日志，发现 broker **其实正常启动了并接受了大量连接**——那一长串
  `New client connected ... disconnected` 就是健康检查用的 `mosquitto_sub`
  在反复重试。真正的问题不是 broker 没起来，是检查命令永远拿不到结果。
- **原因**：`docker/mosquitto.conf` 里设了 `sys_interval 0`（禁用 $SYS 主题更新），
  而健康检查却用 `mosquitto_sub -t '$SYS/broker/version'` 等消息。
  **配置与验证手段自相矛盾**，必然超时。
- **附带问题**：`chown: /mosquitto/config/mosquitto.conf: Read-only file system`
  ——entrypoint 要改配置文件属主，但挂载用了 `:ro`。
- **解法**：
  - 健康检查改用 TCP 端口探测（`socket.create_connection`），不依赖 broker 的任何主题
  - 挂载去掉 `:ro`
  - `docker-compose.yml` 的 healthcheck 同步改为 `mosquitto_pub -q 1`（发布并等 PUBACK）
- **数据**：同一次运行中，3 个内嵌 broker job（Python 3.11 / 3.12 / 3.13）全部通过，
  amqtt 的跨版本兼容性风险由此排除。
- **结论**：**没有运行过的 CI 等于没有 CI。** 我交付这套 workflow 时只在 Windows
  本地跑过 pytest，从未执行过 workflow 本身，于是把一个必然失败的检查逻辑发了出去。
  凡是自己没亲眼见过绿的东西，都不该当作"已完成"交付。

### 本地真实 broker 环境（WSL + 原生 Mosquitto）

**背景**：Windows 侧与 WSL 内都没有 Docker。为了不让"对真实 broker 跑测试"这条
路径只能依赖 CI，在 WSL（Ubuntu 26.04）里装了原生 Mosquitto 2.0.22。

**做法**：
- `apt-get install mosquitto mosquitto-clients`（用 WSL 的 `-u root` 执行，无需 sudo 密码）
- **关键点**：mosquitto 2.x 默认**禁止匿名访问**，且只监听 localhost。把项目里的
  `docker/mosquitto.conf` 放进 `/etc/mosquitto/conf.d/`，即得到
  `allow_anonymous true` + `listener 1883`（绑定所有接口）
- Windows 侧经 WSL2 的 localhost 转发直连 `127.0.0.1:1883`，不需要额外网络配置

**结果**：
- 同一套用例对 Mosquitto 跑通：`16 passed, 6 skipped in 8.07s`
- **与 amqtt 的结果完全一致** —— 这是第一个横向对比数据点
- 顺带验证了 `docker/mosquitto.conf` 本身在真实 mosquitto 上的语法与语义有效性
- 还验证了修复后的 CI 端口探测命令：对已监听端口返回 0、对未监听端口正确返回非 0

**结论**：本地 WSL 原生 broker 是 Docker 的轻量替代，用于快速验证"真实 broker
兼容性"，不必拉镜像。CI 里仍用 Docker（GitHub runner 没有 WSL 可用）。

### 结果

- 测试套件：`16 passed, 6 skipped`，零警告
  - 内嵌 amqtt：8.20s
  - WSL 原生 Mosquitto：8.07s
- 覆盖范围：环境自检 4 条 + QoS 投递语义 12 条
- 项目规模：16 个文件 / 1099 行 Python
- CI：Python 3.11 / 3.12 / 3.13 矩阵（通过）+ 真实 Mosquitto 容器对比 job
  （首次失败，根因见踩坑 6；已修复，并在本地用真实 Mosquitto 验证过）

### 下一步

实现 LWT（遗嘱消息）三条用例。这是物联网设备"失联告警"的协议级基础——
设备断开这件事本身就是信号。`MqttTestClient.kill()` 已提供"不发 DISCONNECT
直接掐断 TCP"的故障注入能力，可以直接用。
