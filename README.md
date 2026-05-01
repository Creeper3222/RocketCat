# RocketCatShell

[![Platform](https://img.shields.io/badge/Platform-OneBot%20v11%20Reverse%20WS-pink)](#)
[![Runtime](https://img.shields.io/badge/Python-%3E%3D3.11-blue)](#环境要求)

将 [Rocket.Chat](https://rocket.chat) 通过桥接方式接入 OneBot v11 生态的独立客户端。它继承了插件版 [RocketCat](https://github.com/Creeper3222/astrbot_plugin_rocketchat_onebot_bridge) 已经验证过的桥接核心、独立 WebUI 和管理能力，但已经不再依附于 AstrBot 插件宿主，而是作为一个可以独立运行、独立配置、独立扩展的本地控制台存在。

本项目的目标不是继续做一个“宿主里的桥接插件”，而是把 RocketCat 发展成一套真正独立的 `Rocket.Chat <-> OneBot v11` 桥接软件。

这意味着：

- RocketCatShell 自己拥有 `config/`、`data/`、`logs/` 目录边界。
- RocketCatShell 自己提供本地 WebUI、登录认证、Bot 管理和插件管理。
- RocketCatShell 仍然可以作为 OneBot reverse WebSocket 客户端与 AstrBot 协同，但不再依赖 AstrBot 插件宿主才能运行。

---

## 架构说明

```text
Rocket.Chat Server
		^
		|  REST API + DDP/WebSocket
		v
RocketCatShell
		^
		|  OneBot v11 Reverse WebSocket Client
		v
OneBot v11 Consumer
		^
		|  plugins / providers / event pipeline
		v
AstrBot or other compatible OneBot-side workflow
```

声明：

- RocketCatShell 当前仍然围绕 OneBot v11 reverse WebSocket 语义工作。
- 目前已经适配 [AstrBot](https://github.com/AstrBotDevs/AstrBot)，其它onebot v11语义后续再考虑实现
- 如果你的上游是 AstrBot，那么可以继续直接复用 AstrBot 自带的 aiocqhttp / OneBot v11 接入链路。
- RocketCatShell 当前不是一个通用的 Rocket.Chat 官方平台适配器，而是一套 OneBot 语义桥接器。

---

## 功能特性

- 支持 Rocket.Chat 频道、私有群组、私聊消息桥接为 OneBot v11 语义。
- 支持统一 Bot 注册表，不再使用主 bot / 副 bot 的分层持久化模型。
- 内置独立 WebUI，可管理网络配置、基础信息、运行日志、基础设置和本地插件。
- WebUI 默认启用登录门禁，初始密码为 `123456`。
- 支持自定义 WebUI 端口，并在端口占用时自动回退到可用端口。
- 支持配置导出 / 导入，统一打包 Bot 设置、WebUI 密码 / 端口和本地插件主配置。
- 支持自动重连、最大连续重连次数限制、自动停用失败 Bot。
- 支持动态订阅新房间，机器人被拉入新房间后无需重启。
- 支持 OneBot 风格的群聊、私聊、消息查询、群成员查询、登录信息查询。
- 支持文本、`at`、引用回复、图片、文件、语音、视频、Markdown 出站发送。
- 支持引用链提取、回复来源识别、提及用户映射、群聊 / 私聊上下文映射。
- 支持远端媒体下载、大小限制控制、本地临时文件落地和 Base64 媒体上传。
- 支持 Rocket.Chat 官方 E2EE 私聊 / 私有群组文本与媒体收发。
- 支持本地插件系统，可发现、启停、重载、卸载本地插件，并在运行时接管 OneBot action。
- [I Am Thinking](https://github.com/sssn-tech/astrbot_plugin_iamthinking) 适配能力已从核心桥接层剥离为本地插件 `rocketcat_plugin_adapt_iamthinking`。

---

## 当前实现范围

### 已实现的 OneBot 动作

- `send_group_msg`
- `send_private_msg`
- `send_msg`
- `get_msg`
- `get_group_info`
- `get_group_member_info`
- `get_group_member_list`
- `get_stranger_info`
- `get_login_info`
- `set_msg_emoji_like`：由本地插件决定是否处理；核心本身不再硬编码 I Am Thinking 逻辑

### 当前不支持的 OneBot 动作

- `send_group_forward_msg`
- `send_private_forward_msg`

RocketCatShell 当前这一版明确不承诺合并转发消息语义。

---

## 消息与媒体能力

### 入站能力

- Rocket.Chat 文本消息会被转换为 OneBot `message` 事件。
- 私聊会映射为 OneBot `private` 消息。
- 频道和私有群组会映射为 OneBot `group` 消息。
- Rocket.Chat `mentions` 会转换为 OneBot `at` 段。
- Rocket.Chat 引用、消息链接、线程回复会转换为 OneBot `reply` 语义，并补充引用上下文文本。
- 图片、普通文件、音频、视频附件会被识别并转换成对应的 OneBot 媒体段。
- 不支持直接桥接的媒体会降级为可读文本占位，避免整条消息消失。

### 出站能力

- OneBot `text` 直接发送为 Rocket.Chat 文本。
- OneBot `at` 会转换为 Rocket.Chat `@username` 或 `@all`。
- OneBot `reply` 会转换为 Rocket.Chat 消息链接引用格式。
- OneBot `image` 支持 HTTP(S) 链接、本地文件和 Base64 数据。
- OneBot `file`、`record`、`video` 支持本地文件；远端媒体会先尝试下载再上传。
- OneBot `markdown` 会按文本内容发往 Rocket.Chat。

### 上下文与映射

- Rocket.Chat 的房间 ID、用户 ID、消息 ID 会被桥接器映射为可持久化的 OneBot surrogate ID。
- 群聊上下文使用上下文房间注册表维持群上下文到真实房间的绑定关系。
- 私聊上下文使用私聊房间映射存储维护用户与私聊房间的绑定关系。
- 可选开启“子频道会话隔离”，把不同子房间拆成不同会话上下文。

---

## E2EE 支持

当前实现支持 Rocket.Chat 官方 E2EE 链路，覆盖：

- 加密私聊房间 `d`
- 加密私有群组 `p`
- 加密文本消息
- 加密图片、语音、视频、普通文件上传和下载

实现特征：

- 启用了 `e2ee_password` 后，桥接器会初始化本机密钥对并请求 / 同步房间密钥。
- 接收入站加密消息时，会自动解密再注入 OneBot 事件流。
- 发送到加密房间时，会自动走加密消息体和加密媒体上传确认流程。
- 如果 E2EE 初始化失败，不会影响未加密房间的正常收发。

---

## 独立 WebUI
<p align="center">
  <img src="https://github.com/user-attachments/assets/9cd515ce-92f5-4a63-8d8d-8f42d360b836" width="100%" />
</p>

RocketCatShell 启动后会在本地启动一个独立 WebUI，默认监听 `127.0.0.1`，默认端口 `5751`。

### 页面能力

- `网络配置`：查看 Bot 状态、创建 / 编辑 / 删除 Bot。
- `基础信息`：查看每个 Bot 的账号信息、OneBot self ID、Rocket.Chat 服务器品牌头像和服务器名称。
- `猫猫日志`：查看 RocketCatShell 运行日志，并支持清空日志。
- `基础设置`：管理 WebUI 登录密码、WebUI 端口，以及配置导出 / 导入。
- `插件管理`：管理 RocketCatShell 本地插件，包括启停、设置、重载和卸载。

### WebUI 认证
<p align="center">
  <img src="https://github.com/user-attachments/assets/d233e9d8-1931-46b0-9309-91957443e8f2" width="100%" />
</p>

- RocketCatShell 默认启用密码访问。
- 初始登录密码为 `123456`。
- 后端提供登录、登出、Cookie 会话和受保护 API 访问控制。
- 会话失效时，前端会自动跳回登录页。
- WebUI 登录密码不允许设置为空。

### 配置导出 / 导入

- 导出默认文件名为 `rocketcat_config.json`。
- 顶层判别字段为 `Is rocketcat config`。
- 导出内容包含所有 Bot 设置、WebUI 登录密码、WebUI 端口和本地插件主配置。
- 导入时会先校验判别字段；若不是 RocketCatShell 配置文件，则会返回失败提示。

---

## 本地插件系统
<p align="center">
  <img src="https://github.com/user-attachments/assets/cd1b4f28-02a7-467a-a6c6-739114a9e5bb" width="100%" />
</p>

RocketCatShell 当前已经拥有自己的本地插件系统，而不再只是依赖外部宿主插件机制。

当前约定如下：

- 插件本体目录：`data/plugins/<plugin>`
- 插件主配置：`config/plugins_config/<plugin>_config.json`
- 插件持久化数据：`data/plugin_data/<plugin>`

当前插件管理能力包括：

- 自动发现本地插件
- 读取 `metadata.yaml` 和可选 `_conf_schema.json`
- 保存插件主配置
- 启用 / 停用插件
- 运行时重载插件
- 卸载插件本体，并可选删除插件主配置与插件持久化数据

当前 `rocketcat_plugin_adapt_iamthinking` 已作为本地插件存在，用于接管 `set_msg_emoji_like` 并把思考中 / 已完成态映射为 Rocket.Chat reaction shortcode。

---

## 环境要求

| 项目 | 要求 |
|------|------|
| Python | `>= 3.11` |
| 运行依赖 | `aiohttp`, `cryptography`, `fastapi`, `uvicorn`, `PyYAML` |
| Rocket.Chat | 需要可用的 REST API、DDP/WebSocket 和 E2EE 接口（如使用加密功能） |
| OneBot 上游 | 需要可用的 OneBot v11 reverse WebSocket 服务 |

---

## 安装依赖

### 方式一：直接运行launcher.bat（推荐）

如果你已经有 Python 环境，直接运行launcher.bat，启动器会自动检测缺少的依赖并自动安装依赖

```bash
launcher.bat
```

### 方式二：使用本地虚拟环境

在项目根目录执行：

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```



---

## 启动

### Windows 启动器

项目根目录已经提供：

```text
launcher.bat
```

它会优先使用本地 `.venv\Scripts\python.exe`。

如果本地 `.venv` 不存在，启动器会自动尝试使用系统 `py -3` 或 `python` 创建 `.venv`。

如果检测到 `aiohttp`、`cryptography`、`fastapi`、`uvicorn` 或 `PyYAML` 等依赖缺失，启动器还会自动执行：

```bash
pip install -r requirements.txt
```

然后再启动 RocketCatShell。

### Python 模块入口

也可以直接使用：

```bash
python -m rocketcat_shell
```

可选参数：

- `--once`：只做初始化和状态构建，不启动 WebUI 服务器。
- `--no-browser`：启动后不自动打开浏览器。
- `--print-status`：把当前 shell 状态输出到标准输出。
- `--verbose`：本次运行强制使用 `DEBUG` 日志级别。

---

## 首次启动与初始化行为

RocketCatShell 在第一次安装、还没有保存过任何配置时，会自动在项目根目录下创建并写入：

- `config/`
- `config/plugins_config/`
- `data/`
- `data/bots/`
- `data/plugins/`
- `data/plugin_data/`
- `logs/`
- `config/shell.json`
- `config/bots.json`

其中初始默认值包括：

- WebUI 地址：`127.0.0.1:5751`
- WebUI 初始密码：`123456`
- shell 默认 OneBot reverse WS 地址：`ws://127.0.0.1:6199/ws/`
- `next_onebot_self_id`：`910001`

也就是说，只要依赖安装正确，RocketCatShell 在空配置状态下可以自己创建必需目录和初始配置文件。

---

## 快速开始

### 1. 准备 OneBot v11 reverse WebSocket 上游

如果你的上游是 [AstrBot](https://github.com/AstrBotDevs/AstrBot)，可以先在 AstrBot 中创建内置 OneBot v11 平台：

1. 打开 `机器人`
2. 点击 `+ 创建机器人`
3. 选择 `OneBot v11`
4. 填写反向 WebSocket 主机、端口与 Token

本地部署最常见的地址是：

```text
ws://127.0.0.1:6199/ws/
```

### 2. 启动 RocketCatShell

启动后打开：

```text
http://127.0.0.1:5751/
```

使用默认密码登录：

```text
123456
```

首次登录后建议立刻在 `基础设置` 页修改密码。

### 3. 创建第一个 Bot

在 `网络配置` 页点击 `新建 Bot`，为该 Bot 填写：

- Rocket.Chat 服务器地址
- Rocket.Chat 用户名
- Rocket.Chat 密码
- 按需填写 E2EE 密钥密码
- OneBot reverse WS 地址
- OneBot Access Token
- OneBot self_id

高级设置中还可以进一步设置：

- 重连延迟
- 最大连续重连次数
- 子频道会话隔离
- 远端媒体大小上限
- 忽略机器人自己的消息
- 调试日志

### 4. 如需导入已有配置

在 `基础设置` 页点击 `导入配置`，选择已有的 `rocketcat_config.json`。

如果要迁移当前环境，也可以先点击 `导出配置` 生成配置快照，再导入到新环境。

---

## 配置项说明

### Shell 主配置

`config/shell.json` 主要包含：

| 配置项 | 说明 |
|--------|------|
| `webui_host` | WebUI 监听主机，默认 `127.0.0.1`。 |
| `webui_port` | WebUI 监听端口，默认 `5751`。 |
| `webui_access_password` | WebUI 登录密码，默认 `123456`。 |
| `log_level` | 日志级别，默认 `INFO`。 |
| `auto_open_browser` | 启动后是否自动打开浏览器。 |
| `default_onebot_ws_url` | 新建 Bot 时使用的默认 OneBot reverse WS 地址。 |
| `default_onebot_access_token` | 新建 Bot 时使用的默认 OneBot Access Token。 |
| `default_reconnect_delay` | 默认重连延迟。 |
| `default_max_reconnect_attempts` | 默认最大连续重连次数。 |
| `default_enable_subchannel_session_isolation` | 默认是否开启子频道会话隔离。 |
| `default_remote_media_max_size` | 默认远端媒体大小上限。 |
| `default_skip_own_messages` | 默认是否忽略机器人自己的消息。 |
| `default_debug` | 默认是否开启调试日志。 |
| `next_onebot_self_id` | 下一个建议的 OneBot self_id。 |

### 单个 Bot 配置

`config/bots.json` 中每个 Bot 主要包含：

| 配置项 | 说明 |
|--------|------|
| `id` | Bot 唯一 ID。 |
| `name` | Bot 显示名。 |
| `enabled` | 是否启用该 Bot。 |
| `server_url` | Rocket.Chat 服务器地址。 |
| `username` | Rocket.Chat 用户名。 |
| `password` | Rocket.Chat 密码。 |
| `e2ee_password` | E2EE 私钥密码。 |
| `onebot_ws_url` | OneBot reverse WebSocket 地址。 |
| `onebot_access_token` | OneBot reverse WebSocket Token。 |
| `onebot_self_id` | OneBot 机器人 ID，必须唯一。 |
| `reconnect_delay` | 断线重连等待秒数。 |
| `max_reconnect_attempts` | 最大重连次数；`0` 表示不限次数。 |
| `enable_subchannel_session_isolation` | 是否按子频道隔离上下文。 |
| `remote_media_max_size` | 远端媒体大小上限。 |
| `skip_own_messages` | 是否忽略自己发出的消息。 |
| `debug` | 是否启用调试模式。 |

---

## 持久化目录

RocketCatShell 当前的正式目录语义如下：

```text
config/
	shell.json
	bots.json
	plugins_config/

data/
	bots/
	plugins/
	plugin_data/

logs/
	rocketcat.log
```

说明：

- `config/` 只保存配置和插件主配置。
- `data/` 保存本地插件本体、插件持久化数据和各 Bot 运行时数据。
- `logs/` 保存 RocketCatShell 自己的运行日志。

当前代码中的路径解析都基于项目根目录的相对布局发现，不依赖写死的 Windows 绝对路径。

---

## 已知限制

- 当前仍然围绕 OneBot v11 reverse WebSocket 工作，不是官方 Rocket.Chat 平台适配器。
- 合并转发消息当前未实现。
- 系统事件、审计事件、编辑 / 撤回 / 已读等非消息类事件不在这一版的桥接承诺范围内。
- E2EE 仅覆盖 Rocket.Chat 加密私聊和加密私有群组。
- 远端媒体如果下载失败、超出大小限制或源地址不可用，相关媒体发送会失败或降级。
- `set_msg_emoji_like` 的扩展行为依赖本地插件；如果未安装对应插件，核心会返回未处理。

---

## 致谢
- 已适配上游[AstrBot](https://github.com/AstrBotDevs/AstrBot)
- 插件版 RocketCat 桥接器为当前独立版提供了已验证的桥接核心和 WebUI 设计基础
- 基础实现参考：[NET-Homeless/astrbot_plugin_rocket_chat_adapter](https://github.com/NET-Homeless/astrbot_plugin_rocket_chat_adapter) `v0.5.3`
- 与 AstrBot 的 OneBot v11 / aiocqhttp 协同链路为当前桥接路径提供了成熟上游
- [Rocket.Chat](https://rocket.chat) — 开源团队协作平台
- [aiohttp](https://github.com/aio-libs/aiohttp) — Python 异步 HTTP 客户端
- [FastAPI](https://fastapi.tiangolo.com/) — 轻量 WebUI 后端框架
