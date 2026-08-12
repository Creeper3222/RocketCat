# RocketCatShell

[![Platform](https://img.shields.io/badge/Platform-OneBot%20v11%20Reverse%20WS-pink)](#)
[![Runtime](https://img.shields.io/badge/Python-%3E%3D3.11-blue)](#环境要求)

将 [Rocket.Chat](https://rocket.chat) 通过桥接方式接入 OneBot v11 生态的独立客户端。它继承了插件版 [RocketCat](https://github.com/Creeper3222/astrbot_plugin_rocketchat_onebot_bridge) 已经验证过的桥接核心、独立 WebUI 和管理能力，但已经不再依附于 AstrBot 插件宿主，而是作为一个可以独立运行、独立配置、独立扩展的本地控制台存在。

本项目的目标不是继续做一个“宿主里的桥接插件”，而是把 RocketCat 发展成一套真正独立的 `Rocket.Chat <-> OneBot v11` 桥接软件。

这意味着：

- RocketCatShell 自己拥有 `config/`、`data/`、`logs/` 目录边界。
- RocketCatShell 自己提供本地 WebUI、登录认证、Bot 管理和插件管理。
- RocketCatShell 仍然可以作为 OneBot reverse WebSocket 客户端与 AstrBot 协同，但不再依赖 AstrBot 插件宿主才能运行。

> 各版本的功能变更、兼容性调整、问题修复和迁移说明统一记录在 [CHANGELOG.md](CHANGELOG.md)。README 仅维护当前功能、配置与使用方式。

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
- 内置独立 WebUI，可管理网络配置、基础信息、运行诊断、日志、本地插件、文件、系统终端和基础设置。
- WebUI 默认启用登录门禁，初始密码为 `123456`。
- 支持自定义 WebUI 端口，并在端口占用时自动回退到可用端口。
- 支持配置导出 / 导入，统一打包 Bot 设置、WebUI 密码 / 端口、消息映射窗口条数上限、卡片顺序和本地插件主配置。
- Rocket.Chat 连接支持可配置的重连延迟、最大连续重连次数限制及失败后自动停用；OneBot 上游采用独立的 5 秒后台等待，不会因为上游未启动而停用 Bot。
- 支持动态订阅新房间，机器人被拉入新房间后无需重启。
- 支持兼容 AstrBot 唤醒词 / 指令的入站消息格式，标准 `message` / `raw_message` 保持为纯当前用户正文。
- 支持 OneBot 风格的群聊、私聊、消息查询、群成员查询、登录信息查询。
- 支持以内存热存储 + snapshot / journal 恢复的运行态，降低高频消息场景下的磁盘读写压力。
- 支持文本、`at`、引用回复、图片、文件、语音、视频、Markdown 出站发送。
- 支持引用链提取、回复来源识别、提及用户映射、群聊 / 私聊上下文映射，以及发送者 / 提及 / 回复 / 子频道等独立认知元数据。
- 支持将两个及以上 Rocket.Chat 顶层引用按原顺序转换为多个普通 OneBot `reply` 段，供 AstrBot 原生构造多个 `Reply.chain`。
- 支持固定大小的 message 索引窗口、超窗自动裁剪和 WebUI 手动窗口重建。
- 支持远端媒体下载、上传大小限制控制、本地临时文件落地和 Base64 媒体上传。
- 支持 Rocket.Chat 官方 E2EE 私聊 / 私有群组文本与媒体收发。
- 支持本地插件系统，可发现、启停、重载、卸载本地插件，并在运行时接管 OneBot action。
- 支持内置指令系统插件 `rocketcat_plugin_built_in_command`，当前提供精确纯文本 `#rocketcat` 与 `#system` 两条本地命令。
- `#rocketcat` 可在 Rocket.Chat 房间内直接返回当前桥接 Bot 基础信息、连接状态、OneBot self_id、bot 头像和服务器 branding 信息。
- `#system` 可在 Rocket.Chat 房间内直接返回当前 Shell 主机的系统快照，用于快速查看版本、CPU、内存与进程占用状态。
- [I Am Thinking](https://github.com/sssn-tech/astrbot_plugin_iamthinking) 适配能力已从核心桥接层剥离为本地插件 `rocketcat_plugin_adapt_iamthinking`。
- `rocketcat_plugin_adapt_iamthinking` 现已支持把 `set_msg_emoji_like` 独立映射为 Rocket.Chat 贴表情与 typing 指示器，并允许分别开关。
- 支持项目级单实例启动保护，阻止同一目录下重复拉起多份 RocketCatShell runtime。
- 采用更紧凑的热存储、`orjson` JSON 快路径、媒体流式落盘、插件 action 索引分发，以及 WebUI 控制面缓存 / 日志长轮询。

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
- `set_msg_emoji_like`：由本地插件决定是否处理；当前 `rocketcat_plugin_adapt_iamthinking` 可把该动作映射为 Rocket.Chat reaction 与可选 typing 指示器

### 当前不支持的 OneBot 动作

- `get_forward_msg`
- `send_group_forward_msg`
- `send_private_forward_msg`

RocketCatShell 当前明确不承诺合并转发消息语义。

---

## 消息与媒体能力

### 入站能力

- Rocket.Chat 文本消息会被转换为 OneBot `message` 事件。
- 私聊会映射为 OneBot `private` 消息。
- 频道和私有群组会映射为 OneBot `group` 消息。
- 标准 OneBot `message` / `raw_message` 会优先保持纯当前用户正文，确保 AstrBot 的唤醒词、命令前缀和 `startswith(...)` 检查仍然成立。
- Rocket.Chat `mentions` 会转换为 OneBot `at` 段。
- Rocket.Chat 引用、消息链接、线程回复会转换为 OneBot `reply` 语义，并补充引用上下文文本。
- 两个及以上顶层 Rocket.Chat 引用会转换为多个并列 OneBot `reply` 段；顺序、重复引用和当前消息自身媒体均会保留。
- 多引用中的每个 `reply` 都可通过 `get_msg` 独立取得被引用消息的文本与媒体；被删除或不可访问的引用使用附件快照回退。
- 发送者、提及、引用链、回复摘要、房间名、房间 slug、上下文群 ID 等 Rocket.Chat 认知信息会以独立字段写入事件和消息注册表。
- 图片、普通文件、音频、视频附件会被识别并转换成对应的 OneBot 媒体段。
- 不支持直接桥接的媒体会降级为可读文本占位，避免整条消息消失。

### 出站能力

- OneBot `text` 直接发送为 Rocket.Chat 文本。
- OneBot `at` 会转换为 Rocket.Chat `@username` 或 `@all`。
- OneBot `image` 支持 HTTP(S) 链接、本地文件和 Base64 数据。
- OneBot `file`、`record`、`video` 支持本地文件；远端媒体会先尝试下载再上传，并统一使用当前 Bot 的媒体大小上限。
- OneBot `markdown` 会按文本内容发往 Rocket.Chat。

### 上下文与映射

- Rocket.Chat 的房间 ID、用户 ID、消息 ID 会被桥接器映射为可持久化的 OneBot surrogate ID，但热路径以内存态为准。
- 每个 bot 的桥接运行态会落盘为 `runtime.snapshot.bin` 与 `runtime.journal.bin`，覆盖 ID 映射、消息缓存、私聊房间映射、群上下文绑定和最近消息窗口，用于快速恢复最近状态。
- message 命名空间采用固定窗口，只保留最近 N 条映射；窗口整理时会同步刷新消息缓存、reply 关联以及 `latest_by_context_sender` 路由提示。
- 群聊上下文使用上下文房间注册表维持群上下文到真实房间的绑定关系。
- 私聊上下文使用私聊房间映射存储维护用户与私聊房间的绑定关系。
- 可选开启“子频道会话隔离”，把不同子房间拆成不同会话上下文。

---

## 性能与诊断

- 启动恢复阶段会记录 `snapshot_load_ms`、`journal_replay_ms` 和 `journal_records_replayed`，便于判断热存储恢复成本。
- 入站 tracing 会拆分 `translate` 与 `emit_event` 两个阶段，并把 `room_lookup`、`mapping_alloc`、`room_bindings`、`mention_segments`、`quote_contexts`、`mention_metadata`、`media_segments`、`context_media`、`message_store`、`batch_commit` 等热路径阶段拆开记录。
- `room_info_cache_ttl_seconds` 用于平衡房间元信息实时性与 REST 开销；默认值适合大多数稳定群组场景。
- “运行诊断”中的“性能与背压”默认折叠，展开后可查看事件循环延迟、日志队列，以及每个 Bot 的入站、OneBot action、Journal 和缓存指标；状态使用数字与文字共同表达。
- Rocket.Chat 首次登录和重连由后台监督器负责，WebUI 健康接口不等待 Bot 上线；OneBot 上游离线时持续后台等待，不消耗 Rocket.Chat 重连次数。
- 入站、OneBot action、Journal 和日志均使用固定容量队列。正常负载保持零丢失与同房间严格顺序；极端持续满载时入站只丢弃最新消息并精确计数、限频告警，OneBot action 会返回明确的忙碌响应。
- JSON 编解码优先走 `orjson`；身份映射、媒体缓存和插件扫描复用 Shell 级共享资源；普通远端媒体使用内容寻址缓存，PBKDF2、RSA 与大文件 E2EE 加解密移至专用双 Worker 线程池。
- 页面隐藏时，网络、诊断和日志轮询会暂停并取消在途请求；恢复可见后立即增量刷新。HTML 始终禁用缓存，带版本标记的静态资源使用长期 immutable 缓存。
- 开发者可使用 `tools/benchmark_inbound_translate.py --control-root <基线目录> --rebuild-root . --profile realistic --repeat 5 --json-output data/perf/benchmark.json` 生成五轮入站对照基准；`tools/stress_v022_full_stack.py --output data/perf/soak.json` 执行隔离全链路压力测试。运行产物保存在已忽略的 `data/perf/`，两项源码工具均不进入最小运行 ZIP。

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
- 发送到加密房间时，会自动走加密消息体和加密媒体上传确认流程；媒体上传会分块读取原文件并分块写出密文临时文件，避免把原文件和密文同时完整常驻内存。
- Rocket.Chat 8.2+ 删除加密附件时产生的 `removed-file` 标记会在解密合并后保留，避免旧密文中的附件重新出现。
- Rocket.Chat 8.3+ E2EE REST 接口启用严格请求校验后，RocketCatShell 只提交对应端点允许的字段。
- Rocket.Chat 8.5 对官方浏览器客户端的密钥存储和密码弹窗进行了调整，但没有更换服务端 E2EE 消息、房间密钥或媒体密文协议；RocketCatShell 继续使用独立的 Python 密钥实现。
- E2EE 多引用消息会把解密正文开头的连续系统引用行归一化为与非加密频道相同的多个顶层 OneBot `reply`，顺序、重复引用、`get_msg` 文本及参考图解析保持一致。
- E2EE 解密媒体不会再把系统 `%TEMP%` 绝对路径直接交给上游；RocketCatShell 会通过 WebUI 端口提供带随机令牌的本机 HTTP 媒体 URL，并在读取旧 `get_msg` 缓存时修复遗留路径或 Base64 引用，兼容 AstrBot 生图插件的安全目录策略。
- 如果 E2EE 初始化失败，不会影响未加密房间的正常收发。

---

## Rocket.Chat 版本兼容

| Rocket.Chat 版本 | 支持状态 | 上传链路 | Method 调用 |
|---|---|---|---|
| `< 7.10.0` | 不支持，启动时拒绝 | 不适用 | 不适用 |
| `7.10.x` | 支持 | 优先 `rooms.media` + `rooms.mediaConfirm`，仅端点不存在时回退 `rooms.upload` | 优先 REST `method.call`，仅端点不存在时回退 DDP method |
| `8.0.x–8.2.x` | 支持 | 固定现代两阶段上传 | REST `method.call` |
| `8.3.x–8.4.x` | 支持 | 固定现代两阶段上传并兼容严格 REST schema | REST `method.call` |
| `8.5.x` | 支持 | 增加文件名与 MIME 安全适配 | REST `method.call` |
| `> 8.5.x` | 未验证，按现代能力运行 | 不回退已移除的旧上传端点 | REST `method.call` |
| 无法识别版本 | 能力探测模式 | 现代链路优先，明确不兼容时允许回退 | REST 优先，明确不兼容时允许回退 |

WebSocket DDP 在所有受支持版本中仍用于 resume 登录、`stream-room-messages` 和 `stream-notify-user` 订阅。普通业务 method 不再默认直接走 WebSocket。

---

## 独立 WebUI
<p align="center">
  <img src="https://github.com/user-attachments/assets/9cd515ce-92f5-4a63-8d8d-8f42d360b836" width="100%" />
</p>

RocketCatShell 启动后会在本地启动一个独立 WebUI，默认监听 `127.0.0.1`，默认端口 `5751`。

### 页面能力

- `网络配置`：查看 Bot 状态、创建 / 编辑 / 删除 Bot。
- `基础信息`：查看每个 Bot 的账号信息、OneBot self ID、Rocket.Chat 服务器品牌头像和服务器名称。
- `运行诊断`：查看主机资源、Bot 运行状态、队列、缓存、快照、Journal 和 Rocket.Chat 服务端兼容信息。
- `猫猫日志`：查看 RocketCatShell 与 `RocketCatPerf` 运行日志，可按级别和 `Perf` 开关过滤，并支持清空日志。
- `插件管理`：管理 RocketCatShell 本地插件，包括启停、设置、重载和卸载。
- `文件管理`：浏览 RocketCatShell 项目根目录内文件，支持目录进入 / 返回、UTF-8 文本查看与允许范围内的编辑保存、图片预览、上传、重命名、移动、删除和打包下载；敏感持久化数据文件需要再次输入 WebUI 登录认证 / 文件管理鉴权密码。
- `系统终端`：创建、切换、排序和关闭本地终端会话，终端 WebSocket 复用 WebUI 登录认证。
- `基础设置`：管理 WebUI 登录认证 / 文件管理鉴权密码、WebUI 端口、消息映射窗口条数上限、配置导出 / 导入和 Windows 版本管理。
- `插件 Dashboard`：只为提供有效页面的插件显示入口，并在受限 iframe 中复用父 WebUI 的认证 Bridge；它由插件管理页动态进入，不占用固定侧栏入口。

### 导航、移动端与键盘操作

- 侧栏按“连接与状态 / 管理工具 / 系统”分组。桌面端可以独立记忆侧栏展开偏好；宽度不超过 `1120px` 时改为粘性顶栏和侧滑抽屉，移动抽屉每次打开 WebUI 时默认关闭。
- 八个核心页面分别使用 `#network`、`#basic`、`#diagnostics`、`#logs`、`#plugins`、`#files`、`#terminal`、`#settings` 地址；刷新、浏览器前进和后退会恢复当前页面。插件 Dashboard 继续使用带插件与页面名称的独立 hash。
- 移动抽屉支持菜单按钮、遮罩、`Escape` 和屏幕左侧 `20px` 边缘滑动打开，也可以拖动抽屉右缘关闭；打开期间会锁定背景滚动，手势完成后会把焦点交给当前导航项或菜单按钮。宽度不超过 `720px` 时，文件列表和 User 映射表会改为保留字段标签的卡片行，无需横向滚动。
- 手机端允许通过顶部拖柄下滑关闭没有未保存内容的安全 Dialog；Bot、插件和文件编辑等可能包含未保存内容的 Dialog，以及更新阻塞层，不提供手势关闭。确认 Dialog 下滑等价于选择“取消”。
- 全站提供可见键盘焦点。Dialog 使用浏览器原生模态语义并恢复触发器焦点；文件移动树支持方向键、`Home`、`End`、`Enter` 和空格。终端标签支持左右键、`Home`、`End` 和 `Alt+Shift+方向键` 即时调整顺序，也可以使用标签内的独立拖柄实时排序；普通标签区域仍可触摸横向滚动。
- 网络配置、基础信息和运行诊断中的 Bot 卡片共享一套显示顺序，插件卡片使用独立顺序。拖动卡片的空白区域或头像等非文字表面即可实时换位；按钮、开关、链接和文字区域不会触发拖动，卡片文字仍可正常选择复制。键盘用户可先聚焦卡片，再按空格或回车选择，使用方向键、`Home`、`End` 调整，随后按空格或回车保存，按 `Escape` 取消。排序只改变 WebUI 展示，不改变 Bot 或插件的运行时启动顺序。
- 状态通知最多同时显示三条，并可手动关闭或向任意水平方向滑走；错误会保留更长时间，鼠标悬停、键盘聚焦、拖动和页面转入后台时都会暂停剩余时间。
- 系统分别尊重 `prefers-reduced-motion`、`prefers-reduced-transparency` 和 `prefers-contrast: more`：减弱动效时停用可选位移/弹簧手势但保留最长 `120ms` 的透明度和颜色反馈，降低透明度时改用近实色表面，高对比度模式会增强边框、焦点环和遮罩。

### WebUI 认证
<p align="center">
  <img src="https://github.com/user-attachments/assets/d233e9d8-1931-46b0-9309-91957443e8f2" width="100%" />
</p>

- RocketCatShell 默认启用密码访问。
- 初始 WebUI 登录认证 / 文件管理鉴权密码为 `123456`。
- 后端提供登录、登出、Cookie 会话和受保护 API 访问控制。

### Windows 版本管理

- 版本管理固定读取 `Creeper3222/RocketCat` 的 GitHub Releases，普通进入页面使用 10 分钟缓存，手动刷新至少间隔 60 秒；检查更新不会自动下载或安装。
- 可选版本按 SemVer 排序，目标高于当前版本时显示“升级”，低于当前版本时显示“回滚”，相同时显示“重装”。后端会直接排除所有 `< v0.2.2` 版本。
- 切换版本前会完整下载并验证官方资产、GitHub SHA-256 和包内清单。只有验证完成后才会关闭当前 Shell，因此网络或包校验失败不会影响正在运行的版本。
- 如果修改过 WebUI 端口但尚未重启生效，需先正常重启 RocketCatShell，确保配置端口与当前 loopback 健康端点一致后才能切换版本。
- 更新只替换 RocketCatShell 核心、两个内置插件和发布清单声明的根运行文件。`config/`、`logs/`、Bot 数据、用户插件、插件数据、媒体缓存、数据库、快照、Journal 与 `.venv` 不会被删除或整体替换。
- 更新期间浏览器会显示重启状态；目标版本通过本机健康检查后自动恢复页面。如果目标启动失败，助手会恢复原版本并在 WebUI 返回后报告自动回滚结果。
- 如果事务进入 `recovery_required`，新的版本切换会被阻止；`launcher.bat` 会在下次启动时重试恢复，仍失败则停止启动，避免在不完整代码上继续运行。
- 版本管理目前只支持 Windows Live；Docker/Linux 不支持此更新链路，两个内置插件随 RocketCatShell 整包更新，不提供独立插件更新渠道。
- 会话失效时，前端会自动跳回登录页。
- WebUI 登录认证 / 文件管理鉴权密码不允许设置为空。

### 配置导出 / 导入

- 导出默认文件名为 `rocketcat_config.json`。
- 顶层判别字段为 `Is rocketcat config`。
- 导出内容包含所有 Bot 设置（包括 `room_info_cache_ttl_seconds` 与 `perf_trace_enabled`）、WebUI 登录认证 / 文件管理鉴权密码、WebUI 端口、消息映射窗口条数上限、共享 Bot / 独立插件卡片顺序和规范化后的本地插件主配置。
- 导入时会先校验判别字段、卡片顺序和已安装插件配置，再以事务方式统一写入。v0.2.1 等旧配置没有卡片顺序字段时会保留当前顺序，并按导入 Bot 顺序追加新实体；新格式中的已卸载实体会被忽略，遗漏实体会自动追加。
- I Am Thinking 适配器的旧配置会自动补齐四组状态 ID 与工具 / 错误 shortcode；单组重复 ID 会按原顺序去重，非法整数或跨状态重复 ID 会在任何配置写入前明确报错。

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
- 全局单例运行：一个启用插件只创建一个实例，各 Bot 只建立轻量 runtime binding
- 插件级原子重载；候选实例初始化或任一 runtime 绑定失败时继续保留旧实例
- 自动发现并在现有 WebUI 内打开插件 Dashboard
- 卸载插件本体，并可选删除插件主配置与插件持久化数据

当前内置示例包括：

- `rocketcat_plugin_built_in_command`：RocketCatShell 自有的内置指令系统插件。当前精确拦截 `#rocketcat` 与 `#system`，在本地直接回复，不再把命令正文继续交给上游；插件回复也会在入站侧抑制自回显再次上报。
- `rocketcat_plugin_adapt_iamthinking`：用于接管 `set_msg_emoji_like`。适配器通过可配置的上游数字 ID 数组识别思考、工具、错误、完成四态，再映射为 Rocket.Chat shortcode；思考和工具阶段维持 typing，错误和完成阶段主动清除，长时间处理仍会自动续期心跳。
- 发布仓库默认仅跟踪 `rocketcat_plugin_built_in_command` 与 `rocketcat_plugin_adapt_iamthinking` 两个内置插件；其它位于 `data/plugins/` 的插件目录视为本地扩展，不随默认源码发布一并提交。

### 插件生命周期

| Hook | 作用域 | 用途 |
|---|---|---|
| `on_initialize()` | 每个插件实例一次 | 初始化全局资源、注册 Dashboard API / SSE。 |
| `on_load(runtime)` | 每个已启用 Bot 一次 | 建立当前 Bot 的轻量绑定；消息与 Action 处理仍显式接收该 `runtime`。 |
| `on_unload(runtime)` | 每个 Bot 解绑一次 | 只清理当前 runtime 的状态，不能误删其他 Bot 的状态。 |
| `on_terminate()` | 每个插件实例一次 | 插件禁用、卸载、成功替换或 Shell 关闭时清理全局任务。 |

### 内置 Dashboard 目录与接口

插件页面按以下目录自动发现：

```text
data/plugins/rocketcat_plugin_example/
├─ metadata.yaml
├─ main.py                         # 纯静态 Dashboard 可省略
└─ pages/
   └─ dashboard/
      ├─ index.html
      ├─ app.js
      └─ styles.css
```

`metadata.yaml` 可用 `dashboard_page: dashboard` 指定默认页面；否则优先使用名为 `dashboard` 的页面，再回退到按名称排序后的第一个页面。没有有效 `pages/<name>/index.html` 的插件不会显示 Dashboard 按钮。

需要后端能力的插件在全局初始化阶段注册 RocketCat 原生接口：

```python
from rocketcat_shell.plugin_system.base import RocketCatPlugin


class Plugin(RocketCatPlugin):
    async def on_initialize(self):
        self.context.register_dashboard_api(
            "status",
            self.get_status,
            methods={"GET"},
        )
        self.context.register_dashboard_sse("events", self.stream_events)

    async def get_status(self, request):
        return {"ok": True, "query": request.query}
```

页面侧通过注入的 `window.RocketCatPluginDashboard` 使用 `getContext()`、`apiGet()`、`apiPost()`、`upload()`、`download()`、`subscribeSSE()` 和 `unsubscribeSSE()`。iframe 不持有 WebUI Cookie 或登录令牌；父页面负责认证和转发。静态资源 URL 使用高强度临时令牌，并在插件禁用、重载、卸载或页面关闭时失效。

---

## 环境要求

| 项目 | 要求 |
|------|------|
| Python | `>= 3.11` |
| 运行依赖 | `aiohttp`, `cryptography`, `fastapi`, `orjson`, `psutil`, `python-multipart`, `uvicorn`, `websockets`；其中 `websockets` 为 WebUI 系统终端和实时通道提供 Uvicorn WebSocket 后端，Windows 额外使用 `pywinpty` |
| Rocket.Chat | 支持 `7.10.x–8.5.x`，需要可用的 REST API、DDP/WebSocket 订阅和 E2EE 接口（如使用加密功能） |
| OneBot 上游 | 需要可用的 OneBot v11 reverse WebSocket 服务 |

---

## 安装依赖

### 方式一：直接运行launcher.bat（推荐）

如果你已经有 Python 环境，直接运行launcher.bat，启动器会自动检测 `requirements.txt` 中缺失或版本不兼容的依赖并自动补装

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

如果检测到 `requirements.txt` 中声明的依赖缺失或版本不兼容，启动器还会自动执行：

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

### 单实例保护

- RocketCatShell 启动时会先尝试获取项目级锁文件 `logs/rocketcat_shell.instance.lock`。
- 如果同一项目目录下已经有一份 RocketCatShell 正在运行，新的启动进程会在 runtime 初始化前直接退出，并输出当前持锁实例的 pid / 启动时间信息。
- 这层保护用于避免旧版多开时出现的“双份 runtime 同时订阅 Rocket.Chat、重复向上游转发消息”的问题。

---

## 首次启动与初始化行为

RocketCatShell 在第一次安装、还没有保存过任何配置时，会自动在项目根目录下创建并写入：

- `config/`
- `config/plugins_config/`
- `data/`
- `data/temp/`
- `data/bots/`
- `data/plugins/`
- `data/plugin_data/`
- `logs/`
- `config/shell.json`
- `config/bots.json`

其中初始默认值包括：

- WebUI 地址：`127.0.0.1:5751`
- WebUI 初始密码：`123456`
- 最大消息映射窗口条数：`1000`
- shell 默认 OneBot reverse WS 地址：`ws://127.0.0.1:6199/ws/`

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
<p align="center">
  <img src="https://github.com/user-attachments/assets/611a6601-0af6-4ebf-ac3c-e301a03631eb" width="100%" />
</p>
在 `网络配置` 页点击 `新建 Bot`，为该 Bot 填写：

- Rocket.Chat 服务器地址
- Rocket.Chat 用户名
- Rocket.Chat 密码
- 按需填写 E2EE 密钥密码
- OneBot reverse WS 地址
- OneBot Access Token

OneBot `self_id` 不需要也不能手动填写；RocketCatShell 会根据 Rocket.Chat Bot 的不可变 `userId` 自动建立 `sha256-linear-v1` 映射。

高级设置中还可以进一步设置：

- Rocket.Chat 重连延迟
- Rocket.Chat 最大连续重连次数
- 子频道会话隔离
- 远端媒体上传 / 下载大小上限
- 忽略机器人自己的消息
- 调试日志

这两项重连设置只约束 Rocket.Chat 聊天服务器侧。AstrBot 等 OneBot 上游未连接时，Bot 会继续保持启用，RocketCatShell 每 5 秒在后台尝试连接；首次进入等待状态和恢复连接时各记录一次信息日志，后续重复失败只写入调试日志。离线期间产生的新事件不会积压，也不会在恢复后作为过期事件补发。

### 4. 如需导入已有配置
<p align="center">
  <img src="https://github.com/user-attachments/assets/ba61315c-9273-4f30-a6a0-ac55a19297f1" width="100%" />
</p>

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
| `webui_access_password` | WebUI 登录认证 / 文件管理鉴权密码，默认 `123456`。该密码同时用于登录 WebUI 和打开敏感持久化数据文件。 |
| `message_index_max_entries` | 最大消息映射窗口条数，默认 `1000`；超出后会清理最早映射，并在达到重置阈值后自动重排当前窗口。 |
| `log_level` | 日志级别，默认 `INFO`。 |
| `auto_open_browser` | 启动后是否自动打开浏览器。 |
| `default_onebot_ws_url` | 新建 Bot 时使用的默认 OneBot reverse WS 地址。 |
| `default_onebot_access_token` | 新建 Bot 时使用的默认 OneBot Access Token。 |
| `default_reconnect_delay` | 默认 Rocket.Chat 重连延迟；不作用于 OneBot 上游。 |
| `default_max_reconnect_attempts` | 默认 Rocket.Chat 最大连续重连次数；不作用于 OneBot 上游。 |
| `default_enable_subchannel_session_isolation` | 默认是否开启子频道会话隔离。 |
| `default_remote_media_max_size` | 默认远端媒体上传 / 下载大小上限。 |
| `default_skip_own_messages` | 默认是否忽略机器人自己的消息。 |
| `default_debug` | 默认是否开启调试日志。 |
| `performance_profile` | 性能策略，当前仅提供 `balanced`。 |
| `inbound_worker_count` | 入站 Worker 数量；`0` 按 CPU 自动选择 2 或 4。 |
| `onebot_outgoing_queue_max_entries` | OneBot 出站队列上限，默认 `512`。 |
| `identity_cache_max_entries` | 用户身份与 Rocket.Chat 用户缓存上限，默认 `4096`。 |
| `media_cache_max_bytes` | `data/temp` 媒体缓存总量上限，默认 `1 GiB`。 |
| `media_cache_max_age_hours` | 媒体缓存最长保留时间，默认 `168` 小时。 |
| `log_file_max_bytes` | 单个日志文件上限，默认 `10 MiB`。 |
| `log_file_backup_count` | 轮转日志备份数量，默认 `3`。 |
| `terminal_max_sessions` | WebUI 终端会话上限，默认 `6`。 |
| `terminal_idle_timeout_seconds` | 无连接终端的空闲关闭时间，默认 `0`；`0` 表示不限制，仅作用于 WebUI 终端会话，不会关闭 RocketCatShell 进程。 |

#### 性能与资源（高级设置）

WebUI 的“性能与资源（高级设置）”统一管理消息映射、入站并发、队列、缓存、日志和终端资源边界。普通部署保持默认值即可；设置会写入 `config/shell.json`，并随“导出配置”完整导出，导入旧配置时缺失字段会自动采用当前安全默认值。

| 设置项 | 详细行为 |
|--------|----------|
| 性能策略 | 对应 `performance_profile`。当前仅提供 `balanced`，作为兼顾吞吐、响应速度和资源占用的稳定基线，并为其他策略预留扩展位。 |
| 入站 Worker | 对应 `inbound_worker_count`，允许 `0`～`8`。默认 `0` 表示自动选择：CPU 核心数不超过 4 时使用 2 个 Worker，否则使用 4 个；显式设置后按指定数量并发处理 Rocket.Chat 入站消息。保存后会协调重建受影响的 Bot runtime。 |
| 最大消息映射窗口条数 | 对应 `message_index_max_entries`，代码默认值为 `1000`，已有配置继续保留用户当前值。该窗口保存 Rocket.Chat 消息 ID 与 OneBot 消息编号之间的近期映射；缩小窗口会立即按新上限整理热存储，达到编号重置阈值时会保留当前窗口并重新编号。窗口越大，历史 `get_msg` / 引用恢复范围越长，同时占用更多内存和快照空间。 |
| OneBot 出站队列上限 | 对应 `onebot_outgoing_queue_max_entries`，允许 `1`～`100000`，默认 `512`。限制 OneBot 已连接时等待发送给 AstrBot 的实时事件数量；队列满或上游离线时，新事件会被丢弃并计入运行诊断，不会阻塞 Rocket.Chat 入站处理，也不会在重连后补发过期事件。保存后会协调重建受影响的 Bot runtime。 |
| 身份缓存上限 | 对应 `identity_cache_max_entries`，允许 `128`～`1000000`，默认 `4096`。限制 Rocket.Chat 用户资料及 `sha256-linear-v1` 身份映射热缓存规模；同一服务器的多个 Bot 共享服务器级身份存储核心，超出上限后按最近使用顺序淘汰，SQLite 持久映射不会被删除。保存后会协调重建受影响的 Bot runtime。 |
| 媒体缓存上限 | 对应 `media_cache_max_bytes`，允许 `1 MiB`～`1 TiB`，默认 `1 GiB`。限制项目级 `data/temp` 内容寻址缓存的总大小；清理器优先移除较旧且当前未发布的缓存文件，不影响正在通过令牌 HTTP URL 上报的媒体。保存后会协调重建受影响的 Bot runtime。 |
| 媒体缓存保留时间 | 对应 `media_cache_max_age_hours`，允许 `1`～`87600` 小时，默认 `168` 小时（7 天）。媒体清理器在启动时及运行期间定期执行，同时受总量上限约束；用户仍可在停止相关操作后手动清理 `data/temp`。保存后会协调重建受影响的 Bot runtime。 |
| 单个日志文件上限 | 对应 `log_file_max_bytes`，允许 `1 MiB`～`1 TiB`，默认 `10 MiB`。`logs/rocketcat.log` 达到上限后执行轮转，防止日志文件无限增长；该边界在下次完整启动 RocketCatShell 时生效。 |
| 日志备份数量 | 对应 `log_file_backup_count`，允许 `0`～`100`，默认 `3`。控制轮转日志的历史副本数量；设为 `0` 时不保留轮转备份。该边界在下次完整启动 RocketCatShell 时生效。 |
| 终端会话上限 | 对应 `terminal_max_sessions`，允许 `1`～`64`，默认 `6`。限制 WebUI 同时保留的交互终端数量，避免终端进程和输出缓冲无界增长；新上限在下次完整启动 RocketCatShell 时生效。 |
| 终端空闲关闭 | 对应 `terminal_idle_timeout_seconds`，允许 `0`～`604800` 秒，默认 `0`。仅清理没有 WebUI 连接的空闲终端会话，不会关闭 Bot runtime 或 RocketCatShell 主进程；设为 `0` 表示不限制空闲时间。新超时值在下次完整启动 RocketCatShell 时生效。 |

保存高级设置时，消息映射窗口会立即整理；入站 Worker、OneBot 队列和身份/媒体缓存边界会触发增量 runtime reconciliation；日志与 WebUI 终端边界则在下次完整启动 Shell 后生效。配置导入会校验同样的取值范围，导出结果会保留全部高级设置，避免迁移环境后悄悄回落到默认值。

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
| `OneBot self_id` | 不再由用户配置；根据 Rocket.Chat Bot 的不可变 userId 自动建立 `sha256-linear-v1` 映射。 |
| `reconnect_delay` | Rocket.Chat 断线重连等待秒数；OneBot 上游固定使用独立的 5 秒等待。 |
| `max_reconnect_attempts` | Rocket.Chat 最大连续重连次数；`0` 表示不限次数。OneBot 上游始终持续等待，不受此项限制。 |
| `enable_subchannel_session_isolation` | 是否按子频道隔离上下文。 |
| `remote_media_max_size` | 当前 Bot 的远端媒体上传 / 下载大小上限。 |
| `room_info_cache_ttl_seconds` | 房间信息缓存 TTL，单位秒，默认 `300`。 |
| `perf_trace_enabled` | 是否输出入站性能追踪日志；也可被环境变量 `ROCKETCAT_PERF_TRACE` 覆盖。 |
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
	temp/
	update/

logs/
	rocketcat.log
```

说明：

- `config/` 只保存配置和插件主配置。
- `data/` 保存全局媒体临时缓存、本地插件本体、插件持久化数据和各 Bot 运行时数据。
- `data/temp/` 保存所有 Bot 共用的解密媒体与临时下载文件；目录内容是可重建缓存，可以由用户手动查看和清理。
- `data/update/` 保存 Windows 版本检查缓存、暂存包、事务记录和恢复备份；它是本机运行状态，不进入源码仓库或发布包。
- `data/bots/<bot>/runtime.snapshot.bin` 保存最近一次热存储快照，覆盖 ID 映射、消息缓存、私聊房间映射和群上下文绑定。
- `data/bots/<bot>/runtime.journal.bin` 保存快照之后的增量变更，用于启动恢复和窗口整理后的状态回放。
- Bot 运行时仍然会按目录划分，但桥接热路径以内存态为准，不再依赖旧版逐文件在线更新模式。
- `logs/` 保存 RocketCatShell 自己的运行日志。

当前代码中的路径解析都基于项目根目录的相对布局发现，不依赖写死的 Windows 绝对路径。

---

## 已知限制

- 当前仍然围绕 OneBot v11 reverse WebSocket 工作，不是官方 Rocket.Chat 平台适配器。
- Rocket.Chat 合并转发消息语义当前未定义，OneBot `get_forward_msg`、`send_group_forward_msg` 和 `send_private_forward_msg` 均未实现；多引用消息使用多个普通 `reply` 段表达，不等同于合并转发。
- 系统事件、审计事件、编辑 / 撤回 / 已读等非消息类事件不在当前桥接承诺范围内。
- E2EE 仅覆盖 Rocket.Chat 加密私聊和加密私有群组。
- 远端媒体如果下载失败、上传 / 下载超出大小限制或源地址不可用，相关媒体发送会失败或降级，并写入 error 日志。
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
