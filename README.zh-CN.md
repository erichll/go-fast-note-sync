# go-fast-note-sync

> [English](README.md) | **简体中文**

一个运行于 Linux 无头环境的 Go CLI 后台守护进程，通过 WebSocket 将本地 [Obsidian](https://obsidian.md) vault 与自建的 [fast-note-sync-service](https://github.com/haierkeys/fast-note-sync-service) 实例进行双向自动同步，无需桌面环境，无需 GUI。

## 功能特性

- **双向同步** — 笔记、附件、插件设置、文件夹全覆盖
- **实时文件监听** — 通过 fsnotify 监听本地变更，带防抖与回声抑制
- **二进制文件分片传输** — 支持断点续传的多分片上传/下载及检查点持久化
- **启动同步** — 连接时全量对账，之后仅推送增量变更
- **自动重连** — 断线后自动重连，支持 systemd `Restart=on-failure`
- **systemd 就绪** — 内置用户服务单元文件，支持无人值守后台运行
- **YAML 配置** — 支持环境变量展开（`${VAR}` 语法）
- **持久化状态** — 原子写入 JSON 状态文件，崩溃安全的 pending 哈希和上传检查点

## 环境要求

- Go 1.24.5+
- 可访问的 `fast-note-sync-service` 实例及有效的 API Token
- Linux（主要目标平台）；其他 POSIX 系统可能可用，但未经测试

## 已验证兼容性

下表只列出经过项目保留的真实服务 smoke 套件验证的组合。其他版本组合可能可以工作，但项目不将其声明为已验证。

| go-fast-note-sync | Obsidian 客户端协议基线 | fast-note-sync-service | 验证情况 |
|-------------------|--------------------------|------------------------|----------|
| `v1.1.x` | 官方客户端 `2.4.0` | `3.6.0` | 17–20-case 真实服务 smoke（`v1.1.0`–`v1.1.3`） |
| `v1.0.0` | 客户端 `2.0.15` 代际协议 | 历史记录未固定版本 | 14-case 真实服务 smoke；不声明明确服务端版本 |

新部署推荐使用最新的 `v1.1.x` 版本（`v1.1.3`）搭配服务端 `3.6.0`。

## 安装

### 从 GitHub Release 下载（推荐）

在 [GitHub Releases 页面](https://github.com/erichll/go-fast-note-sync/releases) 下载对应平台的预编译二进制。每个 Release 包含各平台归档包和用于校验的 `checksums.txt`。

**Linux x86-64 示例：**

```bash
VERSION=v1.1.3
curl -LO "https://github.com/erichll/go-fast-note-sync/releases/download/${VERSION}/go-fast-note-sync_linux_amd64.tar.gz"
curl -LO "https://github.com/erichll/go-fast-note-sync/releases/download/${VERSION}/checksums.txt"
tar -xzf go-fast-note-sync_linux_amd64.tar.gz
install -m 755 go-fast-note-sync ~/.local/bin/go-fast-note-sync
```

校验哈希：

```bash
sha256sum -c checksums.txt --ignore-missing
```

> **二进制发布支持平台：** linux/amd64、linux/arm64、darwin/amd64、darwin/arm64、windows/amd64。
> 项目主要支持的运行目标为 **Linux/headless**。darwin 和 windows 平台归档仅作为发布产物提供，不声明已做真实运行支持。

### 通过 Docker（GHCR）

```bash
docker pull ghcr.io/erichll/go-fast-note-sync:latest
```

GHCR 镜像已设为公开，无需 `docker login`。将 `latest` 替换为具体版本标签（如 `v1.1.3`）可固定版本。

> **架构说明：** 已发布镜像仅为 **linux/amd64**。发布归档额外覆盖 linux/arm64、darwin/amd64、darwin/arm64 和 windows/amd64。其他架构请参考 **[deploy/docker/README.md](deploy/docker/README.md)** 自行构建镜像。

完整操作指南见 **[deploy/docker/README.md](deploy/docker/README.md)**。

### 从源码编译

```bash
git clone https://github.com/erichll/go-fast-note-sync.git
cd go-fast-note-sync
go build -o ~/.local/bin/go-fast-note-sync ./cmd
```

### 验证安装

```bash
go-fast-note-sync --help
go-fast-note-sync --version
```

## 升级到 v1.1.x

v1.1.0 采用官方客户端 2.4.0 和服务端 3.6.0 使用的 rolling hash 与分页同步契约。首次启动时会自动迁移旧版 state：清除不兼容的哈希缓存、pending 哈希、上传检查点、增量同步时间和首次同步标记，从而执行一次完整对账；文件夹快照和 WebSocket 连接计数会保留。

配置文件无需修改。与其他同步引擎升级一样，建议先停止守护进程并备份 vault 和 state 文件，再替换二进制。

## 快速开始

**1. 生成默认配置**

```bash
go-fast-note-sync init-config
# 写入 ~/.config/go-fast-note-sync/config.yaml
```

**2. 编辑配置**

```yaml
# ~/.config/go-fast-note-sync/config.yaml
api: https://your-fast-note-sync-service.example.com
api_token: ${FAST_NOTE_TOKEN}   # 或直接填写 token
vault: MyVault
vault_path: /home/you/vault/MyVault
sync_enabled: true
config_sync_enabled: true
```

**3. 启动守护进程**

```bash
go-fast-note-sync start
# 或指定配置文件路径：
go-fast-note-sync start --config /path/to/config.yaml
```

**查看同步状态（离线）**

```bash
go-fast-note-sync status
# 输出 vault、api、同步开关、各模块最后同步时间、缓存计数和 pending 计数
```

**触发一次性同步后退出**

```bash
go-fast-note-sync sync
# 连接服务、等待启动同步轮完成后以 exit 0 退出
# 使用 --timeout 覆盖默认 60s 超时：
go-fast-note-sync sync --timeout 120s
```

## 配置项说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `api` | — | 同步服务的 Base URL |
| `api_token` | — | 认证 Token（支持 `${ENV_VAR}`） |
| `vault` | — | 服务端 vault 名称 |
| `vault_path` | — | 本地 vault 的绝对路径 |
| `client_type` | `GoFastNoteSync` | 上报给服务端的客户端标识；如 token scope 限制了 `c:<value>` 需与之一致 |
| `sync_enabled` | `true` | 是否启用笔记/附件同步 |
| `config_sync_enabled` | `true` | 是否启用 `.obsidian` 设置同步 |
| `offline_delete_sync_enabled` | `false` | 是否推送离线期间的本地删除操作 |
| `readonly_sync_enabled` | `false` | 单向模式：只把服务端变更落到本地，从不上传本地变更 |
| `manual_sync_enabled` | `false` | 关闭自动同步轮（连接、启动同步、文件监听触发），仅 `sync` 命令触发同步 |
| `offline_sync_strategy` | `""` | 服务端离线冲突策略：`""`、`manualMerge`、`ignoreTimeMerge`、`newTimeMerge`；旧值 `auto` 会归一化为 `""` |
| `sync_update_delay` | `500` | 本地文件事件防抖延迟（毫秒） |
| `binary_sync_limit_enabled` | `true` | 跳过超过 128 MiB 的文件 |
| `concurrency_control_enabled` | `true` | 启用上传并发槽控制 |
| `max_concurrent_uploads` | `3` | 最大并发上传数 |
| `sync_exclude_folders` | `[]` | 字面量 vault 相对路径，或任意层级的单层目录名 |
| `sync_exclude_extensions` | `[]` | 需排除的文件扩展名（不区分大小写） |
| `sync_exclude_whitelist` | `[]` | 覆盖普通排除规则的字面量 vault 相对路径 |
| `config_sync_other_dirs` | `[]` | 除 `.obsidian` 外额外作为设置文件同步的 vault 目录 |
| `startup_delay` | `0` | 首次连接前的等待秒数 |
| `auto_redirect_enabled` | `true` | 跟随 `/api/health` 重定向并采用新的运行时 API 地址 |
| `sync_timeout_seconds` | `0` | 单轮同步最大等待秒数；`0` 使用内置默认 60 秒 |
| `state_file` | 自动 | 覆盖默认状态文件路径 |

默认状态文件路径：`~/.local/share/go-fast-note-sync/state.json`

### 排除规则

文件夹与白名单规则是区分大小写的字面量，不是正则表达式。Windows 分隔符和结尾斜杠会被归一化；匹配遵循路径段边界。例如 `cache` 会排除任意层级中名为 `cache` 的目录，而 `private/cache` 只排除该 vault 相对路径下的子树。

点文件和点目录默认排除。白名单可恢复它们，并覆盖文件夹/扩展名排除规则；被排除的祖先目录仍可遍历，但本身不参与同步。以 `.tmp` 结尾、包含 `.tmp.`、或匹配内部 `.<filename>.tmp-*` 暂存名的临时路径，AppleDouble/`.DS_Store` 产物，以及敏感插件配置，均无法通过白名单恢复。`.obsidian` 保留其专属设置同步范围。这些过滤器不会自动删除已存在于远端的对应文件。

这一点有意与官方插件的正则/默认不区分大小写匹配器不同。目标目录内的附件暂存设计上继承 Windows 目录 ACL，但其 Windows/SMB 运行时效果尚未经过独立验证。

## Token 与客户端标识

`client_type` 在建立 WebSocket 连接时以 `?client=` 查询参数发送给服务端，同时作为 `ClientInfo` 握手消息的 `type` 字段。默认值为 `GoFastNoteSync`。

### 在管理控制台创建令牌

创建或编辑令牌时，控制台提供**客户端限制**输入框。该字段填写的值必须与配置文件中的 `client_type` 完全一致，否则 WebSocket 握手会被服务端以 `AuthorizationFaild`（错误码 315）拒绝。

| 控制台"客户端限制"填写值 | 配置文件 `client_type` | 结果 |
|--------------------------|------------------------|------|
| `GoFastNoteSync` | `GoFastNoteSync`（默认值） | ✅ 连接成功 |
| *（留空 / 不限制）* | 任意值 | ✅ 连接成功 |
| `GoFastNoteSync` | `MyCustomClient` | ❌ 握手被拒绝 |

**推荐做法：** 为本守护进程签发令牌时，客户端限制填写 `GoFastNoteSync`，无需修改配置文件，开箱即用。

若需要自定义客户端标识，两侧保持一致即可：

```yaml
client_type: MyCustomClient
```

## systemd 用户服务

复制单元文件并启用：

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/go-fast-note-sync.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now go-fast-note-sync
```

服务默认读取 `~/.config/go-fast-note-sync/config.yaml`。可将敏感信息写入 `~/.config/go-fast-note-sync/sync.env`：

```bash
# sync.env
FAST_NOTE_TOKEN=your_token_here
```

查看状态与日志：

```bash
systemctl --user status go-fast-note-sync
journalctl --user -u go-fast-note-sync -f
```

## Docker 部署

`deploy/docker/` 目录提供了多阶段 Dockerfile 和 Compose 文件。完整的操作指南（含镜像构建、配置、卷布局和 inotify 调优）详见 **[deploy/docker/README.md](deploy/docker/README.md)**。

## 开发

```bash
# 格式化
go fmt ./...

# 静态检查（需要 golangci-lint）
golangci-lint run ./...

# 带竞态检测的测试
go test -race ./... -coverprofile=coverage.out -covermode=atomic

# 编译
go build ./...
```

覆盖率要求：总体语句覆盖率和 `internal/sync` 语句覆盖率均需不低于 80%。

## 项目结构

```
.github/workflows/  CI（fmt、lint、race 测试、覆盖率门槛）与 tag 发布/GHCR 推送
cmd/                CLI 入口（start / status / sync / init-config）
internal/
  config/           YAML 配置加载、默认值与 ${ENV} 展开
  hash/             与官方协议兼容的 rolling 内容/路径哈希
  local/            watcher 到 sync 的事件契约
  state/            原子 JSON 状态持久化
  sync/             WebSocket 客户端、同步轮、协议处理器、分片传输、排除规则
  watcher/          fsnotify 集成：防抖、重命名配对与 overflow 上报
deploy/systemd/     systemd 用户服务单元
deploy/docker/      多阶段 Dockerfile、Compose 文件与运维 README
test/smoke/         真实服务 smoke 套件（用例、共享 harness、清理脚本）
```

## 协议说明

本工具实现了 `obsidian-fast-note-sync` 插件所使用的 WebSocket 同步协议：

- 健康检查：`GET /api/health`
- WebSocket 地址：`ws(s)://[api]/api/user/sync?lang=&count=&client=&clientName=&clientVersion=`
- 文本帧格式：`ACTION|JSON`
- 二进制帧格式：2 字节前缀（`00`）+ 36 字节 session ID + 4 字节分片索引 + 数据

同步模块：**NoteSync**、**FileSync**、**SettingSync**、**FolderSync**，每个模块均有 `*SyncEnd` 完成信号。

## 开源协议

[Apache 2.0](LICENSE)
