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

## 安装

### 从源码编译

```bash
git clone https://github.com/erichll/go-fast-note-sync.git
cd go-fast-note-sync
go build -o ~/.local/bin/go-fast-note-sync ./cmd
```

### 验证安装

```bash
go-fast-note-sync --help
```

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

## 配置项说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `api` | — | 同步服务的 Base URL |
| `api_token` | — | 认证 Token（支持 `${ENV_VAR}`） |
| `vault` | — | 服务端 vault 名称 |
| `vault_path` | — | 本地 vault 的绝对路径 |
| `client_type` | `LinuxCLI` | 上报给服务端的客户端标识；旧版服务端可改为 `ObsidianPlugin` |
| `sync_enabled` | `true` | 是否启用笔记/附件同步 |
| `config_sync_enabled` | `true` | 是否启用 `.obsidian` 设置同步 |
| `offline_delete_sync_enabled` | `false` | 是否推送离线期间的本地删除操作 |
| `sync_update_delay` | `500` | 本地文件事件防抖延迟（毫秒） |
| `binary_sync_limit_enabled` | `true` | 跳过超过 128 MiB 的文件 |
| `concurrency_control_enabled` | `true` | 启用上传并发槽控制 |
| `max_concurrent_uploads` | `3` | 最大并发上传数 |
| `sync_exclude_folders` | `[]` | 需排除的 vault 相对路径文件夹 |
| `sync_exclude_extensions` | `[]` | 需排除的文件扩展名 |
| `startup_delay` | `0` | 首次连接前的等待秒数 |
| `sync_timeout_seconds` | `60` | 单轮同步最大等待秒数 |
| `state_file` | 自动 | 覆盖默认状态文件路径 |

默认状态文件路径：`~/.local/share/go-fast-note-sync/state.json`

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
cmd/              CLI 入口（start / status / sync / init-config）
internal/
  config/         YAML 配置加载与默认值
  state/          原子 JSON 状态持久化
  hash/           SHA-256 内容与路径哈希
  local/          watcher 到 sync 的事件契约
  sync/           WebSocket 客户端、启动同步、协议处理器
  watcher/        fsnotify 文件监听集成
deploy/systemd/   systemd 用户服务单元
docs/             设计文档与协议参考
test/smoke/       集成冒烟测试套件
```

## 协议说明

本工具实现了 `obsidian-fast-note-sync` 插件所使用的 WebSocket 同步协议：

- 健康检查：`GET /api/health`
- WebSocket 地址：`ws(s)://[api]/api/user/sync?lang=&count=&client=&clientName=&clientVersion=`
- 文本帧格式：`ACTION|JSON`
- 二进制帧格式：2 字节前缀（`00`）+ 36 字节 session ID + 4 字节分片索引 + 数据

同步模块：**NoteSync**、**FileSync**、**SettingSync**、**FolderSync**，每个模块均有 `*SyncEnd` 完成信号。

## 已知限制

- `status` 和手动 `sync` 命令尚未实现
- 暂无 CI 流水线
- `SendFolderModify` 仅在内存中更新文件夹快照；崩溃发生在下次状态保存之前时会丢失该增量
- WebSocket 客户端无读超时/pong 处理器，重连依赖服务端主动关闭连接

## 开源协议

[Apache 2.0](LICENSE)
