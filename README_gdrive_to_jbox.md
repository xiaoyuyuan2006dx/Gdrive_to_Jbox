# Google Drive ➔ 交大云盘（pan.sjtu.edu.cn）Colab 直传工具使用说明

面向 **Google Drive ──(Colab 挂载)──▶ 上海交通大学云盘（Jbox / `pan.sjtu.edu.cn`）** 工作环境的通用传输脚本与配套 Notebook。

直接对接交大云盘底层（腾讯云 SMH + COS）原生分片上传接口，**不再依赖 `TboxWebdav` 或 `rclone`**，实现「**云端整包单文件下载 + 本地 4 MiB 级分片断点续传**」。

---

## 核心特性

- **零额外中间组件**：仅依赖 Colab 预装 Python 环境（`requests`、`tqdm` + 标准库），无需下载、解压或常驻 `.NET` / WebDAV 代理进程。
- **整包落盘，同时保留断点续传**：
  - 本地无压缩打包后，通过交大云盘原生 Multipart API（`confirmKey` + 每批最多 50 片轮询续签）并发上传，服务端合并为**单个完整归档文件**；
  - 网页端/客户端可直接单文件高速下载，本地直接解压，无需再下载分卷并手工 `cat` 合并。
- **可靠校验与自动恢复**：
  - 每个分片上传直连 COS 并校验返回 `ETag`（32 位 hex 时比对本地分片 MD5）；
  - 分片签名 403 过期、SMH `accessToken` 401 过期自动无感刷新；
  - 若旧 `confirmKey` 在服务端过期，自动清理失效断点并重建会话完成上传；
  - 合并确认后再次查询远端元数据核对字节大小，确认一致后才清理临时归档与断点文件。
- **ZIP64 + UTF-8 无压缩打包**：默认使用 `ZIP_STORED`（不压缩），打包速度接近纯读写，支持 >4 GiB 大文件、中文文件名不乱码，并保留空目录，本地双击即可打开。
- **空间预检与凭证零落盘**：上传前校验交大云盘剩余配额与本地临时盘可用空间（约源大小 `105% + 64 MiB`）；任何 `UserToken`、`JAAuthCookie`、`accessToken` 均**绝不写入**断点文件或打印到日志。

---

## 文件说明

位于 Google Drive `/content/drive/MyDrive/Colab Notebooks/`：

| 文件 | 用途 |
| :--- | :--- |
| `gdrive_to_jbox.py` | 核心独立工具脚本（支持 CLI 命令行运行，也支持作为 Python 模块导入调用 `transfer()`） |
| `colab_transfer.ipynb` | 配套 Colab 交互式表单 Notebook，直接调用 `gdrive_to_jbox.py` |

---

## 快速开始：获取交大云盘凭证
可以参考https://github.com/1357310795/TboxWebdav 的README.md

支持两种认证方式，任选其一即可：

### 方式 A：`UserToken`（推荐）
1. 在浏览器登录新版交大云盘：https://pan.sjtu.edu.cn/
2. 按 `F12` 打开开发者工具 ➔ **Application（应用程序）** ➔ **Cookies** ➔ `https://pan.sjtu.edu.cn`
3. 复制 `UserToken` 对应的 Value。

### 方式 B：`JAAuthCookie`（jAccount）
1. 登录任意需要 jAccount 认证的校内系统（如 `my.sjtu.edu.cn`），再访问 https://jaccount.sjtu.edu.cn/jaccount/
2. 按 `F12` 打开开发者工具 ➔ **Application（应用程序）** ➔ **Cookies** ➔ `https://jaccount.sjtu.edu.cn`
3. 复制 `JAAuthCookie` 对应的 Value（脚本会自动完成 SSO 跳转换取 `UserToken`）。

> **凭证读取优先级与自动清洗**：
> - 脚本会自动去除粘贴内容前后的空格、单双引号以及 `UserToken=` / `JAAuthCookie=` 前缀。
> - 优先级：**函数参数 / CLI 参数（`--token` / `--ja-cookie`） > 环境变量（`JBOX_USER_TOKEN` / `JBOX_JA_COOKIE`） > 交互式隐藏输入（`getpass`）**。

---

## 三种使用方式

### 方式一：在 Colab Notebook 中使用（推荐）

打开 `colab_transfer.ipynb`，按顺序执行 3 个代码块即可：

1. **挂载 Google Drive**：将 Drive 挂载到 `/content/drive`。
2. **`1. 传输参数配置`**：在右侧表单填写/选择：
   - `SRC_PATH`：Google Drive 源路径（文件或目录，例如 `/content/drive/MyDrive/gakumas_assets`）
   - `REMOTE_DIR`：交大云盘目标目录（默认 `/colab_transfer`）
   - `PACK_MODE`：`single` / `subdir` / `direct`
   - `ARCHIVE_FORMAT`：`zip`（推荐）或 `tar`
   - `WORKERS`：并发上传分片线程数（`1`~`8`，默认 `4`）
   - `CHUNK_MB`：分片大小 MiB（`4` / `8` / `16`，默认 `4`）
   - `CONFLICT`：远端同名文件处理策略（`overwrite` / `rename` / `ask`）
   - `SCRIPT_PATH`：`gdrive_to_jbox.py` 路径
3. **`2. 一键打包并断点续传至交大云盘 (pan.sjtu.edu.cn)`**：选择 `AUTH_MODE`（`UserToken` / `JAAuthCookie`）及可选开关（`RUN_SPEEDTEST`、`FORCE_REPACK`、`FORCE_UPLOAD`、`KEEP_TEMP`、`DRY_RUN`），运行后按提示粘贴凭证并回车。

### 方式二：命令行（CLI）直接运行

在 Colab 终端或任意已挂载 Drive 的 Linux 环境中执行：

```bash
# 1. 默认模式：将目录无压缩整包打为单个 .zip 并上传到 /colab_transfer/
python3 "/content/drive/MyDrive/Colab Notebooks/gdrive_to_jbox.py" \
  --src "/content/drive/MyDrive/gakumas_assets" \
  --remote-dir "/colab_transfer"

# 2. 按一级子目录独立打成多个完整 .zip，逐个打包上传后即时清理临时包
python3 "/content/drive/MyDrive/Colab Notebooks/gdrive_to_jbox.py" \
  --src "/content/drive/MyDrive/gakumas_assets" \
  --remote-dir "/colab_transfer" \
  --pack-mode subdir

# 3. 不打包，直接上传单文件或按原目录树逐文件同步
python3 "/content/drive/MyDrive/Colab Notebooks/gdrive_to_jbox.py" \
  --src "/content/drive/MyDrive/some_folder" \
  --remote-dir "/backup" \
  --pack-mode direct

# 4. 仅离线预览传输计划与体积（不要求凭证、不打包、不上传）
python3 "/content/drive/MyDrive/Colab Notebooks/gdrive_to_jbox.py" \
  --src "/content/drive/MyDrive/gakumas_assets" \
  --pack-mode single --dry-run

# 5. 非交互传参（通过环境变量）+ 上传前 10 MiB 测速
JBOX_USER_TOKEN="你的UserToken" \
python3 "/content/drive/MyDrive/Colab Notebooks/gdrive_to_jbox.py" \
  --src "/content/drive/MyDrive/my_dataset" \
  --remote-dir "/datasets" \
  --speedtest
```

### 方式三：在其他 Python / Notebook 脚本中导入调用

```python
import importlib.util

script_path = "/content/drive/MyDrive/Colab Notebooks/gdrive_to_jbox.py"
spec = importlib.util.spec_from_file_location("gdrive_to_jbox", script_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

result = mod.transfer(
    src_path="/content/drive/MyDrive/my_folder",
    remote_dir="/colab_transfer",
    pack_mode="single",        # "single" | "subdir" | "direct"
    archive_format="zip",      # "zip" | "tar"
    auth_mode="UserToken",     # "UserToken" | "JAAuthCookie"
    workers=4,
    chunk_mb=4,
    conflict="overwrite",      # "overwrite" | "rename" | "ask"
    speedtest=False,
    dry_run=False,
)
print(result)
```

---

## 打包与传输模式对比

| 模式 (`--pack-mode`) | 云盘落盘结果（假设源目录为 `foo`） | 本地临时盘峰值占用 | 适用场景 |
| :--- | :--- | :--- | :--- |
| `single`（默认） | 单个归档文件 `<remote_dir>/foo.zip`（若源本身是单文件则直接传原文件） | 约 1 倍源目录体积 | **最推荐**。文件数多、希望云盘只保留 1 个包方便单链下载 |
| `subdir` | `<remote_dir>/foo/<子目录名>.zip` + 根目录散落文件 | 仅约**最大单个一级子目录**体积 | 源目录很大、Colab 本地磁盘较紧，或希望按模块/角色单独下载解压 |
| `direct` | `<remote_dir>/foo/<原目录结构>` 原样逐文件上传 | 0（不打包） | 少量大文件、无需打包，或需要在交大云盘网页端直接在线预览图片/文档 |

> **关于 `--archive-format`**：
> - `zip`（默认）：使用 `ZIP_STORED` 无压缩模式，Windows / macOS 双击即可解压，支持 ZIP64 与 UTF-8 中文文件名，且每个文件带有 CRC32 校验。
> - `tar`：使用 POSIX PAX 格式无压缩归档，适合主要在 Linux 端通过 `tar -xf` 解包的场景。

---

## 完整 CLI 参数参考

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--src` | *（必填）* | 本地或 Google Drive 源路径（文件或目录） |
| `--remote-dir` | `/colab_transfer` | 交大云盘目标目录 |
| `--pack-mode` | `single` | 打包模式：`single`（整包单文件）/ `subdir`（一级子目录独立打包）/ `direct`（直接逐文件） |
| `--archive-format` | `zip` | 无压缩归档格式：`zip` / `tar` |
| `--auth-mode` | `UserToken` | 认证方式：`UserToken` / `JAAuthCookie` |
| `--token` | `None` | 直接传入 `UserToken`（也支持环境变量 `JBOX_USER_TOKEN`） |
| `--ja-cookie` | `None` | 直接传入 `JAAuthCookie`（也支持环境变量 `JBOX_JA_COOKIE`；仅传此项时自动切换为 `JAAuthCookie` 模式） |
| `--workers` | `4` | 并发上传分片线程数 |
| `--chunk-mb` | `4` | 分片大小（MiB）；单文件分片若超过 `10000` 个，会自动按 `4 MiB` 步进增大分片尺寸 |
| `--conflict` | `overwrite` | 远端同名冲突策略：`overwrite`（覆盖）/ `rename`（自动重命名）/ `ask`（冲突报错） |
| `--work-dir` | `/content/.jbox_transfer` | 本地临时归档与断点元数据目录 |
| `--force-repack` | `False` | 忽略本地已缓存的归档，强制重新打包 |
| `--force-upload` | `False` | 忽略远端「同名同大小跳过」检查，强制重新上传 |
| `--keep-temp` | `False` | 上传并校验完成后保留本地临时归档文件 |
| `--speedtest` | `False` | 正式传输前先上传并清理一个 10 MiB 随机文件以测试速率 |
| `--dry-run` | `False` | 仅扫描源目录并输出目标路径与预计体积，不要求凭证也不上传 |

---

## 断点续传、缓存复用与安全机制

1. **临时与断点目录结构**（默认 `--work-dir /content/.jbox_transfer`）：
   - `archives/`：存放打包生成的 `.zip`/`.tar` 及对应 `.pack_meta.json`（记录源路径、文件数、源总字节、最大修改时间以及归档自身大小与 mtime）。
   - `checkpoints/`：按归一化远端路径 SHA-256 前 16 位命名的断点 JSON（记录 `confirm_key`、分片布局与已完成分片号）。
2. **中断后如何续传**：
   - 上传中途因网络中断、Colab 断连或手动停止时，本地归档与断点 JSON 会自动保留。
   - **直接重新运行相同命令/Notebook** 即可：
     1. 源文件未变化时自动复用 `archives/` 中已有归档（跳过重新打包）；
     2. 命中匹配的 `checkpoints/` 记录后，调用交大云盘 `?upload` 接口核对服务端已收到的分片列表，跳过已完成分片，仅补传剩余分片；
     3. 全部上传、合并并校验远端字节一致后，自动删除断点文件与临时归档（除非指定 `--keep-temp`）。
   - 若希望即使 Colab 运行时彻底销毁也能保留打包/断点进度，可将 `--work-dir` 指向 Google Drive 下的某个临时目录（注意这会消耗 Drive 空间且读写速度低于 `/content` 本地盘）。
3. **凭证安全**：所有断点文件、打包元数据及标准输出中均**不包含** `UserToken`、`JAAuthCookie` 或 `accessToken`。

---

## 常见问题排查（FAQ）

**Q1：为什么重新运行后提示「远端已存在同名同大小文件，跳过」？**
- 默认情况下，只要远端目标路径已存在同名文件且字节数完全一致，脚本会认为此前已成功传完而自动跳过，以支持增量/多文件批处理。
- 如内容有变但字节数恰好相同或需要强制覆盖，请加上 `--force-upload`（Notebook 中勾选 `FORCE_UPLOAD`）。

**Q2：`conflict` 的三个选项 `overwrite` / `rename` / `ask` 有什么区别？**
- `overwrite`（默认）：配合 `--force-upload` 或新建上传完成合并时，覆盖远端同名文件。
- `rename`：当远端存在同名文件时，由服务端自动在文件名后追加编号保留多份。
- `ask`：若远端已存在同名文件，合并确认阶段会直接报冲突错误而不覆盖。

**Q3：提示本地临时空间不足怎么办？**
- 打包前脚本会预留 `源目录体积 × 105% + 64 MiB` 的空间检查。若单个目录太大导致 `/content` 本地盘装不下整包，请切换为 `--pack-mode subdir`（逐个一级子目录打包上传并立即删除临时包，峰值只需最大单个子目录的体积）或 `--pack-mode direct`。

**Q4：提示交大云盘空间配额不足或上传中途报会话过期？**
- 登录校验阶段会立即对比 `availableSpace` 与预计上传体积，配额不足会提前中止，避免白跑打包。
- 若断点暂停时间过久导致云端 `confirmKey` 失效，脚本会捕获会话过期错误并自动清理旧断点、从第 1 片重新建立会话上传，无需人工干预删除文件。
