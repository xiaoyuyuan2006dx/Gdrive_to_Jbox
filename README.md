# 简要介绍
将 Google Drive （谷歌云盘）的文件传输到 Jbox（交大云盘）。避免直链下载浪费流量。已有学长/学姐做过类似工具https://github.com/1357310795/TboxWebdav

# 简要使用方法
**前提：务必先要挂载 Google Drive，Make sure to mount Google Drive first**

**请正确获取Jaccount的token**

> 具体使用可见 README_gdrive_to_jbox.md

将文件放在正确位置
两个文件都同级位于 Google Drive `/content/drive/MyDrive/Colab Notebooks/`：

| 文件 | 用途 |
| :--- | :--- |
| `gdrive_to_jbox.py` | 核心独立工具脚本（支持 CLI 命令行运行，也支持作为 Python 模块导入调用 `transfer()`） |
| `colab_transfer.ipynb` | 配套 Colab 交互式表单 Notebook，直接调用 `gdrive_to_jbox.py`的

## 配置`colab_transfer.ipynb`
在第二个代码块中配置路径：
```
SRC_PATH = "/content/drive/MyDrive/gakumas_assets" #@param {type:"string"}
REMOTE_DIR = "/colab_transfer" #@param {type:"string"}
```
运行第三个代码块：
运行然后交互式填写token即可

---

> 2026.11.22 py更新为v2。修改了py打包逻辑为多线程，提升打包速率
> 2026.11.28 py更新为v3：优化跨国高延迟上传吞吐（持久化线程池复用 HTTPS 长连接、滑动窗口后台预签分片、节流断点落盘、默认并发 16 / 分片 16 MiB），并同步更新 `colab_transfer.ipynb`