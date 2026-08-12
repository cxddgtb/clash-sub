# 🚀 Clash 免费节点自动订阅 (Auto Clash Sub)

> 基于 GitHub Actions 的每日全自动节点收割机。每天自动从多个公开源及您的私人订阅中爬取节点，经过 TCP 连通性测试、SHA256 指纹去重、历史归档后，自动生成适配 **Clash Meta (Mihomo)** 的完整订阅配置。

---

## 🌟 核心特性

- **多源聚合**：内置 9 个优质公开免费节点源，同时支持通过 GitHub Secrets 无限添加您的私人/机场订阅源。
- **全协议支持**：完美解析 `vmess`, `vless`, `trojan`, `ss`, `ssr`, `hysteria2`, `tuic` 等主流协议。
- **真实测速**：摒弃虚假的 HTTP 测速，采用底层 TCP 连通性测试，记录真实延迟，自动剔除死节点。
- **智能去重**：基于 `协议+IP+端口+名称` 生成 SHA256 指纹，精准剔除重复节点。
- **历史归档**：保留最近 5 次的运行结果，新旧节点自动合并，确保订阅链接的稳定性（节点失效后仍能保留几天）。
- **开箱即用**：内置精心调优的 Clash Meta 规则（包含 AI 分流、流媒体、广告拦截、国内直连等），无需自己写规则。

---

## 🚀 快速部署指南（保姆级 3 步走）

无需服务器，完全免费，只需一个 GitHub 账号即可完成部署。

### 第一步：Fork 仓库
点击本页面右上角的 **Fork** 按钮，将此仓库克隆到你自己的 GitHub 账号下。

### 第二步：开启 GitHub Pages（⚠️ 最关键的一步）
*如果不做这一步，最后生成的网页和订阅链接将会是 404 错误！*
1. 进入你 Fork 后的仓库，点击顶部的 **Settings** (设置)。
2. 在左侧菜单栏找到并点击 **Pages**。
3. 找到 **Build and deployment** 区域。
4. 将 **Source** (来源) 的下拉菜单从 `Deploy from a branch` 更改为 **`GitHub Actions`**。

### 第三步：添加你的私人订阅源 (重点说明)
如果你想加入自己购买的机场订阅，或者特定的私有链接，请严格按照以下步骤配置：

1. 在仓库的 **Settings** (设置) 页面，点击左侧菜单的 **Secrets and variables** -> **Actions**。
2. 点击绿色的 **New repository secret** 按钮。
3. **Name** (名称) 必须严格输入：`SUB_SOURCES` （全大写，带下划线，不能有空格）。
4. **Secret** (内容) 输入你的订阅链接，**每行一个**。支持多种格式：
   - 机场 API 订阅链接 (如 `https://xxx.com/api/v1/client/subscribe?token=xxx`)
   - GitHub Raw 文本链接
   - 普通的 Base64 编码订阅链接
   - 纯文本节点列表链接
   
   **填写示例**：
   ```text
   https://你的机场.com/api/v1/client/subscribe?token=abcdefg
   https://raw.githubusercontent.com/用户名/仓库名/main/sub.txt
   https://别的免费节点网站.com/clash.yaml
   ```
5. 点击 **Add secret** 保存。
*(注：如果你不添加这个 Secret，项目也不会报错，而是默认使用内置的 9 个免费公开源。)*

### 第四步：手动触发第一次运行
1. 点击顶部的 **Actions** 菜单。
2. 左侧选择 **Daily Clash Node Harvest**。
3. 右侧点击 **Run workflow** -> 再次点击绿色的 **Run workflow** 按钮。
4. 等待约 2-3 分钟，直到左侧的运行状态变成绿色的 ✅。

---

## 🔗 获取你的专属订阅链接

运行成功后，你的专属订阅链接格式如下（请将 `<你的用户名>` 和 `<仓库名>` 替换为你自己的实际信息）：

| 格式类型 | 适用客户端 | 订阅链接 |
| :--- | :--- | :--- |
| **Clash Meta 完整配置** (推荐) | Clash Verge, Mihomo, Clash Meta for Android | `https://<你的用户名>.github.io/<仓库名>/clash.yaml` |
| **Base64 编码链接** | V2rayN, Shadowrocket, Quantumult X | `https://<你的用户名>.github.io/<仓库名>/sub.txt` |
| **纯文本 URI 列表** | 开发者调试 / 手动导入 | `https://<你的用户名>.github.io/<仓库名>/uris.txt` |

*(注：你也可以直接访问 `https://<你的用户名>.github.io/<仓库名>/` 查看可视化的统计网页，里面会显示当前存活的节点数量和延迟。)*

---

## 🛠️ 进阶配置与详细说明

### 1. 节点处理流水线
本项目的核心运行逻辑如下，确保了你拿到手的都是高质量节点：
1. **Fetch (爬取)**：并发请求 `SUB_SOURCES` 中的链接，支持自动解码 Base64 混淆的文本。
2. **Parse (解析)**：使用正则和 YAML 解析器，提取出所有合法的代理节点。
3. **Test (测速)**：使用异步协程 (`asyncio`)，以 80 的并发量对节点进行 TCP Ping 测试，超时时间为 8 秒。
4. **Merge (合并)**：将本次存活的节点与 `archives/` 目录下的历史 JSON 档案进行合并，保证节点的连贯性。
5. **Export (输出)**：生成 `clash.yaml` 并推送到 GitHub Pages。

### 2. 内置 Clash Meta 规则说明
生成的 `clash.yaml` 包含以下精心配置的代理组和规则：
- **🚀 自动选择**：`url-test` 模式，每 5 分钟自动测速，选择延迟最低的节点。
- **🌍 手动切换**：`select` 模式，列出前 50 个低延迟节点供手动选择。
- **📺 流媒体**：专门针对 Netflix, Disney+, YouTube, Spotify 等分流。
- **🤖 AI / Copilot**：一键直连或代理 OpenAI, Claude, GitHub Copilot 等服务。
- **🎯 全球直连**：基于 GeoIP 和 GeoSite 数据库，精准识别国内流量，直连不走代理。
- **🛡️ 广告拦截**：拦截常见的广告域名和追踪器。

### 3. 如何修改定时自动更新的时间？
项目默认在 **每天北京时间上午 10:00** (UTC 02:00) 自动运行。
如需修改，请编辑 `.github/workflows/daily.yml` 文件，找到以下代码：
```yaml
on:
  schedule:
    - cron: '0 2 * * *' # 这里使用的是 UTC 时间
```
*提示：GitHub Actions 的定时任务在高峰期可能会有 15-30 分钟的延迟，属于正常现象。*

---

## ❓ 常见问题解答 (FAQ)

**Q: 为什么 Actions 运行到最后一步 "Deploy to GitHub Pages" 报错 404？**
A: 这是因为你没有开启 GitHub Pages。请务必回到 **Settings -> Pages**，将 Source 设置为 **GitHub Actions**。设置后重新 Run workflow 即可解决。

**Q: 为什么生成的 `clash.yaml` 导入到 Clash 客户端提示“配置文件格式错误”？**
A: 早期版本存在 YAML 列表格式生成的 Bug，**当前最新版本已彻底修复此问题**。请确保你使用的是最新提交的代码，并重新运行一次 Actions。

**Q: 为什么测试下来存活的节点很少？**
A: 免费公开节点具有极强的时效性和不稳定性（通常存活时间只有几天甚至几小时）。建议您在 `SUB_SOURCES` 中添加自己稳定的私人机场订阅链接，以保证日常使用的稳定性。

**Q: 可以在本地电脑上运行这个脚本吗？**
A: 可以。请确保安装了 Python 3.9+，然后在终端执行：
```bash
pip install -r requirements.txt
python scripts/main.py
```
生成的文件将保存在 `output/` 目录下。

---

## ⚠️ 免责声明

1. 本项目仅用于**技术学习、研究及测试**，请勿用于任何非法用途。
2. 本项目本身**不提供任何节点资源**，所有节点均来自互联网上的公开分享或用户自行配置。
3. 使用本工具产生的任何网络问题、隐私泄露或违反当地法律法规的行为，**均由使用者自行承担**，本项目及作者不承担任何连带责任。
4. 请遵守您所在国家和地区的法律法规，支持正版，尊重知识产权。

---

**如果这个项目对你有帮助，欢迎点击右上角 ⭐ Star 支持一下！**
