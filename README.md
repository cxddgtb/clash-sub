# Clash 免费节点自动订阅

每天自动从多个公开源爬取免费 Clash/V2Ray/Trojan 节点，TCP 连通性测试后去重归档，输出 Clash Meta 完整订阅配置。

## 使用

订阅链接（部署后）: `https://<你的用户名>.github.io/<仓库名>/clash.yaml`

## 功能

| 功能 | 说明 |
|------|------|
| 爬取 | 9 个公开免费节点源，支持 vmess/vless/trojan/ss/ssr/hysteria2/tuic |
| 测试 | TCP 连通性测试，记录延迟 |
| 去重 | 基于 name+server+port+protocol 的 SHA256 指纹 |
| 存档 | 保留最近 5 次运行结果，新旧合并 |
| 策略 | url-test 自动选择、手动切换、流媒体分流、广告拦截、国内直连 |
| 格式 | Clash Meta YAML + Base64 URI 列表 + 纯文本 URI |

## 部署

1. Fork 此仓库
2. 启用 GitHub Pages（Settings → Pages → Source: GitHub Actions）
3. 手动触发一次 workflow 或等待定时执行

## 定时

每天 UTC 2:00（北京时间 10:00）自动运行。
