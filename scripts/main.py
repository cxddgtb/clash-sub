#!/usr/bin/env python3
"""
Clash 节点爬取 → 测试筛选 → 去重归档 → 输出订阅
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import socket
import ssl
import time
import yaml
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, quote_plus

import aiohttp
import async_timeout

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("harvest")

# ── Config from env ─────────────────────────────────────
ARCHIVE_DIR    = Path(os.getenv("ARCHIVE_DIR", "archives"))
OUTPUT_DIR     = Path(os.getenv("OUTPUT_DIR", "output"))
MAX_ARCHIVE    = int(os.getenv("MAX_ARCHIVE_FILES", "5"))
TEST_TIMEOUT   = int(os.getenv("TEST_TIMEOUT", "8"))      # 每个节点测试超时秒数
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "80"))   # 并发测试数
REPO_BRANCH    = os.getenv("GITHUB_REF_NAME", "main")
REPO_OWNER     = os.getenv("GITHUB_REPOSITORY_OWNER", "qoder")
REPO_NAME      = os.getenv("GITHUB_REPOSITORY", "qoder/clash-sub").split("/")[-1]
MAX_OUTPUT_NODES = 500                                    # 最大输出节点数

# ── 爬取源配置 ──────────────────────────────────
# 默认节点源（作为后备）
DEFAULT_SOURCES = [
    "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt",
    "https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md",
    "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/clash.yml",
    "https://raw.githubusercontent.com/aiboboxx/clashfree/main/clash.yml",
    "https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub",
    "https://sub.794789.xyz/sub",
    "https://clashnode.com/wp-content/uploads/2024/01/20240117.txt",
    "https://raw.githubusercontent.com/ripaojiedian/freenode/main/clash",
    "https://raw.githubusercontent.com/alanbobsun/TinyV2ray/main/clash_sub.yml",
]

# 尝试从 GitHub Secrets (环境变量 SUB_SOURCES) 读取，支持多行文本
raw_sources = os.getenv("SUB_SOURCES", "")
if raw_sources.strip():
    # 按换行符分割，并过滤掉空行
    SOURCES = [url.strip() for url in raw_sources.strip().splitlines() if url.strip()]
    log.info("✅ 成功从 Secret 加载 %d 个自定义订阅源。", len(SOURCES))
else:
    SOURCES = DEFAULT_SOURCES
    log.info("⚠️ 未检测到 SUB_SOURCES Secret，使用默认的 %d 个订阅源。", len(SOURCES))

# ── 代理协议 pattern ────────────────────────────────────
PROXY_URI_RE = re.compile(
    r"(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|socks5|http)://\S+",
    re.IGNORECASE,
)

IP_PORT_RE = re.compile(
    r'\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})\b'
)

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# Data model
# ══════════════════════════════════════════════════════════

@dataclass
class Node:
    uri: str
    protocol: str
    name: str
    server: str
    port: int
    latency: Optional[int] = None
    source: str = ""
    first_seen: str = ""
    last_seen: str = ""

    @property
    def fingerprint(self) -> str:
        raw = f"{self.protocol}://{self.server}:{self.port}#{self.name}"
        return hashlib.sha256(raw.encode()).hexdigest()

    @property
    def alive(self) -> bool:
        return self.latency is not None and self.latency < 5000

# ══════════════════════════════════════════════════════════
# 爬取
# ══════════════════════════════════════════════════════════

async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with async_timeout.timeout(25):
            async with session.get(url, headers={"User-Agent": "clash-sub/1.0"}) as resp:
                if resp.status != 200:
                    log.warning("HTTP %d %s", resp.status, url)
                    return ""
                raw = await resp.text(encoding="utf-8", errors="replace")
    except Exception:
        log.warning("Fetch failed: %s", url)
        return ""

    stripped = raw.strip()
    for _ in range(2):
        try:
            decoded = base64.b64decode(stripped, validate=True).decode("utf-8", errors="replace")
            if any(proto in decoded for proto in ("vmess://", "vless://", "trojan://", "ss://")):
                stripped = decoded.strip()
                continue
        except Exception:
            pass
        break
    return stripped

def parse_vmess_uri(uri: str) -> Optional[dict]:
    try:
        b64 = uri[len("vmess://"):]
        b64 += "=" * (4 - len(b64) % 4)
        j = json.loads(base64.b64decode(b64))
        return j
    except Exception:
        return None

EXTRACTORS = {
    "vmess": lambda u: parse_vmess_uri(u),
    "vless": lambda u: _extract_generic(u, "vless"),
    "trojan": lambda u: _extract_generic(u, "trojan"),
    "ss": lambda u: _extract_ss(u),
    "ssr": lambda u: _extract_ss(u),
    "hysteria2": lambda u: _extract_generic(u, "hysteria2"),
    "hy2": lambda u: _extract_generic(u, "hy2"),
    "tuic": lambda u: _extract_generic(u, "tuic"),
    "hysteria": lambda u: _extract_generic(u, "hysteria"),
}

def _extract_generic(uri: str, proto: str) -> Optional[dict]:
    try:
        u = urlparse(uri)
        name = quote_plus(u.fragment or "")
        return {"name": name, "server": u.hostname or "", "port": u.port or 443}
    except Exception:
        return None

def _extract_ss(uri: str) -> Optional[dict]:
    try:
        body = uri[len("ss://"):]
        if "@" in body:
            u = urlparse(uri)
            return {"name": quote_plus(u.fragment or ""), "server": u.hostname or "", "port": u.port or 8388}
        b64 = body.split("#")[0]
        b64 += "=" * (4 - len(b64) % 4)
        decoded = base64.b64decode(b64).decode()
        method_pw, server_part = decoded.rsplit("@", 1)
        host, port = server_part.rsplit(":", 1)
        name = body.split("#", 1)[1] if "#" in body else ""
        return {"name": quote_plus(name), "server": host, "port": int(port)}
    except Exception:
        return None

def parse_node(uri: str, source: str) -> Optional[Node]:
    proto = uri.split("://", 1)[0].lower()
    extract = EXTRACTORS.get(proto)
    if not extract:
        return None
    info = extract(uri)
    if not info or not info.get("server"):
        return None
    name = info.get("name", info.get("ps", ""))
    if not name:
        name = f"{info['server']}:{info.get('port', '?')}"
    return Node(
        uri=uri.strip(),
        protocol=proto,
        name=name,
        server=info["server"],
        port=int(info.get("port", 443)),
        source=source,
        first_seen=datetime.now(timezone.utc).isoformat(),
        last_seen=datetime.now(timezone.utc).isoformat(),
    )

async def crawl_sources(session: aiohttp.ClientSession) -> list[Node]:
    tasks = [fetch_text(session, url) for url in SOURCES]
    texts = await asyncio.gather(*tasks)

    nodes: dict[str, Node] = {}
    for url, text in zip(SOURCES, texts):
        if not text:
            continue
        log.info("Crawled %s → %d chars", url, len(text))

        uris = PROXY_URI_RE.findall(text)
        for uri in uris:
            n = parse_node(uri, source=url)
            if n:
                nodes[n.fingerprint] = n

        try:
            y = yaml.safe_load(text)
            if isinstance(y, dict):
                for proxy in y.get("proxies", []):
                    if not isinstance(proxy, dict):
                        continue
                    _add_yaml_proxy(proxy, url, nodes)
        except Exception:
            pass

    log.info("Total unique nodes crawled: %d", len(nodes))
    return list(nodes.values())

def _add_yaml_proxy(proxy: dict, source: str, nodes: dict[str, Node]):
    proto = (proxy.get("type") or "").lower()
    server = proxy.get("server", "")
    port = proxy.get("port", 443)
    name = proxy.get("name", f"{server}:{port}")
    if not server or proto not in EXTRACTORS:
        return

    if proto == "vmess":
        j = {
            "v": "2", "ps": name, "add": server, "port": str(port),
            "id": proxy.get("uuid", ""), "aid": proxy.get("alterId", "0"),
            "net": proxy.get("network", "tcp"), "type": proxy.get("cipher", "none"),
            "host": proxy.get("ws-opts", {}).get("headers", {}).get("Host", ""),
            "path": proxy.get("ws-opts", {}).get("path", "/"),
            "tls": proxy.get("tls", ""),
        }
        b64 = base64.b64encode(json.dumps(j, separators=(",", ":")).encode()).decode()
        uri = f"vmess://{b64}"
    elif proto == "ss":
        uri = f"ss://{proxy.get('cipher','aes-256-gcm')}:{proxy.get('password','')}@{server}:{port}#{quote_plus(name)}"
    elif proto == "trojan":
        uri = f"trojan://{proxy.get('password','')}@{server}:{port}?sni={proxy.get('sni','')}#{quote_plus(name)}"
    elif proto == "vless":
        uri = f"vless://{proxy.get('uuid','')}@{server}:{port}?type={proxy.get('network','tcp')}&security={proxy.get('tls','')}#{quote_plus(name)}"
    else:
        uri = f"{proto}://{proxy.get('password','')}@{server}:{port}#{quote_plus(name)}"

    n = Node(
        uri=uri, protocol=proto, name=name, server=server,
        port=int(port), source=source,
        first_seen=datetime.now(timezone.utc).isoformat(),
        last_seen=datetime.now(timezone.utc).isoformat(),
    )
    nodes[n.fingerprint] = n

# ══════════════════════════════════════════════════════════
# 测试: TCP 连通性 + 延迟
# ══════════════════════════════════════════════════════════

async def test_one(node: Node, sem: asyncio.Semaphore) -> None:
    async with sem:
        try:
            t0 = time.monotonic()
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(node.server, node.port),
                timeout=TEST_TIMEOUT,
            )
            latency = int((time.monotonic() - t0) * 1000)
            writer.close()
            await writer.wait_closed()
            node.latency = latency
        except Exception:
            node.latency = None

async def test_nodes(nodes: list[Node]) -> list[Node]:
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    log.info("Testing %d nodes (concurrency=%d, timeout=%ds) ...", len(nodes), MAX_CONCURRENT, TEST_TIMEOUT)
    t0 = time.monotonic()
    await asyncio.gather(*(test_one(n, sem) for n in nodes))
    elapsed = time.monotonic() - t0
    alive = [n for n in nodes if n.alive]
    log.info("Test done in %.1fs — %d/%d alive", elapsed, len(alive), len(nodes))
    return alive

# ══════════════════════════════════════════════════════════
# 存档系统
# ══════════════════════════════════════════════════════════

def load_archives() -> dict[str, Node]:
    merged: dict[str, Node] = {}
    files = sorted(ARCHIVE_DIR.glob("nodes_*.json"), reverse=True)
    for fp in files[:MAX_ARCHIVE]:
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
            for item in data:
                fp_key = item["fingerprint"]
                if fp_key not in merged:
                    merged[fp_key] = Node(**item)
        except Exception:
            log.warning("Corrupted archive: %s", fp)
    log.info("Loaded %d unique nodes from %d archive(s)", len(merged), min(len(files), MAX_ARCHIVE))
    return merged

def save_archive(nodes: list[Node]) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = ARCHIVE_DIR / f"nodes_{ts}.json"
    data = [asdict(n) for n in nodes]
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Archive saved: %s (%d nodes)", path, len(nodes))
    return path

def prune_archives():
    files = sorted(ARCHIVE_DIR.glob("nodes_*.json"), reverse=True)
    for fp in files[MAX_ARCHIVE:]:
        fp.unlink()
        log.info("Pruned old archive: %s", fp)

# ══════════════════════════════════════════════════════════
# 输出：Clash Meta / Mihomo 订阅配置
# ══════════════════════════════════════════════════════════

CLASH_TEMPLATE = """# ═══ Clash Meta 自动订阅 ═══
# 更新时间: {datetime}
# 节点总数: {total} (本次存活: {alive})
# 输出节点数: {output_count} (延迟优先)
# 存档去重覆盖天数: ~{days}d

mixed-port: 7890
allow-lan: true
bind-address: '*'
mode: rule
log-level: info
external-controller: '0.0.0.0:9090'
ipv6: true
unified-delay: true
tcp-concurrent: true
geodata-mode: true
geox-url:
  geoip: "https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/geoip.dat"
  geosite: "https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/geosite.dat"
  mmdb: "https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/country.mmdb"

profile:
  store-selected: true
  store-fake-ip: true

# ── DNS ──
dns:
  enable: true
  ipv6: true
  listen: 0.0.0.0:53
  enhanced-mode: fake-ip
  fake-ip-range: 198.18.0.1/16
  fake-ip-filter:
    - '*.lan'
    - '*.local'
    - '*.localhost'
    - '*.home.arpa'
    - time.*.com
    - time.*.apple.com
    - swscan.apple.com
    - mesu.apple.com
    - gspe*.*.apple.com
    - '*.msftconnecttest.com'
    - '*.msftncsi.com'
    - '*.stun.*'
    - '+.stun.*.*'
    - '+.stun.*.*.*'
    - '+.stun.*.*.*.*'
  default-nameserver:
    - 223.5.5.5
    - 119.29.29.29
    - 114.114.114.114
  nameserver:
    - https://doh.pub/dns-query
    - https://dns.alidns.com/dns-query
    - https://doh.360.cn/dns-query
  fallback:
    - https://1.0.0.1/dns-query
    - https://8.8.4.4/dns-query
    - tls://dns.google:853
  fallback-filter:
    geoip: true
    geoip-code: CN

# ── 代理组 ──
proxy-groups:
  - name: "🚀 自动选择"
    type: url-test
    proxies:
      {proxy_names}
    url: 'https://www.gstatic.com/generate_204'
    interval: 300
    tolerance: 50

  - name: "🌍 手动切换"
    type: select
    proxies:
      - "🚀 自动选择"
      - DIRECT
      {proxy_names_select}

  - name: "📺 流媒体"
    type: select
    proxies:
      - "🚀 自动选择"
      - "🌍 手动切换"
      - DIRECT

  - name: "🎯 全球直连"
    type: select
    proxies:
      - DIRECT
      - "🚀 自动选择"

  - name: "🛡️ 广告拦截"
    type: select
    proxies:
      - REJECT
      - DIRECT

  - name: "🐟 漏网之鱼"
    type: select
    proxies:
      - "🚀 自动选择"
      - DIRECT

# ── 规则 ──
rules:
  # 广告
  - DOMAIN-KEYWORD,admarvel,🛡️ 广告拦截
  - DOMAIN-KEYWORD,admaster,🛡️ 广告拦截
  - DOMAIN-KEYWORD,adsage,🛡️ 广告拦截
  - DOMAIN-KEYWORD,adsmogo,🛡️ 广告拦截
  - DOMAIN-KEYWORD,adsrvmedia,🛡️ 广告拦截
  - DOMAIN-KEYWORD,adwords,🛡️ 广告拦截
  - DOMAIN-KEYWORD,adservice,🛡️ 广告拦截
  - DOMAIN-SUFFIX,ads.google.com,🛡️ 广告拦截
  - DOMAIN-SUFFIX,doubleclick.net,🛡️ 广告拦截
  - GEOSITE,category-ads-all,🛡️ 广告拦截

  # 国内直连
  - GEOIP,CN,🎯 全球直连
  - GEOSITE,CN,🎯 全球直连
  - GEOSITE,category-games@cn,🎯 全球直连
  - DOMAIN-SUFFIX,cn,🎯 全球直连
  - DOMAIN-SUFFIX,baidu.com,🎯 全球直连
  - DOMAIN-SUFFIX,bilibili.com,🎯 全球直连
  - DOMAIN-SUFFIX,weixin.qq.com,🎯 全球直连
  - DOMAIN-SUFFIX,qcloud.com,🎯 全球直连
  - DOMAIN-SUFFIX,myqcloud.com,🎯 全球直连
  - DOMAIN-SUFFIX,alipay.com,🎯 全球直连
  - DOMAIN-SUFFIX,alipayobjects.com,🎯 全球直连

  # AI / Copilot
  - DOMAIN-SUFFIX,openai.com,🚀 自动选择
  - DOMAIN-SUFFIX,chatgpt.com,🚀 自动选择
  - DOMAIN-SUFFIX,oaistatic.com,🚀 自动选择
  - DOMAIN-SUFFIX,oaiusercontent.com,🚀 自动选择
  - DOMAIN-SUFFIX,claude.ai,🚀 自动选择
  - DOMAIN-SUFFIX,anthropic.com,🚀 自动选择
  - DOMAIN-SUFFIX,githubcopilot.com,🚀 自动选择
  - DOMAIN-SUFFIX,bing.com,🚀 自动选择
  - DOMAIN-KEYWORD,openai,🚀 自动选择

  # Google
  - DOMAIN-SUFFIX,google.com,🚀 自动选择
  - DOMAIN-SUFFIX,googleapis.com,🚀 自动选择
  - DOMAIN-SUFFIX,googleusercontent.com,🚀 自动选择
  - DOMAIN-SUFFIX,gstatic.com,🚀 自动选择
  - DOMAIN-SUFFIX,googlevideo.com,🚀 自动选择
  - DOMAIN-SUFFIX,googlemail.com,🚀 自动选择
  - DOMAIN-SUFFIX,gmail.com,🚀 自动选择
  - DOMAIN-SUFFIX,google.com.hk,🚀 自动选择
  - DOMAIN-SUFFIX,google.com.tw,🚀 自动选择

  # YouTube
  - DOMAIN-SUFFIX,youtube.com,🚀 自动选择
  - DOMAIN-SUFFIX,ytimg.com,🚀 自动选择
  - DOMAIN-SUFFIX,ggpht.com,🚀 自动选择
  - DOMAIN-SUFFIX,googlevideo.com,🚀 自动选择
  - DOMAIN-SUFFIX,youtu.be,🚀 自动选择
  - DOMAIN-KEYWORD,youtube,🚀 自动选择

  # Meta
  - DOMAIN-SUFFIX,facebook.com,🚀 自动选择
  - DOMAIN-SUFFIX,fbcdn.net,🚀 自动选择
  - DOMAIN-SUFFIX,instagram.com,🚀 自动选择
  - DOMAIN-SUFFIX,cdninstagram.com,🚀 自动选择
  - DOMAIN-SUFFIX,whatsapp.com,🚀 自动选择
  - DOMAIN-SUFFIX,whatsapp.net,🚀 自动选择
  - DOMAIN-SUFFIX,messenger.com,🚀 自动选择
  - DOMAIN-SUFFIX,threads.net,🚀 自动选择

  # X / Twitter
  - DOMAIN-SUFFIX,x.com,🚀 自动选择
  - DOMAIN-SUFFIX,twitter.com,🚀 自动选择
  - DOMAIN-SUFFIX,t.co,🚀 自动选择
  - DOMAIN-SUFFIX,twimg.com,🚀 自动选择

  # Telegram
  - DOMAIN-SUFFIX,telegram.org,🚀 自动选择
  - DOMAIN-SUFFIX,t.me,🚀 自动选择
  - DOMAIN-SUFFIX,tdesktop.com,🚀 自动选择
  - DOMAIN-SUFFIX,telegram.me,🚀 自动选择
  - IP-CIDR,91.108.56.0/22,🚀 自动选择
  - IP-CIDR,91.108.4.0/22,🚀 自动选择
  - IP-CIDR,91.108.8.0/22,🚀 自动选择
  - IP-CIDR,109.239.140.0/24,🚀 自动选择
  - IP-CIDR,149.154.160.0/20,🚀 自动选择

  # GitHub
  - DOMAIN-SUFFIX,github.com,🚀 自动选择
  - DOMAIN-SUFFIX,github.io,🚀 自动选择
  - DOMAIN-SUFFIX,githubassets.com,🚀 自动选择
  - DOMAIN-SUFFIX,githubusercontent.com,🚀 自动选择

  # Streaming
  - DOMAIN-SUFFIX,netflix.com,📺 流媒体
  - DOMAIN-SUFFIX,nflxvideo.net,📺 流媒体
  - DOMAIN-SUFFIX,nflximg.com,📺 流媒体
  - DOMAIN-SUFFIX,nflxext.com,📺 流媒体
  - DOMAIN-SUFFIX,disneyplus.com,📺 流媒体
  - DOMAIN-SUFFIX,dssott.com,📺 流媒体
  - DOMAIN-SUFFIX,hulu.com,📺 流媒体
  - DOMAIN-SUFFIX,hbomax.com,📺 流媒体
  - DOMAIN-SUFFIX,hbo.com,📺 流媒体
  - DOMAIN-SUFFIX,spotify.com,📺 流媒体
  - DOMAIN-SUFFIX,scdn.co,📺 流媒体
  - DOMAIN-SUFFIX,tidal.com,📺 流媒体
  - DOMAIN-SUFFIX,primevideo.com,📺 流媒体

  # Apple
  - DOMAIN-SUFFIX,apple.com,🎯 全球直连
  - DOMAIN-SUFFIX,icloud.com,🎯 全球直连
  - DOMAIN-SUFFIX,apple-dns.net,🎯 全球直连
  - DOMAIN-SUFFIX,mzstatic.com,🎯 全球直连

  # Microsoft
  - DOMAIN-SUFFIX,microsoft.com,🎯 全球直连
  - DOMAIN-SUFFIX,live.com,🎯 全球直连
  - DOMAIN-SUFFIX,office.com,🎯 全球直连
  - DOMAIN-SUFFIX,office.net,🎯 全球直连

  # LAN 局域网
  - IP-CIDR,10.0.0.0/8,🎯 全球直连
  - IP-CIDR,172.16.0.0/12,🎯 全球直连
  - IP-CIDR,192.168.0.0/16,🎯 全球直连
  - IP-CIDR,127.0.0.0/8,🎯 全球直连

  # Final
  - MATCH,🐟 漏网之鱼

# ── 代理节点 ──
proxies:
{yaml_nodes}
"""

def generate_subscription(nodes: list[Node], archive_count: int) -> str:
    # 只获取存活节点
    alive_nodes = [n for n in nodes if n.alive]
    
    # 存活节点按延迟从低到高排序
    alive_nodes.sort(key=lambda n: n.latency or 9999)
    
    # 只取前 500 个存活节点，不用死亡节点补足
    top_nodes = alive_nodes[:MAX_OUTPUT_NODES]

    # 自动选择组：使用这 500 个存活节点
    proxy_names_list = [f'- "{n.name}"' for n in top_nodes]
    if not proxy_names_list:
        proxy_names_list = ['- "DIRECT"']
    proxy_names = "\n      ".join(proxy_names_list)

    # 手动切换组：使用这 500 个存活节点
    select_list = [f'- "{n.name}"' for n in top_nodes]
    if not select_list:
        proxy_names_select = ""
    else:
        proxy_names_select = "\n      ".join(select_list)

    # YAML 节点列表：只输出这 500 个存活节点
    yaml_nodes_list = [_node_to_yaml(n) for n in top_nodes]
    yaml_block = "\n".join(yaml_nodes_list)

    return CLASH_TEMPLATE.format(
        datetime=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        total=len(nodes),
        alive=len(alive_nodes),
        output_count=len(top_nodes),
        days=archive_count,
        proxy_names=proxy_names,
        proxy_names_select=proxy_names_select,
        yaml_nodes=yaml_block,
    )

def _node_to_yaml(n: Node) -> str:
    latency_tag = f" [{n.latency}ms]" if n.alive else ""
    name = f"{n.name}{latency_tag}"

    if n.protocol == "vmess":
        info = parse_vmess_uri(n.uri) or {}
        return f"""  - name: "{name}"
    type: vmess
    server: {n.server}
    port: {n.port}
    uuid: {info.get('id','')}
    alterId: {int(info.get('aid',0))}
    cipher: {info.get('type','auto')}
    udp: true"""

    elif n.protocol == "vless":
        u = urlparse(n.uri)
        params = dict(p.split("=", 1) for p in u.query.split("&") if "=" in p)
        return f"""  - name: "{name}"
    type: vless
    server: {n.server}
    port: {n.port}
    uuid: {u.username or ''}
    udp: true
    tls: {params.get('security') == 'tls'}
    network: {params.get('type','tcp')}"""

    elif n.protocol == "trojan":
        u = urlparse(n.uri)
        return f"""  - name: "{name}"
    type: trojan
    server: {n.server}
    port: {n.port}
    password: "{u.username or ''}"
    udp: true"""

    elif n.protocol in ("ss", "ssr"):
        u = urlparse(n.uri)
        user = u.username or ""
        pw = u.password or ""
        if "@" not in n.uri[len("ss://"):]:
            try:
                b64 = n.uri.split("://")[1].split("#")[0]
                b64 += "=" * (4 - len(b64) % 4)
                decoded = base64.b64decode(b64).decode()
                method_pw = decoded.rsplit("@", 1)[0]
                user, pw = method_pw.split(":", 1)
            except:
                pass
        return f"""  - name: "{name}"
    type: ss
    server: {n.server}
    port: {n.port}
    cipher: {user or 'aes-256-gcm'}
    password: "{pw or ''}"
    udp: true"""

    elif n.protocol in ("hysteria2", "hy2", "hysteria"):
        u = urlparse(n.uri)
        return f"""  - name: "{name}"
    type: hysteria2
    server: {n.server}
    port: {n.port}
    password: "{u.username or ''}"
    up: "50 Mbps"
    down: "200 Mbps"
    sni: {u.hostname or n.server}
    skip-cert-verify: true"""

    elif n.protocol == "tuic":
        u = urlparse(n.uri)
        return f"""  - name: "{name}"
    type: tuic
    server: {n.server}
    port: {n.port}
    uuid: "{u.username or ''}"
    password: "{u.password or ''}"
    udp-relay-mode: native"""

    return f"""  - name: "{name}"
    type: {n.protocol}
    server: {n.server}
    port: {n.port}"""

# ══════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════

async def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    connector = aiohttp.TCPConnector(limit=50, limit_per_host=10, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        fresh = await crawl_sources(session)
        if not fresh:
            log.error("No nodes crawled — aborting.")
            return

        old = load_archives()
        merged: dict[str, Node] = {}
        for n in old.values():
            merged[n.fingerprint] = n
        for n in fresh:
            if n.fingerprint in merged:
                merged[n.fingerprint].last_seen = n.last_seen
                merged[n.fingerprint].source = n.source
            else:
                merged[n.fingerprint] = n
        all_nodes = list(merged.values())
        log.info("After merge: %d (fresh %d + old %d)", len(all_nodes), len(fresh), len(old))

        alive = await test_nodes(all_nodes)
        # 对存活节点按延迟从低到高排序
        alive.sort(key=lambda n: n.latency or 9999)

        save_archive(all_nodes)
        prune_archives()

        archive_count = len(list(ARCHIVE_DIR.glob("nodes_*.json")))
        yaml_out = generate_subscription(all_nodes, archive_count)
        (OUTPUT_DIR / "clash.yaml").write_text(yaml_out, encoding="utf-8")

        # 输出 Base64 和纯文本时，只取前 500 个存活节点
        uri_list = chr(10).join(n.uri for n in alive[:MAX_OUTPUT_NODES])
        b64_out = base64.b64encode(uri_list.encode()).decode()
        (OUTPUT_DIR / "sub.txt").write_text(b64_out, encoding="utf-8")
        (OUTPUT_DIR / "uris.txt").write_text(uri_list, encoding="utf-8")

        html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Clash 订阅</title>
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; background: #111; color: #eee; }}
a {{ color: #4fc3f7; }}
table {{ width:100%; border-collapse: collapse; margin-top: 1.5rem; }}
th, td {{ padding: 0.5rem 0.75rem; text-align: left; border-bottom: 1px solid #333; }}
th {{ color: #888; font-weight: 600; font-size: 0.85rem; }}
</style>
</head>
<body>
<h1>Clash 订阅链接</h1>
<p>更新时间: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC</p>
<ul>
<li><strong>Clash Meta 完整配置</strong> (推荐):
  <a href="clash.yaml">clash.yaml</a></li>
<li><strong>Base64 URI 列表</strong> (兼容旧客户端):
  <a href="sub.txt">sub.txt</a></li>
<li><strong>纯文本 URI 列表</strong>:
  <a href="uris.txt">uris.txt</a></li>
</ul>
<table>
<thead><tr><th>统计</th><th></th></tr></thead>
<tbody>
<tr><td>总节点</td><td>{len(all_nodes)}</td></tr>
<tr><td>存活节点</td><td>{len(alive)}</td></tr>
<tr><td>输出节点数</td><td>{len(alive[:MAX_OUTPUT_NODES])} (延迟优先)</td></tr>
<tr><td>存档数量</td><td>{archive_count}</td></tr>
</tbody></table>
</body>
</html>"""
        (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")

        log.info("Output written to %s/", OUTPUT_DIR)
        log.info("Subscription URL: https://%s.github.io/%s/clash.yaml", REPO_OWNER, REPO_NAME)

if __name__ == "__main__":
    asyncio.run(main())
