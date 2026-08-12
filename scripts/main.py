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
from datetime import datetime, timedelta
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
REPO_BRANCH    = os.getenv("GITHUB_REF_NAME", "main")     # 用于生成 raw 链接
REPO_OWNER     = os.getenv("GITHUB_REPOSITORY_OWNER", "qoder")
REPO_NAME      = os.getenv("GITHUB_REPOSITORY", "qoder/clash-sub").split("/")[-1]

# ── 爬取源 ──────────────────────────────────────────────
# 格式: URL → 解析策略
SOURCES = [
    # 直接返回 base64 编码的 Clash/v2ray 订阅
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

# ── 代理协议 pattern ────────────────────────────────────
# 匹配 vmess://, vless://, trojan://, ss://, ssr://, hysteria2://, tuic://, hy2://
PROXY_URI_RE = re.compile(
    r'(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|socks5|http)://[^\s\'"<>\[\]{}|\\^`]+',
    re.IGNORECASE,
)

# 通用 IP:端口 匹配（用于某些 raw 文本）
IP_PORT_RE = re.compile(
    r'\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})\b'
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════
# Data model
# ══════════════════════════════════════════════════════════

@dataclass
class Node:
    uri: str              # 原始 URI
    protocol: str         # vmess / vless / trojan / ss / ssr / hysteria2 / tuic …
    name: str             # 节点名称 (ps / remarks / name)
    server: str           # host 或 IP
    port: int
    latency: Optional[int] = None   # ms, None = 未测试/不通
    source: str = ""                # 来源 URL
    first_seen: str = ""            # ISO datetime
    last_seen: str = ""

    @property
    def fingerprint(self) -> str:
        """基于 name+server+port+protocol 的去重指纹"""
        raw = f"{self.protocol}://{self.server}:{self.port}#{self.name}"
        return hashlib.sha256(raw.encode()).hexdigest()

    @property
    def alive(self) -> bool:
        return self.latency is not None and self.latency < 5000


# ══════════════════════════════════════════════════════════
# 爬取：从各源拉文本，提取 proxy URI
# ══════════════════════════════════════════════════════════

async def fetch_text(session: aiohttp.ClientSession
