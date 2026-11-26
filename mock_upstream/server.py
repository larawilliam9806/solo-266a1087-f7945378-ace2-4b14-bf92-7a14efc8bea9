"""可调答复延迟的本地 mock DNS 上游。

行为（仅支持 A 记录查询，演示用）：
- 域名以 "nx." 开头（例如 nx.example.test）：返回 NXDOMAIN，
  权威区带 SOA，SOA 的 minimum 字段作为否定缓存 TTL；
- 其他域名：返回 NOERROR + 1 条 A 记录，地址由上游标识和域名派生，
  便于在页面上区分是哪一个上游答的。

同时监听一个 HTTP 控制口：
- GET  /delay   读取当前答复延迟
- POST /delay   {"delay_seconds": 1.5} 修改答复延迟
- GET  /stats   查询计数
- POST /reset   清零计数
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dnslib import A, QTYPE, RCODE, RR, SOA, DNSRecord
from dnslib.server import BaseResolver, DNSServer

# ---------- 可调参数（环境变量） ----------
UPSTREAM_ID = os.getenv("UPSTREAM_ID", "upstream-a")
DNS_PORT = int(os.getenv("DNS_PORT", "53"))
CONTROL_PORT = int(os.getenv("CONTROL_PORT", "8080"))
A_TTL = int(os.getenv("A_TTL", "60"))
SOA_TTL = int(os.getenv("SOA_TTL", "30"))
SOA_MINIMUM = int(os.getenv("SOA_MINIMUM", "30"))
BIND_ADDR = os.getenv("BIND_ADDR", "0.0.0.0")

# 不同上游派生不同网段，页面上一眼能看出是谁答的
IP_BASE = int(os.getenv("IP_BASE", "10"))

_state_lock = threading.Lock()
_state = {
    "delay_seconds": float(os.getenv("INITIAL_DELAY", "0")),
    "queries": 0,
}


def _derived_ip(upstream_id: str, qname: str) -> str:
    """根据上游 id + 域名派生稳定的 A 记录地址。"""
    digest = hashlib.sha1(f"{upstream_id}:{qname}".encode()).digest()
    return f"{IP_BASE}.{digest[0]}.{digest[1]}.{digest[2]}"


class MockResolver(BaseResolver):
    def resolve(self, request: DNSRecord, handler) -> DNSRecord:
        reply = request.reply()
        q = request.q
        qname = str(q.qname).rstrip(".")
        qtype = QTYPE[q.qtype]

        with _state_lock:
            delay = _state["delay_seconds"]
            _state["queries"] += 1

        if delay > 0:
            # 每个查询在自己的线程里 sleep，不阻塞其他查询
            threading.Event().wait(delay)

        zone = "mock.internal"
        soa_rr = RR(
            zone,
            QTYPE.SOA,
            ttl=SOA_TTL,
            rdata=SOA(
                mname=f"ns1.{zone}",
                rname=f"admin.{zone}",
                times=(2026010100, 7200, 3600, 1209600, SOA_MINIMUM),
            ),
        )

        if qname.startswith("nx."):
            # NXDOMAIN：权威应答 + SOA，TTL 走 minimum
            reply.header.rcode = RCODE.NXDOMAIN
            reply.add_auth(soa_rr)
            return reply

        if qtype == "A":
            reply.add_answer(
                RR(qname, QTYPE.A, ttl=A_TTL, rdata=A(_derived_ip(UPSTREAM_ID, qname)))
            )
            return reply

        # 其他类型：NOERROR + SOA（NODATA）
        reply.add_auth(soa_rr)
        return reply


class ControlHandler(BaseHTTPRequestHandler):
    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/delay":
            with _state_lock:
                self._send_json(200, {"delay_seconds": _state["delay_seconds"]})
        elif self.path == "/stats":
            with _state_lock:
                self._send_json(200, dict(_state, upstream_id=UPSTREAM_ID))
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/delay":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                data = json.loads(self.rfile.read(length) or b"{}")
                delay = float(data["delay_seconds"])
                if delay < 0 or delay > 30:
                    raise ValueError("delay_seconds 需在 0~30 秒之间")
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self._send_json(400, {"error": f"请求参数错误: {exc}"})
                return
            with _state_lock:
                _state["delay_seconds"] = delay
            self._send_json(200, {"delay_seconds": delay})
        elif self.path == "/reset":
            with _state_lock:
                _state["queries"] = 0
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "not found"})

    def log_message(self, fmt, *args) -> None:  # 安静一点
        pass


def main() -> None:
    resolver = MockResolver()
    dns_server = DNSServer(
        resolver, port=DNS_PORT, address=BIND_ADDR, tcp=False
    )
    dns_server.start_thread()
    print(
        f"[{UPSTREAM_ID}] mock DNS on udp://{BIND_ADDR}:{DNS_PORT}, "
        f"control on http://{BIND_ADDR}:{CONTROL_PORT}",
        flush=True,
    )

    httpd = ThreadingHTTPServer((BIND_ADDR, CONTROL_PORT), ControlHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        dns_server.stop()


if __name__ == "__main__":
    main()
