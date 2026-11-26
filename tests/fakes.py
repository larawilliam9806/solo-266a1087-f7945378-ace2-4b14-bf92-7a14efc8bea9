"""测试用假 DNS 上游：可配置延迟、可按查询计数切换答复。"""
from __future__ import annotations

import socket
import threading
from dataclasses import dataclass

from dnslib import A, QTYPE, RCODE, RR, DNSRecord, SOA


@dataclass
class FakeUpstream:
    host: str
    port: int
    ip_base: int
    soa_minimum: int = 30
    delay: float = 0.0

    def __post_init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.settimeout(0.2)
        self._stop = threading.Event()
        self.queries: list[str] = []
        self._lock = threading.Lock()
        # 第 n 次查询（1-based）返回 NXDOMAIN，其余返回 A；用于精确控制
        self.nx_on_calls: set[int] = set()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while not self._stop.is_set():
            try:
                data, addr = self._sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            req = DNSRecord.parse(data)
            qname = str(req.q.qname).rstrip(".")
            if self.delay:
                self._stop.wait(self.delay)
                if self._stop.is_set():
                    return
            with self._lock:
                self.queries.append(qname)
                call_no = len(self.queries)
            reply = req.reply()
            if call_no in self.nx_on_calls:
                reply.header.rcode = RCODE.NXDOMAIN
                reply.add_auth(
                    RR(
                        "mock.internal",
                        QTYPE.SOA,
                        ttl=self.soa_minimum,
                        rdata=SOA(
                            "ns1.mock.internal",
                            "admin.mock.internal",
                            (1, 7200, 3600, 1209600, self.soa_minimum),
                        ),
                    )
                )
            else:
                reply.add_answer(
                    RR(qname, QTYPE.A, ttl=60, rdata=A(f"{self.ip_base}.0.0.{call_no}"))
                )
            self._sock.sendto(reply.pack(), addr)

    @property
    def query_count(self) -> int:
        with self._lock:
            return len(self.queries)

    def stop(self):
        self._stop.set()
        self._sock.close()
        self._thread.join(timeout=2)
