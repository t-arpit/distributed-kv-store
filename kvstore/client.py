"""Tiny client for the load balancer, reusing one keep-alive connection."""
import http.client
import json
from urllib.parse import quote, urlparse


class KVClient:
    def __init__(self, url="http://127.0.0.1:8000", timeout=5.0):
        u = urlparse(url)
        self.host, self.port, self.timeout = u.hostname, u.port, timeout
        self._conn = None

    def _request(self, method, path, body=None):
        for attempt in (1, 2):
            if self._conn is None:
                self._conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            try:
                self._conn.request(method, path, body=body)
                r = self._conn.getresponse()
                return r.status, json.loads(r.read() or b"null")
            except (OSError, http.client.HTTPException):
                self._conn.close()
                self._conn = None
                if attempt == 2:
                    raise

    def put(self, key: str, value: str) -> bool:
        status, _ = self._request("PUT", "/kv/" + quote(key, safe=""), value.encode())
        return status == 200

    def get(self, key: str):
        status, body = self._request("GET", "/kv/" + quote(key, safe=""))
        return body["value"] if status == 200 else None

    def cluster(self):
        return self._request("GET", "/cluster")[1]

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
