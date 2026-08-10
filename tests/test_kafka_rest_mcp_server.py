import importlib
import io
import json
import sys
from pathlib import Path


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
rest = importlib.import_module("kafka_rest_mcp_server")


class FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


def test_topic_metadata_uses_encoded_topic_and_returns_partitions(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, request.method, timeout))
        return FakeResponse({"partitions": []} if request.full_url.endswith("/partitions") else {"name": "orders"})

    monkeypatch.setattr(rest.urllib.request, "urlopen", fake_urlopen)
    result = rest.kafka_rest_topic_metadata("orders/eu")

    assert result["topic"] == "orders/eu"
    assert calls[0][0].endswith("/topics/orders%2Feu")
    assert calls[1][0].endswith("/topics/orders%2Feu/partitions")


def test_produce_json_posts_rest_proxy_record(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["method"] = request.method
        captured["content_type"] = request.headers["Content-type"]
        captured["body"] = json.loads(request.data.decode())
        return FakeResponse({"offsets": [{"partition": 0, "offset": 7}]})

    monkeypatch.setattr(rest.urllib.request, "urlopen", fake_urlopen)
    result = rest.kafka_rest_produce_json("orders", '{"id": 7}', key="k7")

    assert result["result"]["offsets"][0]["offset"] == 7
    assert captured["method"] == "POST"
    assert captured["content_type"] == "application/vnd.kafka.json.v2+json"
    assert captured["body"] == {"records": [{"value": {"id": 7}, "key": "k7"}]}
