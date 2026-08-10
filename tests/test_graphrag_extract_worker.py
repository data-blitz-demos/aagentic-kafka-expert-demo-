import importlib
import io
import json
import sys
from pathlib import Path


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
worker = importlib.import_module("graphrag_extract_worker")


class FakeChat:
    instances = []
    content = ""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.bind_kwargs = {}
        self.prompt = ""
        self.__class__.instances.append(self)

    def bind(self, **kwargs):
        self.bind_kwargs = kwargs
        return self

    def invoke(self, prompt):
        self.prompt = prompt
        return type("Response", (), {"content": self.__class__.content})()


def run_worker(monkeypatch, payload, *, api_key=""):
    FakeChat.instances = []
    monkeypatch.setattr(worker, "ChatOpenAI", FakeChat)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    if api_key:
        monkeypatch.setenv("GRAPH_RAG_WORKER_API_KEY", api_key)
    else:
        monkeypatch.delenv("GRAPH_RAG_WORKER_API_KEY", raising=False)
    assert worker.main() == 0
    return json.loads(output.getvalue()), FakeChat.instances[0]


def test_worker_builds_ollama_request(monkeypatch) -> None:
    FakeChat.content = '{"edges":[]}'
    result, chat = run_worker(
        monkeypatch,
        {
            "provider": "ollama",
            "model": "gemma3:4b",
            "temperature": 0.2,
            "base_url": "http://ollama:11434/v1/",
            "max_tokens": 99,
            "prompt": "extract",
        },
        api_key="ollama",
    )
    assert result == {"content": '{"edges":[]}'}
    assert chat.kwargs["reasoning_effort"] == "none"
    assert chat.kwargs["base_url"] == "http://ollama:11434/v1"
    assert chat.kwargs["api_key"] == "ollama"
    assert chat.bind_kwargs["max_tokens"] == 99
    assert chat.prompt == "extract"


def test_worker_serializes_non_string_hosted_response(monkeypatch) -> None:
    FakeChat.content = [{"text": "edge"}]
    result, chat = run_worker(
        monkeypatch,
        {"model": "gpt-test", "prompt": "extract"},
    )
    assert json.loads(result["content"]) == [{"text": "edge"}]
    assert chat.kwargs["api_key"] == "not-required"
    assert "base_url" not in chat.kwargs
    assert "reasoning_effort" not in chat.kwargs
    assert chat.bind_kwargs["max_tokens"] == 320
