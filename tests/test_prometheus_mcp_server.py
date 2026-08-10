import importlib
import sys
from pathlib import Path


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
prometheus = importlib.import_module("prometheus_mcp_server")


def test_cluster_snapshot_returns_all_named_results_when_one_query_fails(monkeypatch) -> None:
    def fake_prom_get(_path, params):
        query = params["query"]
        if query == 'up{job="kafka_jmx"}':
            raise TimeoutError("slow Prometheus query")
        return {"raw_json": '{"data":{"result":[]}}', "query": query}

    monkeypatch.setattr(prometheus, "_prom_get", fake_prom_get)
    snapshot = prometheus.kafka_cluster_state_snapshot()

    assert len(snapshot["queries"]) >= 20
    assert set(snapshot["results"]) == set(snapshot["queries"])
    assert "error" in snapshot["results"]["prometheus_target_up"]
    assert "raw_json" in snapshot["results"]["node_exporter_target_up"]
