import importlib
import sys
from pathlib import Path


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
utils = importlib.import_module("graph_ingest_utils")


def test_fallback_edges_extracts_bounded_deduplicated_kafka_relationships() -> None:
    edges = utils.fallback_graph_edges(
        "Kafka brokers store topic partitions and replicas.\n"
        "A topic partition has a leader and followers.\n"
        "Kafka brokers store topic partitions again.",
        max_edges=3,
    )

    assert len(edges) == 3
    assert edges[0] == {
        "source": "Kafka",
        "relation": "relates_to",
        "target": "Broker",
        "evidence": "Kafka brokers store topic partitions and replicas.",
    }
    assert len({(edge["source"], edge["target"]) for edge in edges}) == 3


def test_fallback_edges_handles_empty_single_concept_and_minimum_limit() -> None:
    assert utils.fallback_graph_edges("") == []
    assert utils.fallback_graph_edges("Only Kafka is mentioned.") == []
    edges = utils.fallback_graph_edges("Kafka has brokers and topics.", max_edges=0)
    assert len(edges) == 1


def test_fallback_edges_exhausts_pairs_and_skips_duplicates() -> None:
    edges = utils.fallback_graph_edges(
        "Kafka brokers own topics. Kafka brokers own topics.",
        max_edges=20,
    )
    assert [(edge["source"], edge["target"]) for edge in edges] == [
        ("Kafka", "Broker"),
        ("Broker", "Topic"),
    ]
