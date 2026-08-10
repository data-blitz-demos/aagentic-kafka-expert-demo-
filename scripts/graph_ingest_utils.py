"""Fast deterministic fallbacks for Graph RAG PDF ingestion."""

import re


KAFKA_CONCEPTS = (
    ("Kafka", ("kafka",)),
    ("Broker", ("broker", "brokers")),
    ("Controller", ("controller", "controllers")),
    ("KRaft", ("kraft",)),
    ("ZooKeeper", ("zookeeper",)),
    ("Topic", ("topic", "topics")),
    ("Partition", ("partition", "partitions")),
    ("Replica", ("replica", "replicas", "replication")),
    ("ISR", ("isr", "in-sync replica", "in sync replica")),
    ("Leader", ("leader", "leaders")),
    ("Follower", ("follower", "followers")),
    ("Producer", ("producer", "producers")),
    ("Consumer", ("consumer", "consumers")),
    ("Consumer Group", ("consumer group", "consumer groups")),
    ("Offset", ("offset", "offsets")),
    ("Schema Registry", ("schema registry",)),
    ("Avro", ("avro",)),
)


def fallback_graph_edges(text: str, *, max_edges: int = 8) -> list[dict[str, str]]:
    """Create bounded Kafka concept edges when an extraction LLM is too slow or unavailable."""
    limit = max(1, int(max_edges))
    sentences = [
        re.sub(r"\s+", " ", sentence).strip()
        for sentence in re.split(r"(?<=[.!?])\s+|\n+", text or "")
        if sentence.strip()
    ]
    edges: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for sentence in sentences:
        lowered = sentence.lower()
        found = [
            canonical
            for canonical, aliases in KAFKA_CONCEPTS
            if any(re.search(rf"\b{re.escape(alias)}\b", lowered) for alias in aliases)
        ]
        if len(found) < 2:
            continue
        for source, target in zip(found, found[1:]):
            key = (source.lower(), target.lower())
            if key in seen:
                continue
            seen.add(key)
            edges.append(
                {
                    "source": source,
                    "relation": "relates_to",
                    "target": target,
                    "evidence": sentence[:300],
                }
            )
            if len(edges) >= limit:
                return edges
    return edges
