"""Retrieval-only Neo4j Graph RAG access shared by Kafka Monitor and Doer agents."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import re
from typing import Any

from neo4j import GraphDatabase


def graph_question_terms(question: str, *, max_terms: int = 10) -> list[str]:
    """Extract stable search terms for the PDF-derived Kafka knowledge graph."""
    stop = {
        "about",
        "after",
        "before",
        "broker",
        "cluster",
        "does",
        "from",
        "into",
        "kafka",
        "that",
        "this",
        "topic",
        "what",
        "when",
        "where",
        "which",
        "with",
    }
    terms: list[str] = []
    for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", question.lower()):
        if token in stop or token in terms:
            continue
        terms.append(token)
        if len(terms) >= max(1, max_terms):
            break
    return terms or ["kafka"]


def retrieve_neo4j_knowledge(
    question: str,
    *,
    uri: str,
    username: str,
    password: str,
    auth_disabled: bool,
    max_facts: int = 30,
    max_excerpts: int = 8,
) -> dict[str, Any]:
    """Retrieve graph facts and source excerpts without asking a second LLM."""
    terms = graph_question_terms(question)
    auth = None if auth_disabled else (username, password)
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as session:
            facts = session.run(
                """
                UNWIND $terms AS term
                MATCH (seed:Entity)
                WHERE toLower(seed.name) CONTAINS term
                MATCH (seed)-[r:RELATES]-(neighbor:Entity)
                RETURN DISTINCT seed.name AS source,
                       coalesce(r.relation, "related_to") AS relation,
                       neighbor.name AS target,
                       coalesce(r.evidence, "") AS evidence,
                       coalesce(r.weight, 0) AS weight
                ORDER BY weight DESC
                LIMIT $limit
                """,
                {"terms": terms, "limit": max(1, int(max_facts))},
            ).data()
            excerpts = session.run(
                """
                UNWIND $terms AS term
                MATCH (chunk:Chunk)-[:MENTIONS]->(entity:Entity)
                WHERE toLower(entity.name) CONTAINS term
                OPTIONAL MATCH (doc:Document)-[:HAS_CHUNK]->(chunk)
                RETURN DISTINCT entity.name AS entity,
                       coalesce(doc.source_file, doc.title, "unknown") AS source_file,
                       chunk.chunk_index AS chunk_index,
                       left(coalesce(chunk.text, ""), 1200) AS text
                LIMIT $limit
                """,
                {"terms": terms, "limit": max(1, int(max_excerpts))},
            ).data()
    finally:
        driver.close()

    return {
        "kind": "neo4j_graph_rag",
        "query": question,
        "query_terms": terms,
        "facts": facts,
        "source_excerpts": excerpts,
        "instruction": (
            "Treat this as reference knowledge from ingested PDFs. Verify current cluster state "
            "with Prometheus or Kafka Admin tools before drawing an operational conclusion."
        ),
    }
