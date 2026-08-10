import importlib
import sys
from pathlib import Path
from types import SimpleNamespace


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
kafka_agent_tools = importlib.import_module("kafka_agent_tools")


def test_monitor_excludes_mutation_tools() -> None:
    tools = [
        SimpleNamespace(name="kafka_describe_cluster"),
        SimpleNamespace(name="kafka_list_topics"),
        SimpleNamespace(name="kafka_create_topic"),
        SimpleNamespace(name="kafka_delete_topic"),
        SimpleNamespace(name="kafka_set_topic_config"),
    ]

    selected = kafka_agent_tools.select_kafka_admin_tools(tools, allow_mutations=False)

    assert [tool.name for tool in selected] == ["kafka_describe_cluster", "kafka_list_topics"]


def test_remediation_receives_read_and_mutation_tools() -> None:
    tools = [SimpleNamespace(name="kafka_describe_cluster"), SimpleNamespace(name="kafka_create_topic")]

    selected = kafka_agent_tools.select_kafka_admin_tools(tools, allow_mutations=True)

    assert [tool.name for tool in selected] == ["kafka_describe_cluster", "kafka_create_topic"]


def test_monitor_gets_rest_reads_but_not_produce() -> None:
    tools = [
        SimpleNamespace(name="kafka_rest_proxy_health"),
        SimpleNamespace(name="kafka_rest_list_topics"),
        SimpleNamespace(name="kafka_rest_topic_metadata"),
        SimpleNamespace(name="kafka_rest_produce_json"),
    ]

    selected = kafka_agent_tools.select_kafka_rest_tools(tools, allow_mutations=False)

    assert [tool.name for tool in selected] == [
        "kafka_rest_proxy_health",
        "kafka_rest_list_topics",
        "kafka_rest_topic_metadata",
    ]
