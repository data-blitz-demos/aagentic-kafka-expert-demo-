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
