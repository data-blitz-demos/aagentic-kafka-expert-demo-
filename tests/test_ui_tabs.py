import re
from pathlib import Path


SOURCE = Path(__file__).resolve().parents[1] / "scripts" / "kafka_expert_ui.py"


def _source() -> str:
    return SOURCE.read_text()


def test_every_tab_button_has_a_panel_and_switch_mapping() -> None:
    text = _source()
    buttons = dict(re.findall(r'<button id="(tab\w+Btn)"[^>]+onclick="switchTab\(\'([^\']+)\'\)', text))
    panels = set(re.findall(r'<section id="(\w+Panel)" class="panel', text))
    mapping_block = text[text.index("const tabs = {"):text.index("};", text.index("const tabs = {"))]

    assert len(buttons) == 11
    for button_id, tab_name in buttons.items():
        panel_id = {
            "expert": "expertPanel",
            "graph_rag": "graphRagPanel",
            "rag_metrics": "ragMetricsPanel",
            "llm": "llmPanel",
            "deepeval": "deepevalPanel",
            "fine_tuning": "fineTuningPanel",
            "producer": "producerPanel",
            "consumer": "consumerPanel",
            "grafana": "grafanaPanel",
            "neo4j_browser": "neo4jBrowserPanel",
            "kafka_ui": "kafkaUiPanel",
        }[tab_name]
        assert panel_id in panels
        assert f"{tab_name}: ['{panel_id}', '{button_id}']" in mapping_block


def test_switch_tab_guards_missing_role_specific_dom_nodes() -> None:
    text = _source()
    switch = text[text.index("function switchTab(tab)"):text.index("function esc(s)")]

    assert "if (panel) panel.classList.toggle" in switch
    assert "if (button) button.classList.toggle" in switch
    assert "Object.values(tabs).forEach" in switch


def test_cluster_state_uses_background_polling_instead_of_blocking_click() -> None:
    text = _source()
    assert "fetch('/api/cluster_state/status')" in text
    assert '"/api/cluster_state/status"' in text
    assert 'return jsonify({"ok": True, "pending": True' in text
    assert 'name="cluster-state-assessment"' in text


def test_fine_tuning_data_directory_is_configurable() -> None:
    text = _source()

    assert 'os.getenv("MLX_TRAINING_DATA_DIR", "output/fine-tuning")' in text
    assert 'value="__MLX_TRAINING_DATA_DIR__"' in text
    assert '"__MLX_TRAINING_DATA_DIR__", html.escape(MLX_TRAINING_DATA_DIR, quote=True)' in text
