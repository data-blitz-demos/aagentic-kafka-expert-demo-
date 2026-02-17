#!/usr/bin/env python3
"""Generate current Kafka Expert architecture diagram PDF."""

from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.pdfgen import canvas

OUT = "/Users/paulharvener/workspace/kafka-expert/output/pdf/kafka-expert-current-architecture.pdf"


def box(c, x, y, w, h, title, subtitle="", fill=colors.HexColor("#1f2937"), stroke=colors.HexColor("#334155")):
    c.setFillColor(fill)
    c.setStrokeColor(stroke)
    c.roundRect(x, y, w, h, 8, stroke=1, fill=1)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 8, y + h - 14, title)
    if subtitle:
        c.setFont("Helvetica", 8)
        c.setFillColor(colors.HexColor("#d1d5db"))
        c.drawString(x + 8, y + h - 27, subtitle)


def arrow(c, x1, y1, x2, y2, color=colors.HexColor("#60a5fa"), label=""):
    c.setStrokeColor(color)
    c.setFillColor(color)
    c.setLineWidth(1.2)
    c.line(x1, y1, x2, y2)

    # Simple arrow head.
    dx = x2 - x1
    dy = y2 - y1
    length = (dx * dx + dy * dy) ** 0.5
    if length == 0:
        return
    ux, uy = dx / length, dy / length
    px, py = -uy, ux
    ah = 7
    aw = 3
    p1x = x2 - ah * ux + aw * px
    p1y = y2 - ah * uy + aw * py
    p2x = x2 - ah * ux - aw * px
    p2y = y2 - ah * uy - aw * py
    c.line(x2, y2, p1x, p1y)
    c.line(x2, y2, p2x, p2y)

    if label:
        c.setFont("Helvetica", 7)
        c.setFillColor(color)
        c.drawString((x1 + x2) / 2 + 3, (y1 + y2) / 2 + 3, label)


def main():
    c = canvas.Canvas(OUT, pagesize=landscape(letter))
    width, height = landscape(letter)

    c.setFillColor(colors.HexColor("#0b1220"))
    c.rect(0, 0, width, height, stroke=0, fill=1)

    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(24, height - 28, "Kafka Expert - Current Architecture")
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.HexColor("#cbd5e1"))
    c.drawString(24, height - 42, f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

    # Main panels
    c.setStrokeColor(colors.HexColor("#334155"))
    c.setFillColor(colors.HexColor("#111827"))
    c.roundRect(18, 58, 250, 520, 10, stroke=1, fill=1)
    c.roundRect(282, 58, 310, 520, 10, stroke=1, fill=1)
    c.roundRect(606, 58, 176, 520, 10, stroke=1, fill=1)

    c.setFillColor(colors.HexColor("#93c5fd"))
    c.setFont("Helvetica-Bold", 10)
    c.drawString(28, 560, "Client Layer")
    c.drawString(292, 560, "App/Agent Layer")
    c.drawString(616, 560, "Platform Services")

    # Client layer
    box(c, 32, 494, 220, 58, "User Browser", "Tabs: Kafka Expert, Producer, Consumer, Grafana, Kafka UI", fill=colors.HexColor("#1e3a8a"))
    box(c, 32, 420, 220, 58, "Kafka Expert UI", "Flask + LangChain/LangGraph", fill=colors.HexColor("#065f46"))
    box(c, 32, 346, 220, 58, "Producer UI", "Avro schema editor + message sender", fill=colors.HexColor("#374151"))
    box(c, 32, 272, 220, 58, "Consumer UI", "Live consumed feed + latency", fill=colors.HexColor("#374151"))
    box(c, 32, 198, 220, 58, "Grafana UI", "Dark dashboards", fill=colors.HexColor("#374151"))
    box(c, 32, 124, 220, 58, "Kafka UI", "Cluster/topic browsing", fill=colors.HexColor("#374151"))

    # App/agent layer
    box(c, 296, 476, 282, 76, "Kafka Expert Backend", "OpenAI model + MCP orchestration + deterministic Kafka ops", fill=colors.HexColor("#0f766e"))
    box(c, 296, 386, 134, 68, "Prometheus MCP", "Prom/Schema tools", fill=colors.HexColor("#1f2937"))
    box(c, 444, 386, 134, 68, "Kafka Admin MCP", "Topic/group/config ops", fill=colors.HexColor("#1f2937"))
    box(c, 296, 304, 282, 64, "Graph RAG Runtime", "PDF ingest -> entities/edges -> graph QA", fill=colors.HexColor("#1f2937"))
    box(c, 296, 228, 134, 58, "OpenAI API", "LLM inference", fill=colors.HexColor("#1f2937"))
    box(c, 444, 228, 134, 58, "Schema Registry API", "Subjects + contracts", fill=colors.HexColor("#1f2937"))

    # Platform layer
    box(c, 620, 484, 148, 68, "Kafka Cluster", "kafka1,kafka2,kafka3", fill=colors.HexColor("#7c2d12"))
    box(c, 620, 402, 148, 60, "Schema Registry", "localhost:8081", fill=colors.HexColor("#7c2d12"))
    box(c, 620, 328, 148, 60, "CouchDB", "Consumed message store", fill=colors.HexColor("#7c2d12"))
    box(c, 620, 254, 148, 60, "Kafka JMX Exporter", "localhost:9404/metrics", fill=colors.HexColor("#7c2d12"))
    box(c, 620, 180, 148, 60, "Prometheus", "localhost:9090", fill=colors.HexColor("#7c2d12"))
    box(c, 620, 106, 148, 60, "Neo4j", "Graph store (7474/7687)", fill=colors.HexColor("#7c2d12"))

    # Flows
    arrow(c, 252, 523, 296, 514, label="user actions")
    arrow(c, 252, 449, 296, 514, label="query/chat/fix")
    arrow(c, 252, 375, 620, 520, label="produce")
    arrow(c, 620, 516, 252, 301, label="consume")
    arrow(c, 252, 227, 620, 210, label="dashboards")
    arrow(c, 252, 153, 620, 512, label="admin view")

    arrow(c, 438, 510, 430, 420, label="metrics tool calls")
    arrow(c, 438, 510, 512, 420, label="admin tool calls")
    arrow(c, 438, 510, 438, 336, label="graph ops")
    arrow(c, 438, 510, 364, 257, label="LLM calls")

    arrow(c, 430, 420, 620, 210, label="PromQL")
    arrow(c, 430, 420, 620, 432, label="subjects")
    arrow(c, 512, 420, 620, 520, label="cluster mutate/read")
    arrow(c, 438, 336, 620, 136, label="nodes/edges")

    arrow(c, 620, 284, 620, 210, label="scraped")
    arrow(c, 620, 520, 620, 284, label="JMX source")

    # Legend
    c.setFillColor(colors.HexColor("#cbd5e1"))
    c.setFont("Helvetica", 8)
    c.drawString(22, 30, "Legend: Blue=client interactions, Teal=agent internals, Brown=platform services")

    c.showPage()
    c.save()
    print(OUT)


if __name__ == "__main__":
    main()
