import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import networkx as nx
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.graph import build_graph, trace_subgraph
from app.ingest import REQUIRED_TABLES, rebuild_database
from app.main import app
from app.query import (
    build_query_plan,
    collect_highlight_nodes,
    execute_query,
    infer_trace_node_id,
    is_domain_query,
    validate_sql,
)


@pytest.fixture()
def data_dir() -> Path:
    project_dir = Path(__file__).resolve().parents[2]
    return project_dir.parent / "sap-o2c-data"


@pytest.fixture()
def connection(tmp_path: Path, data_dir: Path):
    conn = rebuild_database(data_dir, tmp_path / "test.sqlite3")
    try:
        yield conn
    finally:
        conn.close()


def test_rebuild_database_creates_required_tables_and_views(connection) -> None:
    table_names = {
        row[0]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    }
    for table_name in REQUIRED_TABLES:
        assert table_name in table_names
    assert "o2c_flow_view" in table_names
    assert "broken_flow_view" in table_names


def test_support_entities_are_normalized_for_addresses_and_products(connection) -> None:
    address_row = connection.execute(
        """
        SELECT id, business_partner_id, address_id
        FROM business_partner_addresses
        WHERE business_partner_id IS NOT NULL AND address_id IS NOT NULL
        LIMIT 1
        """
    ).fetchone()
    assert address_row is not None
    assert ":" in address_row["id"]

    description_row = connection.execute(
        """
        SELECT id, product_id, product_description
        FROM product_descriptions
        WHERE product_id IS NOT NULL AND product_description IS NOT NULL
        LIMIT 1
        """
    ).fetchone()
    assert description_row is not None
    assert description_row["id"].startswith(f"{description_row['product_id']}:")


def test_graph_contains_end_to_end_and_support_relationships(connection) -> None:
    graph, _ = build_graph(connection)

    flow_row = connection.execute(
        """
        SELECT sales_order_id, sales_order_item_id, delivery_id, delivery_item_id,
               billing_document_id, billing_item_id, journal_entry_id, payment_id
        FROM o2c_flow_view
        WHERE sales_order_id IS NOT NULL
          AND delivery_id IS NOT NULL
          AND billing_document_id IS NOT NULL
          AND journal_entry_id IS NOT NULL
          AND payment_id IS NOT NULL
        LIMIT 1
        """
    ).fetchone()
    assert flow_row is not None

    assert graph.has_edge(f"sales_order:{flow_row['sales_order_id']}", f"sales_order_item:{flow_row['sales_order_item_id']}")
    assert graph.has_edge(
        f"sales_order_item:{flow_row['sales_order_item_id']}",
        f"delivery_item:{flow_row['delivery_item_id']}",
    )
    assert graph.has_edge(f"delivery:{flow_row['delivery_id']}", f"delivery_item:{flow_row['delivery_item_id']}")
    assert graph.has_edge(f"billing_document:{flow_row['billing_document_id']}", f"billing_item:{flow_row['billing_item_id']}")
    assert graph.has_edge(
        f"billing_item:{flow_row['billing_item_id']}",
        f"journal_entry:{flow_row['journal_entry_id']}",
    )
    assert graph.has_edge(f"journal_entry:{flow_row['journal_entry_id']}", f"payment:{flow_row['payment_id']}")

    address_row = connection.execute(
        """
        SELECT business_partner_id, id
        FROM business_partner_addresses
        WHERE business_partner_id IS NOT NULL
        LIMIT 1
        """
    ).fetchone()
    assert address_row is not None
    assert graph.has_edge(
        f"customer:{address_row['business_partner_id']}",
        f"address:{address_row['id']}",
    )

    plant_row = connection.execute(
        """
        SELECT id, plant_id
        FROM sales_order_items
        WHERE plant_id IN (SELECT id FROM plants)
        LIMIT 1
        """
    ).fetchone()
    assert plant_row is not None
    assert graph.has_edge(f"sales_order_item:{plant_row['id']}", f"plant:{plant_row['plant_id']}")


def test_product_billing_leaderboard_query_returns_ranked_rows(connection) -> None:
    question = "Which products are associated with the highest number of billing documents?"
    plan = asyncio.run(build_query_plan(question, sql_generator=AsyncMock()))
    rows = execute_query(connection, validate_sql(plan.sql))

    assert plan.intent == "product_billing_leaderboard"
    assert rows
    assert "billing_document_count" in rows[0]


def test_billing_trace_query_returns_traceable_rows(connection) -> None:
    billing_document_id = connection.execute(
        "SELECT billing_document_id FROM o2c_flow_view WHERE billing_document_id IS NOT NULL LIMIT 1"
    ).fetchone()["billing_document_id"]

    plan = asyncio.run(
        build_query_plan(f"Trace the full flow of billing document {billing_document_id}", sql_generator=AsyncMock())
    )
    rows = execute_query(connection, validate_sql(plan.sql))

    assert plan.intent == "billing_flow_trace"
    assert plan.trace_node_id == f"billing_document:{billing_document_id}"
    assert rows
    assert rows[0]["billing_document_id"] == billing_document_id


def test_broken_flow_query_returns_flagged_records(connection) -> None:
    plan = asyncio.run(
        build_query_plan(
            "Identify sales orders with broken or incomplete flows",
            sql_generator=AsyncMock(),
        )
    )
    rows = execute_query(connection, validate_sql(plan.sql))

    assert plan.intent == "broken_flow_detection"
    assert rows
    assert rows[0]["flow_issue"]


def test_journal_rows_link_to_all_billing_items_for_multi_item_invoice(connection) -> None:
    row = connection.execute(
        """
        SELECT b.billing_document_id
        FROM billing_document_items b
        JOIN journal_entry_items_accounts_receivable j ON j.billing_item_id = b.id
        GROUP BY b.billing_document_id, j.id
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    assert row is not None


def test_trace_from_billing_document_includes_upstream_sales_order(connection) -> None:
    flow_row = connection.execute(
        """
        SELECT billing_document_id, sales_order_id
        FROM o2c_flow_view
        WHERE billing_document_id IS NOT NULL
          AND sales_order_id IS NOT NULL
        LIMIT 1
        """
    ).fetchone()
    assert flow_row is not None

    graph, _ = build_graph(connection)
    trace_payload = trace_subgraph(graph, f"billing_document:{flow_row['billing_document_id']}")
    traced_node_ids = {node["id"] for node in trace_payload["nodes"]}
    assert f"sales_order:{flow_row['sales_order_id']}" in traced_node_ids


def test_trace_subgraph_is_bounded_by_radius() -> None:
    graph = nx.MultiDiGraph()
    for index in range(7):
        node_id = f"n{index}"
        graph.add_node(node_id, type="test", label=node_id, properties={})
        if index > 0:
            graph.add_edge(f"n{index - 1}", node_id, type="link")

    payload = trace_subgraph(graph, "n0")
    traced_node_ids = {node["id"] for node in payload["nodes"]}

    assert "n4" in traced_node_ids
    assert "n5" not in traced_node_ids


def test_collect_highlight_nodes_prefers_typed_matches() -> None:
    rows = [
        {
            "billing_document_id": "90504298",
            "customer_name": "Sample Customer",
            "payment_amount": "120.00",
        }
    ]
    value_index = {
        "billing_document:90504298": ["billing_document:90504298"],
        "90504298": ["address:320000082:9526", "billing_document:90504298"],
    }

    highlighted = collect_highlight_nodes(rows, value_index, max_nodes=10)

    assert highlighted[0] == "billing_document:90504298"
    assert "address:320000082:9526" not in highlighted


def test_infer_trace_node_id_from_generic_trace_prompt() -> None:
    rows = [
        {
            "sales_order_id": "740550",
            "sales_order_item_id": "740550:000010",
            "customer_name": "Example Customer",
        }
    ]

    inferred = infer_trace_node_id("trace flow of 91150187", rows)
    assert inferred == "sales_order:740550"


def test_broken_flow_phrase_variant_stays_deterministic() -> None:
    plan = asyncio.run(
        build_query_plan(
            "identify sales order that are delivered and not billed",
            sql_generator=AsyncMock(),
        )
    )

    assert plan.intent == "broken_flow_detection"
    assert "broken_flow_view" in plan.sql


def test_greeting_query_returns_non_sql_plan() -> None:
    plan = asyncio.run(build_query_plan("Hello", sql_generator=AsyncMock()))

    assert plan.intent == "greeting"
    assert plan.sql is None
    assert "Order-to-Cash" in plan.answer


def test_guardrails_reject_non_domain_questions_and_unsafe_sql() -> None:
    assert not is_domain_query("Write me a joke about cats")
    assert not is_domain_query("What is the weather in Mumbai?")

    with pytest.raises(ValueError):
        validate_sql("UPDATE sales_order_headers SET customer_id = 'x'")

    with pytest.raises(ValueError):
        validate_sql("SELECT * FROM sqlite_master")


def test_chat_endpoint_uses_mocked_llm_for_bounded_fallback(tmp_path: Path) -> None:
    original_sqlite_path = settings.sqlite_path
    object.__setattr__(settings, "sqlite_path", tmp_path / "api-test.sqlite3")

    try:
        with TestClient(app) as client:
            client.app.state.sql_generator.generate_sql = AsyncMock(
                return_value="SELECT sales_order_id, billing_document_id FROM o2c_flow_view LIMIT 2"
            )

            response = client.post("/chat", json={"question": "Show a few dataset-backed O2C flow records"})
    finally:
        object.__setattr__(settings, "sqlite_path", original_sqlite_path)

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "llm_sql"
    assert payload["rows"]
    assert "answer" in payload
