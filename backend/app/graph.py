from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

import networkx as nx


OVERVIEW_LIMITS = {
    "sales_order": 12,
    "sales_order_item": 18,
    "delivery": 12,
    "delivery_item": 18,
    "billing_document": 12,
    "billing_item": 18,
    "journal_entry": 12,
    "payment": 12,
    "customer": 10,
    "product": 10,
    "address": 8,
    "plant": 8,
}


def load_table_rows(connection: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    cursor = connection.execute(f'SELECT * FROM "{table_name}"')
    return [dict(row) for row in cursor.fetchall()]


def build_graph(connection: sqlite3.Connection) -> tuple[nx.MultiDiGraph, dict[str, list[str]]]:
    graph = nx.MultiDiGraph()
    value_index: dict[str, set[str]] = defaultdict(set)

    table_rows = {
        table: load_table_rows(connection, table)
        for table in (
            "sales_order_headers",
            "sales_order_items",
            "outbound_delivery_headers",
            "outbound_delivery_items",
            "billing_document_headers",
            "billing_document_items",
            "journal_entry_items_accounts_receivable",
            "payments_accounts_receivable",
            "business_partners",
            "business_partner_addresses",
            "products",
            "product_descriptions",
            "plants",
        )
    }

    product_descriptions = build_product_description_lookup(table_rows["product_descriptions"])

    customer_nodes: dict[str, str] = {}
    for row in table_rows["business_partners"]:
        customer_id = row.get("id")
        if not customer_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "customer",
            customer_id,
            row,
            label=row.get("business_partner_full_name") or row.get("business_partner_name") or customer_id,
            aliases=[row.get("customer_id"), row.get("customer")],
        )
        customer_nodes[customer_id] = node_id

    product_nodes: dict[str, str] = {}
    for row in table_rows["products"]:
        product_id = row.get("id")
        if not product_id:
            continue
        description = product_descriptions.get(product_id)
        properties = {**row}
        if description and not properties.get("product_description"):
            properties["product_description"] = description
        node_id = add_node(
            graph,
            value_index,
            "product",
            product_id,
            properties,
            label=description or row.get("product_old_id") or product_id,
        )
        product_nodes[product_id] = node_id

    plant_nodes: dict[str, str] = {}
    for row in table_rows["plants"]:
        plant_id = row.get("id")
        if not plant_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "plant",
            plant_id,
            row,
            label=row.get("plant_name") or plant_id,
            aliases=[row.get("address_id")],
        )
        plant_nodes[plant_id] = node_id

    address_nodes: dict[str, str] = {}
    for row in table_rows["business_partner_addresses"]:
        address_id = row.get("id")
        if not address_id:
            continue
        label_parts = [row.get("city_name"), row.get("region"), row.get("country")]
        label = ", ".join(part for part in label_parts if part) or row.get("street_name") or address_id
        node_id = add_node(
            graph,
            value_index,
            "address",
            address_id,
            row,
            label=label,
            aliases=[row.get("address_id"), row.get("business_partner_id")],
        )
        address_nodes[address_id] = node_id

    sales_order_nodes: dict[str, str] = {}
    for row in table_rows["sales_order_headers"]:
        order_id = row.get("id")
        if not order_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "sales_order",
            order_id,
            row,
            label=f"Sales Order {order_id}",
            aliases=[row.get("customer_id")],
        )
        sales_order_nodes[order_id] = node_id
        link_if_present(graph, node_id, customer_nodes.get(row.get("customer_id")), "belongs_to_customer")

    sales_order_item_nodes: dict[str, str] = {}
    for row in table_rows["sales_order_items"]:
        item_id = row.get("id")
        if not item_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "sales_order_item",
            item_id,
            row,
            label=f"SO Item {item_id}",
            aliases=[row.get("sales_order_id"), row.get("product_id"), row.get("plant_id")],
        )
        sales_order_item_nodes[item_id] = node_id
        link_if_present(graph, sales_order_nodes.get(row.get("sales_order_id")), node_id, "contains_item")
        link_if_present(graph, node_id, product_nodes.get(row.get("product_id")), "references_product")
        link_if_present(graph, node_id, plant_nodes.get(row.get("plant_id")), "planned_from_plant")

    delivery_nodes: dict[str, str] = {}
    for row in table_rows["outbound_delivery_headers"]:
        delivery_id = row.get("id")
        if not delivery_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "delivery",
            delivery_id,
            row,
            label=f"Delivery {delivery_id}",
        )
        delivery_nodes[delivery_id] = node_id

    delivery_item_nodes: dict[str, str] = {}
    for row in table_rows["outbound_delivery_items"]:
        item_id = row.get("id")
        if not item_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "delivery_item",
            item_id,
            row,
            label=f"Delivery Item {item_id}",
            aliases=[row.get("delivery_id"), row.get("sales_order_item_id"), row.get("plant_id")],
        )
        delivery_item_nodes[item_id] = node_id
        link_if_present(graph, delivery_nodes.get(row.get("delivery_id")), node_id, "contains_item")
        link_if_present(graph, sales_order_item_nodes.get(row.get("sales_order_item_id")), node_id, "fulfilled_by_delivery_item")
        link_if_present(graph, node_id, plant_nodes.get(row.get("plant_id")), "shipped_from_plant")

    billing_document_nodes: dict[str, str] = {}
    for row in table_rows["billing_document_headers"]:
        billing_id = row.get("id")
        if not billing_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "billing_document",
            billing_id,
            row,
            label=f"Billing {billing_id}",
            aliases=[row.get("customer_id"), row.get("accounting_document")],
        )
        billing_document_nodes[billing_id] = node_id
        link_if_present(graph, node_id, customer_nodes.get(row.get("customer_id")), "billed_to_customer")

    billing_item_nodes: dict[str, str] = {}
    for row in table_rows["billing_document_items"]:
        item_id = row.get("id")
        if not item_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "billing_item",
            item_id,
            row,
            label=f"Billing Item {item_id}",
            aliases=[row.get("billing_document_id"), row.get("delivery_item_id"), row.get("product_id")],
        )
        billing_item_nodes[item_id] = node_id
        link_if_present(graph, billing_document_nodes.get(row.get("billing_document_id")), node_id, "contains_item")
        link_if_present(graph, delivery_item_nodes.get(row.get("delivery_item_id")), node_id, "billed_by_billing_item")
        link_if_present(graph, node_id, product_nodes.get(row.get("product_id")), "references_product")

    journal_nodes: dict[str, str] = {}
    for row in table_rows["journal_entry_items_accounts_receivable"]:
        journal_id = row.get("id")
        if not journal_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "journal_entry",
            journal_id,
            row,
            label=f"Journal {row.get('accounting_document') or journal_id}",
            aliases=[row.get("billing_item_id"), row.get("customer_id"), row.get("accounting_document")],
        )
        journal_nodes[journal_id] = node_id
        link_if_present(graph, billing_item_nodes.get(row.get("billing_item_id")), node_id, "posted_to_journal_entry")
        link_if_present(graph, node_id, customer_nodes.get(row.get("customer_id")), "posted_for_customer")

    payment_nodes: dict[str, str] = {}
    for row in table_rows["payments_accounts_receivable"]:
        payment_id = row.get("id")
        if not payment_id:
            continue
        node_id = add_node(
            graph,
            value_index,
            "payment",
            payment_id,
            row,
            label=f"Payment {row.get('accounting_document') or payment_id}",
            aliases=[row.get("journal_entry_id"), row.get("customer_id"), row.get("accounting_document")],
        )
        payment_nodes[payment_id] = node_id
        link_if_present(graph, journal_nodes.get(row.get("journal_entry_id")), node_id, "settled_by_payment")
        link_if_present(graph, node_id, customer_nodes.get(row.get("customer_id")), "received_from_customer")

    for row in table_rows["business_partner_addresses"]:
        customer_node = customer_nodes.get(row.get("business_partner_id"))
        address_node = address_nodes.get(row.get("id"))
        link_if_present(graph, customer_node, address_node, "has_address")

    normalized_index = {key: sorted(values) for key, values in value_index.items()}
    return graph, normalized_index


def add_node(
    graph: nx.MultiDiGraph,
    value_index: dict[str, set[str]],
    node_type: str,
    natural_id: str,
    properties: dict[str, Any],
    *,
    label: str,
    aliases: list[str | None] | None = None,
) -> str:
    node_id = f"{node_type}:{natural_id}"
    payload = {
        **properties,
        "entity_id": natural_id,
    }
    graph.add_node(node_id, type=node_type, label=label, properties=payload)
    index_values(value_index, node_id, [natural_id, node_id, label, *(aliases or [])])
    return node_id


def link_if_present(graph: nx.MultiDiGraph, source: str | None, target: str | None, edge_type: str) -> None:
    if source and target and graph.has_node(source) and graph.has_node(target):
        graph.add_edge(source, target, type=edge_type)


def build_product_description_lookup(rows: list[dict[str, Any]]) -> dict[str, str]:
    descriptions: dict[str, str] = {}
    for row in rows:
        product_id = row.get("product_id")
        description = row.get("product_description")
        if not product_id or not description:
            continue
        if row.get("language") == "EN" or product_id not in descriptions:
            descriptions[product_id] = description
    return descriptions


def index_values(value_index: dict[str, set[str]], node_id: str, values: list[str | None]) -> None:
    for value in values:
        if value not in (None, ""):
            value_index[str(value)].add(node_id)


def serialize_graph(graph: nx.MultiDiGraph, node_ids: set[str] | None = None) -> dict[str, Any]:
    selected_nodes = node_ids or set(graph.nodes())
    nodes = [
        {
            "id": node_id,
            "type": data["type"],
            "label": data.get("label", node_id),
            "properties": data["properties"],
        }
        for node_id, data in graph.nodes(data=True)
        if node_id in selected_nodes
    ]
    edges = []
    for source, target, data in graph.edges(data=True):
        if source in selected_nodes and target in selected_nodes:
            edges.append({"source": source, "target": target, "type": data["type"]})
    return {"nodes": sorted(nodes, key=lambda item: item["id"]), "edges": edges}


def serialize_overview_graph(graph: nx.MultiDiGraph) -> dict[str, Any]:
    selected: set[str] = set()
    by_type: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for node_id, data in graph.nodes(data=True):
        degree = graph.degree(node_id)
        by_type[data["type"]].append((node_id, degree))

    for node_type, limit in OVERVIEW_LIMITS.items():
        ranked = sorted(by_type.get(node_type, []), key=lambda item: (-item[1], item[0]))
        selected.update(node_id for node_id, _ in ranked[:limit])

    if not selected:
        return {"nodes": [], "edges": []}

    for node_id in list(selected):
        selected.update(graph.predecessors(node_id))
        selected.update(graph.successors(node_id))

    return serialize_graph(graph, selected)


def neighborhood_subgraph(graph: nx.MultiDiGraph, node_id: str, depth: int = 1) -> dict[str, Any]:
    if not graph.has_node(node_id):
        return {"nodes": [], "edges": []}
    radius = max(1, min(depth, 2))
    undirected = graph.to_undirected(as_view=True)
    neighborhood = nx.ego_graph(undirected, node_id, radius=radius)
    return serialize_graph(graph, set(neighborhood.nodes()))


def trace_subgraph(graph: nx.MultiDiGraph, node_id: str) -> dict[str, Any]:
    if not graph.has_node(node_id):
        return {"nodes": [], "edges": [], "path_types": []}

    # Include both upstream and downstream flow context while keeping traces
    # bounded to a practical radius for UI usability.
    undirected = graph.to_undirected(as_view=True)
    connected = set(nx.single_source_shortest_path_length(undirected, node_id, cutoff=4).keys())
    payload = serialize_graph(graph, connected)
    payload["path_types"] = sorted({edge["type"] for edge in payload["edges"]})
    return payload
