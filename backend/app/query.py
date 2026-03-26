from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Any


DOMAIN_MESSAGE = "This system is designed to answer questions related to the provided SAP Order-to-Cash dataset only."
ROW_LIMIT = 100
DENIED_SQL_PATTERNS = re.compile(
    r"\b(drop|delete|update|insert|alter|create|attach|pragma|replace|truncate)\b",
    re.IGNORECASE,
)
COMMENT_PATTERN = re.compile(r"(--|/\*)")
OBJECT_PATTERN = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE)
LIMIT_PATTERN = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
ID_PATTERN = re.compile(r"\b[0-9]{6,}\b")
FLOW_ID_PATTERN = re.compile(r"\b[0-9]{6,}:[0-9]{6}\b")

ALLOWED_QUERY_OBJECTS = {
    "o2c_flow_view",
    "broken_flow_view",
    "business_partners",
    "products",
}

DOMAIN_KEYWORDS = {
    "accounts receivable",
    "billing",
    "billing document",
    "cash",
    "customer",
    "delivery",
    "flow",
    "invoice",
    "journal",
    "material",
    "o2c",
    "order",
    "order to cash",
    "payment",
    "product",
    "sales",
    "sales order",
    "sap",
}

NON_DOMAIN_PATTERNS = [
    re.compile(r"\b(capital of|movie|poem|recipe|sports|stock price|weather|write a story|joke)\b", re.IGNORECASE),
    re.compile(r"\b(explain quantum|who won|latest news|translate)\b", re.IGNORECASE),
]

LOOKUP_KEYWORDS = {
    "billing_document": ("billing", "invoice"),
    "sales_order": ("sales order", "order"),
    "delivery": ("delivery",),
    "journal_entry": ("journal", "accounting document"),
    "payment": ("payment", "clearing"),
    "customer": ("customer", "business partner"),
    "product": ("product", "material"),
}

HIGHLIGHT_COLUMN_HINTS: dict[str, tuple[str, ...]] = {
    "sales_order_id": ("sales_order",),
    "sales_order_item_id": ("sales_order_item",),
    "delivery_id": ("delivery",),
    "delivery_item_id": ("delivery_item",),
    "billing_document_id": ("billing_document",),
    "billing_item_id": ("billing_item",),
    "journal_entry_id": ("journal_entry",),
    "payment_id": ("payment",),
    "sales_order_customer_id": ("customer",),
    "customer_id": ("customer",),
    "product_id": ("product",),
    "sales_order_plant_id": ("plant",),
    "delivery_plant_id": ("plant",),
    "plant_id": ("plant",),
}


@dataclass(frozen=True)
class QueryPlan:
    intent: str
    sql: str | None = None
    answer: str | None = None
    trace_node_id: str | None = None


def is_domain_query(question: str) -> bool:
    lowered = question.lower()
    if any(pattern.search(lowered) for pattern in NON_DOMAIN_PATTERNS):
        return False
    return any(keyword in lowered for keyword in DOMAIN_KEYWORDS) or bool(ID_PATTERN.search(question))


def is_greeting_query(question: str) -> bool:
    lowered = question.strip().lower()
    greetings = (
        "hi",
        "hello",
        "hey",
        "good morning",
        "good afternoon",
        "good evening",
        "what can you do",
        "help",
    )
    return any(lowered == phrase or lowered.startswith(f"{phrase} ") for phrase in greetings)


async def build_query_plan(question: str, sql_generator: Any) -> QueryPlan:
    lowered = question.lower()
    record_id = extract_record_id(question)
    item_id = extract_flow_item_id(question)

    if is_greeting_query(question):
        return QueryPlan(
            intent="greeting",
            answer=(
                "Hi! I am a context-first SAP Order-to-Cash assistant. "
                "Ask me about billed products, broken handoffs, journal entries, payments, "
                "or trace a billing document by its ID."
            ),
        )

    if is_product_billing_leaderboard(lowered):
        return QueryPlan(intent="product_billing_leaderboard", sql=build_product_billing_leaderboard_sql())

    if is_flow_trace(lowered):
        if record_id:
            return QueryPlan(
                intent="billing_flow_trace",
                sql=build_billing_flow_trace_sql(record_id),
                trace_node_id=f"billing_document:{record_id}",
            )
        return QueryPlan(
            intent="trace_guidance",
            answer="Share a billing document ID and I will trace its full Sales Order -> Delivery -> Billing -> Journal Entry -> Payment flow.",
        )

    if is_broken_flow_question(lowered):
        return QueryPlan(intent="broken_flow_detection", sql=build_broken_flow_sql(question))

    lookup_plan = build_lookup_plan(question, lowered, record_id, item_id)
    if lookup_plan:
        return lookup_plan

    llm_sql = await sql_generator.generate_sql(question)
    return QueryPlan(intent="llm_sql", sql=llm_sql)


def build_product_billing_leaderboard_sql() -> str:
    return """
        SELECT
            COALESCE(product_description, product_id) AS product_name,
            product_id,
            COUNT(DISTINCT billing_document_id) AS billing_document_count
        FROM o2c_flow_view
        WHERE billing_document_id IS NOT NULL
        GROUP BY product_id, product_description
        ORDER BY billing_document_count DESC, product_id ASC
        LIMIT 10
    """


def build_billing_flow_trace_sql(record_id: str) -> str:
    return f"""
        SELECT
            sales_order_id,
            sales_order_item_id,
            delivery_id,
            delivery_item_id,
            billing_document_id,
            billing_item_id,
            journal_entry_id,
            payment_id,
            customer_name,
            product_id,
            product_description
        FROM o2c_flow_view
        WHERE billing_document_id = '{record_id}' OR billing_item_id LIKE '{record_id}:%'
        ORDER BY sales_order_item_id
        LIMIT 50
    """


def build_broken_flow_sql(question: str) -> str:
    lowered = question.lower()
    filters: list[str] = []
    if (
        "delivered but not billed" in lowered
        or "delivered not billed" in lowered
        or "delivered and not billed" in lowered
    ):
        filters.append("flow_issue = 'DELIVERED_NOT_BILLED'")
    if "billed without delivery" in lowered:
        filters.append("flow_issue = 'BILLED_WITHOUT_DELIVERY'")
    if "billed not posted" in lowered or "not posted" in lowered:
        filters.append("flow_issue = 'BILLED_NOT_POSTED'")
    if "not cleared" in lowered or "posted not cleared" in lowered or "payment pending" in lowered:
        filters.append("flow_issue = 'POSTED_NOT_CLEARED'")

    where_clause = f"WHERE {' OR '.join(filters)}" if filters else ""
    return f"""
        SELECT
            flow_issue,
            sales_order_id,
            sales_order_item_id,
            delivery_id,
            billing_document_id,
            journal_entry_id,
            payment_id,
            customer_name,
            product_id,
            product_description
        FROM broken_flow_view
        {where_clause}
        ORDER BY flow_issue, sales_order_id, sales_order_item_id
        LIMIT 50
    """


def build_lookup_plan(
    question: str,
    lowered: str,
    record_id: str | None,
    item_id: str | None,
) -> QueryPlan | None:
    if item_id and any(keyword in lowered for keyword in LOOKUP_KEYWORDS["sales_order"]):
        return QueryPlan(
            intent="sales_order_item_lookup",
            sql=f"""
                SELECT *
                FROM o2c_flow_view
                WHERE sales_order_item_id = '{item_id}'
                LIMIT 25
            """,
            trace_node_id=f"sales_order_item:{item_id}",
        )

    if record_id is None:
        return None

    if any(keyword in lowered for keyword in LOOKUP_KEYWORDS["journal_entry"]):
        return QueryPlan(
            intent="journal_entry_lookup",
            sql=f"""
                SELECT
                    billing_document_id,
                    billing_item_id,
                    journal_entry_id,
                    journal_accounting_document,
                    journal_posting_date,
                    customer_name
                FROM o2c_flow_view
                WHERE billing_document_id = '{record_id}'
                    OR billing_item_id LIKE '{record_id}:%'
                    OR journal_accounting_document = '{record_id}'
                    OR journal_entry_id = '{record_id}'
                LIMIT 25
            """,
            trace_node_id=f"billing_document:{record_id}" if "billing" in lowered or "invoice" in lowered else None,
        )

    if any(keyword in lowered for keyword in LOOKUP_KEYWORDS["payment"]):
        return QueryPlan(
            intent="payment_lookup",
            sql=f"""
                SELECT
                    billing_document_id,
                    journal_entry_id,
                    payment_id,
                    payment_accounting_document,
                    payment_posting_date,
                    payment_amount,
                    transaction_currency
                FROM o2c_flow_view
                WHERE payment_accounting_document = '{record_id}'
                    OR payment_id = '{record_id}'
                    OR journal_accounting_document = '{record_id}'
                    OR billing_document_id = '{record_id}'
                    OR billing_item_id LIKE '{record_id}:%'
                LIMIT 25
            """,
        )

    if any(keyword in lowered for keyword in LOOKUP_KEYWORDS["billing_document"]):
        return QueryPlan(
            intent="billing_lookup",
            sql=f"""
                SELECT
                    billing_document_id,
                    billing_item_id,
                    customer_name,
                    product_id,
                    product_description,
                    journal_entry_id,
                    payment_id
                FROM o2c_flow_view
                WHERE billing_document_id = '{record_id}' OR billing_item_id LIKE '{record_id}:%'
                LIMIT 25
            """,
            trace_node_id=f"billing_document:{record_id}",
        )

    if any(keyword in lowered for keyword in LOOKUP_KEYWORDS["delivery"]):
        return QueryPlan(
            intent="delivery_lookup",
            sql=f"""
                SELECT
                    delivery_id,
                    delivery_item_id,
                    sales_order_id,
                    billing_document_id,
                    product_id,
                    product_description
                FROM o2c_flow_view
                WHERE delivery_id = '{record_id}' OR delivery_item_id LIKE '{record_id}:%'
                LIMIT 25
            """,
            trace_node_id=f"delivery:{record_id}",
        )

    if any(keyword in lowered for keyword in LOOKUP_KEYWORDS["sales_order"]):
        return QueryPlan(
            intent="sales_order_lookup",
            sql=f"""
                SELECT *
                FROM o2c_flow_view
                WHERE sales_order_id = '{record_id}' OR sales_order_item_id LIKE '{record_id}:%'
                LIMIT 25
            """,
            trace_node_id=f"sales_order:{record_id}",
        )

    if any(keyword in lowered for keyword in LOOKUP_KEYWORDS["customer"]):
        return QueryPlan(
            intent="customer_lookup",
            sql=f"""
                SELECT
                    id,
                    business_partner_full_name,
                    business_partner_name,
                    business_partner_category
                FROM business_partners
                WHERE id = '{record_id}' OR customer_id = '{record_id}'
                LIMIT 25
            """,
            trace_node_id=f"customer:{record_id}",
        )

    if any(keyword in lowered for keyword in LOOKUP_KEYWORDS["product"]):
        return QueryPlan(
            intent="product_lookup",
            sql=f"""
                SELECT
                    product_id,
                    product_description,
                    COUNT(DISTINCT billing_document_id) AS billing_document_count
                FROM o2c_flow_view
                WHERE product_id = '{record_id}'
                GROUP BY product_id, product_description
                LIMIT 25
            """,
            trace_node_id=f"product:{record_id}",
        )

    return QueryPlan(
        intent="record_lookup",
        sql=f"""
            SELECT *
            FROM o2c_flow_view
            WHERE sales_order_id = '{record_id}'
                OR delivery_id = '{record_id}'
                OR billing_document_id = '{record_id}'
                OR journal_accounting_document = '{record_id}'
                OR payment_accounting_document = '{record_id}'
            LIMIT 25
        """,
    )


def is_product_billing_leaderboard(lowered: str) -> bool:
    return (
        "product" in lowered or "material" in lowered
    ) and ("highest" in lowered or "top" in lowered or "most" in lowered) and (
        "billing" in lowered or "invoice" in lowered
    )


def is_flow_trace(lowered: str) -> bool:
    return ("trace" in lowered or "full flow" in lowered or "show flow" in lowered) and (
        "billing" in lowered or "invoice" in lowered
    )


def is_broken_flow_question(lowered: str) -> bool:
    markers = (
        "broken",
        "incomplete",
        "not billed",
        "without delivery",
        "not posted",
        "not cleared",
        "flow issue",
    )
    return ("sales order" in lowered or "flow" in lowered or "orders" in lowered) and any(
        marker in lowered for marker in markers
    )


def extract_record_id(question: str) -> str | None:
    match = ID_PATTERN.search(question)
    return match.group(0) if match else None


def extract_flow_item_id(question: str) -> str | None:
    match = FLOW_ID_PATTERN.search(question)
    return match.group(0) if match else None


def validate_sql(sql: str) -> str:
    cleaned = sql.strip().strip("`")
    cleaned = re.sub(r"^sql\s*", "", cleaned, flags=re.IGNORECASE)
    if COMMENT_PATTERN.search(cleaned):
        raise ValueError("SQL comments are not allowed.")
    if ";" in cleaned.rstrip(";"):
        raise ValueError("Only a single read-only query is allowed.")
    if DENIED_SQL_PATTERNS.search(cleaned):
        raise ValueError("Only read-only SELECT queries are allowed.")
    if not re.match(r"^(select|with)\b", cleaned, flags=re.IGNORECASE):
        raise ValueError("Only read-only SELECT queries are allowed.")

    referenced_objects = {
        match.group(1).strip('"').lower()
        for match in OBJECT_PATTERN.finditer(cleaned)
    }
    if not referenced_objects:
        raise ValueError("Query must reference an allowlisted dataset object.")
    if not referenced_objects.issubset(ALLOWED_QUERY_OBJECTS):
        raise ValueError("Query references objects outside the allowed dataset scope.")

    limit_match = LIMIT_PATTERN.search(cleaned)
    if limit_match:
        if int(limit_match.group(1)) > ROW_LIMIT:
            raise ValueError(f"Query row limit cannot exceed {ROW_LIMIT}.")
        return cleaned.rstrip(";")

    return f"{cleaned.rstrip(';')} LIMIT {ROW_LIMIT}"


def execute_query(connection: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    cursor = connection.execute(sql)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def compose_answer(question: str, intent: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No matching records were found in the dataset."

    if intent == "product_billing_leaderboard":
        leader = rows[0]
        return (
            f"{leader.get('product_name')} ({leader.get('product_id')}) appears on the most billing documents "
            f"with {leader.get('billing_document_count')} billing documents. "
            f"I returned the top {len(rows)} ranked products from the dataset."
        )

    if intent == "billing_flow_trace":
        first = rows[0]
        sales_order_id = first.get("sales_order_id")
        delivery_id = first.get("delivery_id")
        billing_document_id = first.get("billing_document_id")
        journal_entry_id = first.get("journal_entry_id")
        payment_id = first.get("payment_id")
        step_parts = []
        if sales_order_id:
            step_parts.append(f"Sales Order {sales_order_id}")
        if delivery_id:
            step_parts.append(f"Delivery {delivery_id}")
        if billing_document_id:
            step_parts.append(f"Billing {billing_document_id}")
        if journal_entry_id:
            step_parts.append(f"Journal Entry {journal_entry_id}")
        if payment_id:
            if payment_id == journal_entry_id:
                step_parts.append(f"Payment recorded ({payment_id})")
            else:
                step_parts.append(f"Payment {payment_id}")
        else:
            step_parts.append("Payment pending")
        flow = " -> ".join(step_parts)
        return (
            f"I traced billing document {billing_document_id} across {len(rows)} related flow line(s): {flow}."
        )

    if intent == "broken_flow_detection":
        counts = summarize_issue_counts(rows)
        return f"I found {len(rows)} incomplete or broken flow records. Issue mix: {counts}."

    if intent in {"journal_entry_lookup", "payment_lookup", "billing_lookup", "sales_order_lookup", "delivery_lookup"}:
        first = rows[0]
        preview = ", ".join(f"{key}={value}" for key, value in list(first.items())[:4] if value not in (None, ""))
        return f"I found {len(rows)} matching flow records. First result: {preview}."

    if intent in {"customer_lookup", "product_lookup", "record_lookup", "sales_order_item_lookup"}:
        first = rows[0]
        preview = ", ".join(f"{key}={value}" for key, value in list(first.items())[:4] if value not in (None, ""))
        return f"I found {len(rows)} matching dataset records. First result: {preview}."

    if intent == "llm_sql":
        first = rows[0]
        focus_columns = (
            "sales_order_id",
            "sales_order_item_id",
            "delivery_id",
            "delivery_item_id",
            "billing_document_id",
            "billing_item_id",
            "journal_entry_id",
            "payment_id",
            "customer_name",
            "product_id",
            "payment_amount",
            "transaction_currency",
        )
        preview_parts = []
        for column in focus_columns:
            value = first.get(column)
            if value not in (None, ""):
                preview_parts.append(f"{column}={value}")
            if len(preview_parts) >= 4:
                break
        preview = ", ".join(preview_parts) if preview_parts else "I returned dataset-backed rows for your question."
        return f"I found {len(rows)} matching records. Example: {preview}."

    columns = ", ".join(list(rows[0].keys())[:6])
    return f'I answered "{question}" with {len(rows)} matching rows using dataset-backed SQL. Columns returned: {columns}.'


def summarize_rows(rows: list[dict[str, Any]], max_rows: int = 3) -> list[dict[str, Any]]:
    return rows[:max_rows]


def summarize_issue_counts(rows: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        issue = str(row.get("flow_issue") or "UNKNOWN")
        counts[issue] = counts.get(issue, 0) + 1
    return ", ".join(f"{issue}={count}" for issue, count in sorted(counts.items()))


def infer_trace_node_id(question: str, rows: list[dict[str, Any]]) -> str | None:
    lowered = question.lower()
    if not rows or not any(keyword in lowered for keyword in ("trace", "flow", "path")):
        return None

    first = rows[0]
    trace_columns = (
        ("billing_document_id", "billing_document"),
        ("billing_item_id", "billing_item"),
        ("payment_id", "payment"),
        ("journal_entry_id", "journal_entry"),
        ("delivery_id", "delivery"),
        ("delivery_item_id", "delivery_item"),
        ("sales_order_id", "sales_order"),
        ("sales_order_item_id", "sales_order_item"),
        ("product_id", "product"),
        ("customer_id", "customer"),
        ("sales_order_customer_id", "customer"),
    )
    for column, node_type in trace_columns:
        value = first.get(column)
        if value not in (None, ""):
            return f"{node_type}:{value}"
    return None


def collect_highlight_nodes(
    rows: list[dict[str, Any]],
    value_index: dict[str, list[str]],
    max_nodes: int = 36,
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def add_nodes(nodes: list[str]) -> None:
        for node_id in nodes:
            if node_id not in seen:
                seen.add(node_id)
                ordered.append(node_id)

    for row in rows:
        for column, value in row.items():
            if value is None or value == "":
                continue
            text = str(value)
            hinted_types = HIGHLIGHT_COLUMN_HINTS.get(column, ())
            for node_type in hinted_types:
                typed_key = f"{node_type}:{text}"
                add_nodes(value_index.get(typed_key, []))
            if hinted_types and len(ordered) < max_nodes:
                fallback_nodes = value_index.get(text, [])
                if hinted_types:
                    fallback_nodes = [
                        node_id for node_id in fallback_nodes
                        if any(node_id.startswith(f"{node_type}:") for node_type in hinted_types)
                    ]
                add_nodes(fallback_nodes)
            if len(ordered) >= max_nodes:
                return ordered[:max_nodes]

    if ordered:
        return ordered[:max_nodes]

    # Last resort fallback for odd schemas/LLM columns.
    for row in rows:
        for value in row.values():
            if value is None or value == "":
                continue
            add_nodes(value_index.get(str(value), []))
            if len(ordered) >= max_nodes:
                return ordered[:max_nodes]

    return ordered[:max_nodes]
