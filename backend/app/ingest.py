from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


CORE_TABLES = [
    "sales_order_headers",
    "sales_order_items",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "billing_document_headers",
    "billing_document_items",
    "journal_entry_items_accounts_receivable",
    "payments_accounts_receivable",
    "business_partners",
    "products",
]

SUPPORT_TABLES = [
    "business_partner_addresses",
    "product_descriptions",
    "plants",
]

REQUIRED_TABLES = [*CORE_TABLES, *SUPPORT_TABLES]

TEXT_COLUMNS = {
    "id",
    "address_id",
    "billing_document_id",
    "billing_item_id",
    "business_partner_id",
    "customer_id",
    "delivery_id",
    "delivery_item_id",
    "journal_entry_id",
    "payment_id",
    "plant_id",
    "product_id",
    "sales_order_id",
    "sales_order_item_id",
}

INDEXED_COLUMNS = {
    "customer_id",
    "product_id",
    "sales_order_id",
    "delivery_id",
    "sales_order_item_id",
    "billing_document_id",
    "delivery_item_id",
    "billing_item_id",
    "journal_entry_id",
    "business_partner_id",
    "plant_id",
}


def snake_case(name: str) -> str:
    name = name.replace(".", "_")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)
    return name.strip("_").lower()


def normalize_segment(value: Any, width: int = 0) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if width and text.isdigit():
        return text.zfill(width)
    return text


def composite_id(*parts: Any) -> str:
    return ":".join(normalize_segment(part) for part in parts if normalize_segment(part))


def flatten_record(data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flattened.update(flatten_record(value, full_key))
        elif isinstance(value, list):
            flattened[snake_case(full_key)] = json.dumps(value, ensure_ascii=True)
        elif value is None:
            flattened[snake_case(full_key)] = None
        else:
            flattened[snake_case(full_key)] = value
    return flattened


def normalize_row(
    table_name: str,
    raw: dict[str, Any],
    billing_item_lookup: dict[str, list[str]],
) -> dict[str, Any]:
    row = flatten_record(raw)

    if table_name == "sales_order_headers":
        row["id"] = normalize_segment(raw.get("salesOrder"))
        row["customer_id"] = normalize_segment(raw.get("soldToParty"))
    elif table_name == "sales_order_items":
        order_id = normalize_segment(raw.get("salesOrder"))
        item_id = normalize_segment(raw.get("salesOrderItem"), 6)
        row["id"] = composite_id(order_id, item_id)
        row["sales_order_id"] = order_id
        row["product_id"] = normalize_segment(raw.get("material"))
        row["plant_id"] = normalize_segment(raw.get("productionPlant"))
    elif table_name == "outbound_delivery_headers":
        row["id"] = normalize_segment(raw.get("deliveryDocument"))
    elif table_name == "outbound_delivery_items":
        delivery_id = normalize_segment(raw.get("deliveryDocument"))
        delivery_item = normalize_segment(raw.get("deliveryDocumentItem"), 6)
        source_order = normalize_segment(raw.get("referenceSdDocument"))
        source_item = normalize_segment(raw.get("referenceSdDocumentItem"), 6)
        row["id"] = composite_id(delivery_id, delivery_item)
        row["delivery_id"] = delivery_id
        row["sales_order_item_id"] = composite_id(source_order, source_item)
        row["plant_id"] = normalize_segment(raw.get("plant"))
    elif table_name == "billing_document_headers":
        row["id"] = normalize_segment(raw.get("billingDocument"))
        row["customer_id"] = normalize_segment(raw.get("soldToParty"))
    elif table_name == "billing_document_items":
        billing_id = normalize_segment(raw.get("billingDocument"))
        billing_item = normalize_segment(raw.get("billingDocumentItem"), 6)
        ref_delivery = normalize_segment(raw.get("referenceSdDocument"))
        ref_delivery_item = normalize_segment(raw.get("referenceSdDocumentItem"), 6)
        row["id"] = composite_id(billing_id, billing_item)
        row["billing_document_id"] = billing_id
        row["delivery_item_id"] = composite_id(ref_delivery, ref_delivery_item)
        row["product_id"] = normalize_segment(raw.get("material"))
    elif table_name == "journal_entry_items_accounts_receivable":
        accounting_doc = normalize_segment(raw.get("accountingDocument"))
        accounting_item = normalize_segment(raw.get("accountingDocumentItem"), 6)
        row["id"] = composite_id(accounting_doc, accounting_item)
        # Keep the original reference first; expand to line-level links later if needed.
        row["billing_item_id"] = normalize_segment(raw.get("referenceDocument"))
        row["customer_id"] = normalize_segment(raw.get("customer"))
    elif table_name == "payments_accounts_receivable":
        accounting_doc = normalize_segment(raw.get("accountingDocument"))
        accounting_item = normalize_segment(raw.get("accountingDocumentItem"), 6)
        row["id"] = composite_id(accounting_doc, accounting_item)
        row["journal_entry_id"] = composite_id(accounting_doc, accounting_item)
        row["customer_id"] = normalize_segment(raw.get("customer"))
    elif table_name == "business_partners":
        partner_id = normalize_segment(raw.get("businessPartner"))
        row["id"] = partner_id
        row["customer_id"] = normalize_segment(raw.get("customer")) or partner_id
    elif table_name == "business_partner_addresses":
        partner_id = normalize_segment(raw.get("businessPartner"))
        address_id = normalize_segment(raw.get("addressId"))
        row["id"] = composite_id(partner_id, address_id)
        row["business_partner_id"] = partner_id
        row["address_id"] = address_id
    elif table_name == "products":
        row["id"] = normalize_segment(raw.get("product"))
    elif table_name == "product_descriptions":
        product_id = normalize_segment(raw.get("product"))
        language = normalize_segment(raw.get("language"))
        row["id"] = composite_id(product_id, language)
        row["product_id"] = product_id
    elif table_name == "plants":
        row["id"] = normalize_segment(raw.get("plant"))
        row["address_id"] = normalize_segment(raw.get("addressId"))

    return row


def infer_sqlite_type(values: Iterable[Any], column_name: str) -> str:
    if column_name in TEXT_COLUMNS:
        return "TEXT"
    saw_value = False
    for value in values:
        if value is None or value == "":
            continue
        saw_value = True
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            continue
        if isinstance(value, float):
            return "REAL"
        text = str(value)
        if re.fullmatch(r"-?\d+", text):
            continue
        if re.fullmatch(r"-?\d+\.\d+", text):
            return "REAL"
        return "TEXT"
    return "INTEGER" if saw_value else "TEXT"


def load_jsonl_rows(folder: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file_path in sorted(folder.glob("*.jsonl")):
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                rows.append(json.loads(stripped))
    return rows


def build_billing_item_lookup(data_dir: Path) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = defaultdict(list)
    folder = data_dir / "billing_document_items"
    for raw in load_jsonl_rows(folder):
        document = normalize_segment(raw.get("billingDocument"))
        item = normalize_segment(raw.get("billingDocumentItem"), 6)
        lookup[document].append(composite_id(document, item))
    return {key: sorted(set(values)) for key, values in lookup.items()}


def create_connection(sqlite_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    return connection


def rebuild_database(data_dir: Path, sqlite_path: Path) -> sqlite3.Connection:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if sqlite_path.exists():
        sqlite_path.unlink()

    connection = create_connection(sqlite_path)
    billing_item_lookup = build_billing_item_lookup(data_dir)

    for table_name in REQUIRED_TABLES:
        folder = data_dir / table_name
        raw_rows = load_jsonl_rows(folder)
        normalized_rows = normalize_rows(table_name, raw_rows, billing_item_lookup)
        create_table_and_insert(connection, table_name, normalized_rows)

    create_views(connection)
    connection.commit()
    return connection


def create_table_and_insert(connection: sqlite3.Connection, table_name: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        connection.execute(f'CREATE TABLE "{table_name}" ("id" TEXT)')
        return

    columns = sorted({key for row in rows for key in row.keys()})
    column_types = {
        column: infer_sqlite_type((row.get(column) for row in rows), column)
        for column in columns
    }
    column_defs = ", ".join(f'"{column}" {column_types[column]}' for column in columns)
    connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    connection.execute(f'CREATE TABLE "{table_name}" ({column_defs})')

    placeholders = ", ".join("?" for _ in columns)
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'
    values = [[coerce_value(row.get(column)) for column in columns] for row in rows]
    connection.executemany(sql, values)

    if "id" in columns:
        connection.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_id" ON "{table_name}" ("id")')
    for column in sorted(INDEXED_COLUMNS.intersection(columns)):
        connection.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{column}" ON "{table_name}" ("{column}")'
        )


def normalize_rows(
    table_name: str,
    raw_rows: list[dict[str, Any]],
    billing_item_lookup: dict[str, list[str]],
) -> list[dict[str, Any]]:
    normalized_rows = [normalize_row(table_name, raw, billing_item_lookup) for raw in raw_rows]
    if table_name != "journal_entry_items_accounts_receivable":
        return normalized_rows

    expanded_rows: list[dict[str, Any]] = []
    for row in normalized_rows:
        reference_document = normalize_segment(row.get("billing_item_id"))
        billing_items = billing_item_lookup.get(reference_document)
        if not billing_items:
            expanded_rows.append(row)
            continue
        for billing_item_id in billing_items:
            expanded_rows.append({**row, "billing_item_id": billing_item_id})
    return expanded_rows


def create_views(connection: sqlite3.Connection) -> None:
    connection.execute('DROP VIEW IF EXISTS "o2c_flow_view"')
    connection.execute('DROP VIEW IF EXISTS "broken_flow_view"')

    connection.execute(
        """
        CREATE VIEW "o2c_flow_view" AS
        WITH product_labels AS (
            SELECT
                product_id,
                COALESCE(
                    MAX(CASE WHEN language = 'EN' THEN product_description END),
                    MAX(product_description)
                ) AS product_description
            FROM product_descriptions
            GROUP BY product_id
        ),
        sales_order_driven_flow AS (
            SELECT
                soh.id AS sales_order_id,
                soh.customer_id AS sales_order_customer_id,
                bp.business_partner_full_name AS customer_name,
                soi.id AS sales_order_item_id,
                COALESCE(soi.product_id, bdi.product_id) AS product_id,
                pl.product_description,
                soi.requested_quantity,
                soi.net_amount AS sales_order_item_net_amount,
                COALESCE(soi.transaction_currency, bdi.transaction_currency) AS transaction_currency,
                soi.plant_id AS sales_order_plant_id,
                odi.delivery_id,
                odi.id AS delivery_item_id,
                odh.creation_date AS delivery_creation_date,
                odi.actual_delivery_quantity,
                odi.plant_id AS delivery_plant_id,
                bdh.id AS billing_document_id,
                bdi.id AS billing_item_id,
                bdh.billing_document_date,
                bdh.accounting_document AS billing_accounting_document,
                bdh.company_code,
                bdi.billing_quantity,
                bdi.net_amount AS billing_net_amount,
                jei.id AS journal_entry_id,
                jei.accounting_document AS journal_accounting_document,
                jei.posting_date AS journal_posting_date,
                jei.amount_in_transaction_currency AS journal_amount,
                jei.clearing_accounting_document,
                pay.id AS payment_id,
                pay.accounting_document AS payment_accounting_document,
                pay.posting_date AS payment_posting_date,
                pay.amount_in_transaction_currency AS payment_amount
            FROM sales_order_items soi
            LEFT JOIN sales_order_headers soh ON soh.id = soi.sales_order_id
            LEFT JOIN outbound_delivery_items odi ON odi.sales_order_item_id = soi.id
            LEFT JOIN outbound_delivery_headers odh ON odh.id = odi.delivery_id
            LEFT JOIN billing_document_items bdi ON bdi.delivery_item_id = odi.id
            LEFT JOIN billing_document_headers bdh ON bdh.id = bdi.billing_document_id
            LEFT JOIN business_partners bp ON bp.id = COALESCE(soh.customer_id, bdh.customer_id)
            LEFT JOIN product_labels pl ON pl.product_id = COALESCE(soi.product_id, bdi.product_id)
            LEFT JOIN journal_entry_items_accounts_receivable jei ON jei.billing_item_id = bdi.id
            LEFT JOIN payments_accounts_receivable pay ON pay.journal_entry_id = jei.id
        ),
        direct_billing_flow AS (
            SELECT
                NULL AS sales_order_id,
                bdh.customer_id AS sales_order_customer_id,
                bp.business_partner_full_name AS customer_name,
                NULL AS sales_order_item_id,
                bdi.product_id AS product_id,
                pl.product_description,
                NULL AS requested_quantity,
                NULL AS sales_order_item_net_amount,
                bdi.transaction_currency AS transaction_currency,
                NULL AS sales_order_plant_id,
                NULL AS delivery_id,
                NULL AS delivery_item_id,
                NULL AS delivery_creation_date,
                NULL AS actual_delivery_quantity,
                NULL AS delivery_plant_id,
                bdh.id AS billing_document_id,
                bdi.id AS billing_item_id,
                bdh.billing_document_date,
                bdh.accounting_document AS billing_accounting_document,
                bdh.company_code,
                bdi.billing_quantity,
                bdi.net_amount AS billing_net_amount,
                jei.id AS journal_entry_id,
                jei.accounting_document AS journal_accounting_document,
                jei.posting_date AS journal_posting_date,
                jei.amount_in_transaction_currency AS journal_amount,
                jei.clearing_accounting_document,
                pay.id AS payment_id,
                pay.accounting_document AS payment_accounting_document,
                pay.posting_date AS payment_posting_date,
                pay.amount_in_transaction_currency AS payment_amount
            FROM billing_document_items bdi
            LEFT JOIN outbound_delivery_items odi ON odi.id = bdi.delivery_item_id
            LEFT JOIN billing_document_headers bdh ON bdh.id = bdi.billing_document_id
            LEFT JOIN business_partners bp ON bp.id = bdh.customer_id
            LEFT JOIN product_labels pl ON pl.product_id = bdi.product_id
            LEFT JOIN journal_entry_items_accounts_receivable jei ON jei.billing_item_id = bdi.id
            LEFT JOIN payments_accounts_receivable pay ON pay.journal_entry_id = jei.id
            WHERE odi.id IS NULL
        )
        SELECT
            *
        FROM sales_order_driven_flow
        UNION ALL
        SELECT
            *
        FROM direct_billing_flow
        """
    )

    connection.execute(
        """
        CREATE VIEW "broken_flow_view" AS
        SELECT
            *,
            CASE
                WHEN delivery_item_id IS NOT NULL AND billing_item_id IS NULL THEN 'DELIVERED_NOT_BILLED'
                WHEN delivery_item_id IS NULL AND billing_item_id IS NOT NULL THEN 'BILLED_WITHOUT_DELIVERY'
                WHEN billing_item_id IS NOT NULL AND journal_entry_id IS NULL THEN 'BILLED_NOT_POSTED'
                WHEN journal_entry_id IS NOT NULL AND payment_id IS NULL THEN 'POSTED_NOT_CLEARED'
                ELSE NULL
            END AS flow_issue
        FROM o2c_flow_view
        WHERE
            (delivery_item_id IS NOT NULL AND billing_item_id IS NULL)
            OR (delivery_item_id IS NULL AND billing_item_id IS NOT NULL)
            OR (billing_item_id IS NOT NULL AND journal_entry_id IS NULL)
            OR (journal_entry_id IS NOT NULL AND payment_id IS NULL)
        """
    )


def coerce_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True)
    return value
