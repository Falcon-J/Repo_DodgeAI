from __future__ import annotations

from typing import Any

import httpx

from .config import settings


SCHEMA_DESCRIPTION = """
Only use the following SQLite objects and columns:

1. o2c_flow_view
- sales_order_id
- sales_order_customer_id
- customer_name
- sales_order_item_id
- product_id
- product_description
- requested_quantity
- sales_order_item_net_amount
- transaction_currency
- sales_order_plant_id
- delivery_id
- delivery_item_id
- delivery_creation_date
- actual_delivery_quantity
- delivery_plant_id
- billing_document_id
- billing_item_id
- billing_document_date
- billing_accounting_document
- company_code
- billing_quantity
- billing_net_amount
- journal_entry_id
- journal_accounting_document
- journal_posting_date
- journal_amount
- clearing_accounting_document
- payment_id
- payment_accounting_document
- payment_posting_date
- payment_amount

2. broken_flow_view
- all o2c_flow_view columns
- flow_issue

3. business_partners
- id
- business_partner_full_name
- business_partner_name
- business_partner_category

4. products
- id
- product_old_id
- product_type
- base_unit
- product_group

Rules:
- Return exactly one SQLite SELECT query and nothing else.
- Use only the objects listed above.
- Use explicit JOIN clauses when you need multiple objects.
- Prefer o2c_flow_view and broken_flow_view over raw tables.
- Always include LIMIT 100 or less.
"""


class GroqSQLGenerator:
    def __init__(self) -> None:
        self.api_key = settings.groq_api_key
        self.base_url = settings.groq_base_url
        self.model = settings.groq_model

    async def generate_sql(self, question: str) -> str:
        if not self.api_key:
            raise ValueError("GROQ_API_KEY is required for this question.")
        return await self._generate_with_groq(question)

    async def _generate_with_groq(self, question: str) -> str:
        prompt = (
            "You are a SQLite SQL generator for an SAP Order-to-Cash dataset.\n"
            "Return SQL only.\n"
            "Do not add markdown fences, comments, or prose.\n"
            "Schema:\n"
            f"{SCHEMA_DESCRIPTION}\n"
            f"Question: {question}"
        )
        payload: dict[str, Any] = {
            "model": self.model,
            "temperature": 0.00000001,
            "messages": [
                {"role": "system", "content": "Return one SQLite SELECT query only."},
                {"role": "user", "content": prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(self.base_url, headers=headers, json=payload)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                detail = extract_provider_error(exc.response)
                raise RuntimeError(detail) from exc
            data = response.json()
        return data["choices"][0]["message"]["content"].strip()


def extract_provider_error(response: httpx.Response) -> str:
    status = response.status_code
    payload: dict[str, Any] = {}
    try:
        payload = response.json()
    except ValueError:
        payload = {}

    message = payload.get("error", {}).get("message") or payload.get("message") or response.text
    if status == 401:
        return f"Groq authentication failed: {message}"
    if status == 402:
        return f"Groq billing or credit issue: {message}"
    if status == 429:
        return f"Groq rate limit hit: {message}"
    return f"Groq API request failed with {status}: {message}"
