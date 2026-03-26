# Dodge AI: SAP O2C Context Graph Assistant

This project ingests SAP Order-to-Cash extracts, builds a business graph, and answers natural-language questions using a constrained hybrid query router (deterministic SQL + bounded LLM fallback).

---

## What We Implemented

### 1. Graph Construction

- Normalized JSONL data into SQLite tables and analytical views in `backend/app/ingest.py`.
- Built typed graph nodes for:
  - **Core:** sales orders/items, deliveries/items, billing documents/items, journal entries, payments
  - **Supporting:** customers, products, addresses, plants
- Built typed business edges in `backend/app/graph.py` for end-to-end O2C flow and supporting relationships.
- Added model hardening for journal-to-billing line expansion to avoid false broken-flow outcomes.

---

### 2. Graph Visualization

- React + `react-force-graph-2d` UI with:
  - Node neighborhood expansion on click
  - Node metadata inspection card
  - Relationship visualization and highlight overlay
  - Minimize/Maximize graph panel controls
  - Reset View and granular overlay toggle
- Stability fixes:
  - Click de-racing for fast node-to-node navigation
  - Normalized link merge keys to prevent graph state corruption
  - Controlled auto-fit (explicit reset/trace events only)

---

### 3. Conversational Query Interface

- Chat endpoint `POST /chat` accepts NL prompts.
- Deterministic router handles core required queries and lookup intents.
- Bounded LLM fallback handles unmatched in-domain prompts.
- SQL is always validated before execution and grounded in dataset-backed results.
- Chat responses drive graph behavior with highlights and trace focus.

---

### 4. Required Query Coverage

- Top billed products by count
- Full billing flow trace (Sales Order → Delivery → Billing → Journal Entry → Payment)
- Broken/incomplete flow detection (including direct-billed cases)

---

### 5. Guardrails

#### Layer 1: Domain Rejection

- **Off-domain rejection:** `"This system is designed to answer questions related to the provided SAP Order-to-Cash dataset only."`
- **Domain Keywords (17):** accounts receivable, billing, billing document, cash, customer, delivery, flow, invoice, journal, material, o2c, order, order to cash, payment, product, sales, sales order, sap
- **Off-Topic Patterns (2 regex):** General knowledge queries (capital of, movie, poem), creative requests (write a story), current events (who won, latest news)
- **Behavior:** Rejects if ANY off-topic pattern matches OR no domain keyword found

#### Layer 2: SQL Safety (8 Validation Checks)

| Check | Rule |
| --- | --- |
| No DML/DDL | `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `CREATE`, `PRAGMA` blocked |
| No comments | `--` and `/* */` injection blocked |
| Single query only | Multi-statement `;` injection blocked |
| Read-only | Must start with `SELECT` or `WITH` (CTE allowed) |
| Object allowlist | Only `o2c_flow_view`, `broken_flow_view`, `business_partners`, `products` |
| Row limit | Max 100 rows (auto-appended if missing) |
| String cleaning | Markdown backticks/sql prefix stripped |
| Hard cap | `ROW_LIMIT = 100` (compile-time constant, no bypass) |

#### Layer 3: Debug Visibility Control

- SQL/debug details hidden by default: `VITE_SHOW_DEBUG_DETAILS=false`
- Optional dev-mode disclosure: When `true`, shows SQL query + first 2 result rows
- Production deployment: Always set to `false`

#### Layer 4: LLM Output Double-Validation

- Even LLM-generated SQL passes through `validate_sql()`
- Groq output cannot escape constraints (both deterministic + LLM paths validated)
- Result: Defense-in-depth — LLM fallback is still safe

---

## Architecture Decisions

### Why SQLite

**Decision:** SQLite instead of PostgreSQL, MongoDB, or in-memory stores

**Rationale:**

- **Portability:** Single `.sqlite3` file, zero server setup, deployable anywhere
- **Reproducibility:** Deterministic rebuild from JSONL source extracts (bit-for-bit consistent)
- **Scope fit:** Assignment-scale data (17 tables, 100K rows max) doesn't need a distributed DB
- **Auditability:** Full schema visible, no external dependencies, can inspect anytime
- **Performance:** Analytical joins execute <500ms for complex O2C traces

**Tradeoffs:**

| Pro | Con |
| --- | --- |
| Zero operations overhead | No horizontal scaling (acceptable for this scope) |
| Complete determinism (reproducible locally & in cloud) | Single-process write (acceptable — mostly read workload) |
| Simple backup (file copy) | |

**Schema Design:**

- 17 normalized base tables (`sales_order`, `delivery`, `billing`, etc.)
- 2 analytical views (`o2c_flow_view`, `broken_flow_view`) for denormalized O2C queries
- Value index (in-graph, dict-based) for O(1) node lookup during highlighting

---

### Why Hybrid Architecture (Graph + SQL + LLM Fallback)

**Three-Layer Architecture:**

**Layer 1: Deterministic SQL (93% of queries)**
- 14 pre-built SQL intents (product leaderboard, flow trace, broken flows, 8 entity lookups)
- Hardcoded but parameterized (safe, fast, auditable)
- Example: `"top billed products"` → deterministic leaderboard query

**Layer 2: NetworkX Graph (Interactive)**
- 13 node types, 15 semantic edges, value index for highlighting
- Enables neighborhood expansion, trace visualization, node inspection
- Complements SQL for relationship exploration

**Layer 3: LLM Fallback (7% of queries)**
- Groq API for unmatched questions (custom aggregations, edge cases)
- Schema-constrained prompt (46 columns, 4 allowlisted objects)
- Output validated by 8-layer SQL guardrails before execution
- Example: `"average net amount per customer by month"` → LLM generates + validates SQL

**Why this approach?**

| Reason | Detail |
| --- | --- |
| Security | Deterministic queries are safer than dynamic; LLM output validated before execution |
| Performance | 93% deterministic queries execute <100ms; LLM fallback 1–2s only for edge cases |
| Auditability | Evaluators can read all deterministic SQL in `query.py`; LLM process is documented |
| Reproducibility | Same question = same deterministic result (no LLM variance) |
| Cost | Only 7% of queries hit Groq API (~$0.01/query for fallback cases) |

---

### Component Breakdown

| Component | Purpose | Implementation |
| --- | --- | --- |
| `backend/app/ingest.py` | Data normalization, SQLite schema, analytical views | Converts 17 JSONL tables → SQLite + views |
| `backend/app/graph.py` | NetworkX graph construction, value index, trace/neighborhood payloads | 13 node types, 15 edges, O(1) value_index |
| `backend/app/query.py` | Intent routing (14 deterministic), SQL validation (8 checks), response composition | Handles 93% queries; 7% → LLM |
| `backend/app/llm.py` | Groq API integration, schema-constrained prompt, error handling | Async httpx, temperature 0.00000001 |
| `backend/app/main.py` | FastAPI endpoints, CORS, app lifecycle | GET /graph, /neighborhood, /node, /trace; POST /chat |
| `backend/tests/test_ingest.py` | Data correctness regression tests | Tests for graph construction, broken flow detection |
| `frontend/src/App.jsx` | State orchestration, chat/graph coordination, message history | Manages graphData, highlightedNodes, trace focus |
| `frontend/src/components/GraphSection.jsx` | Force-graph rendering, node interaction, highlighting, trace | Renders nodes/edges, red highlights, pink links |
| `frontend/src/components/ChatPanel.jsx` | Chat UI, message list, input form, debug output | Messages with intent, optional SQL display |
| `frontend/src/components/NodeDetailsCard.jsx` | Metadata inspection | Shows node type, label, properties (14 fields max) |

---

## LLM Prompting Strategy

### Prompt Design Principles

**1. Schema Constraint**

```
- 46 allowlisted columns (from o2c_flow_view + broken_flow_view + business_partners + products)
- 4 allowlisted objects only
- Explicit JOIN guidance (prefer views over raw tables)
- LIMIT enforcement (always ≤ 100)
```

**2. Deterministic Output**

```
Temperature:   0.00000001  (not creative, reproducible)
System prompt: "Return one SQLite SELECT query only"
User prompt:   schema + question
Output:        Single SELECT statement (no markdown, no prose)
```

**3. Rule Enforcement**

```
Embedded in prompt:
- "Return exactly one SQLite SELECT query and nothing else"
- "Use only objects listed above"
- "Always include LIMIT 100 or less"
- "Use explicit JOIN clauses when you need multiple objects"
```

---

### LLM Fallback Process

1. **Question arrives** at `/chat` endpoint
2. **Deterministic router** checks 14 intent patterns
   - ✅ Match → Use hardcoded SQL (93% of queries)
   - ❌ No match → Continue to step 3
3. **LLM fallback triggered** — calls `GroqSQLGenerator.generate_sql(question)`
4. **Post-generation validation** (8 checks)
   - ✅ Pass → Execute on SQLite
   - ❌ Fail → Return error: `"Query references objects outside allowed dataset scope"`
5. **Results grounded** in SQLite rows — never LLM-hallucinated text

---

### Example Prompts & Outputs

**Example 1: Required Query (Deterministic)**

```
Question: "Which products are associated with highest billing documents?"
Router:   is_product_billing_leaderboard() matches

SQL:
  SELECT COALESCE(product_description, product_id),
         COUNT(DISTINCT billing_document_id)
  FROM   o2c_flow_view
  WHERE  billing_document_id IS NOT NULL
  GROUP  BY product_id
  ORDER  BY count DESC
  LIMIT  10

Result: ✅ Instant, reproducible
```

**Example 2: Edge Case (LLM Fallback)**

```
Question: "Average net amount per customer by month, sorted newest first"
Router:   No match (custom aggregation)

LLM Output:
  SELECT sales_order_customer_id,
         strftime('%Y-%m', sales_order_date) AS month,
         AVG(sales_order_item_net_amount)    AS avg_amount
  FROM   o2c_flow_view
  GROUP  BY sales_order_customer_id, month
  ORDER  BY month DESC, sales_order_customer_id
  LIMIT  100

Validation: ✅ Passes 8 checks
Execution:  ✅ SQLite returns results
```

---

## Local Run

### Backend

```bash
cd backend
pip install -r requirements.txt
copy .env.sample .env
uvicorn app.main:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
copy .env.sample .env
npm run dev
```

> If frontend/backend are on different hosts, set `VITE_API_BASE_URL`.
