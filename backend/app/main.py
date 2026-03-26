from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .graph import build_graph, neighborhood_subgraph, serialize_overview_graph, trace_subgraph
from .ingest import rebuild_database
from .llm import GroqSQLGenerator
from .query import (
    DOMAIN_MESSAGE,
    build_query_plan,
    collect_highlight_nodes,
    compose_answer,
    execute_query,
    infer_trace_node_id,
    is_greeting_query,
    is_domain_query,
    summarize_rows,
    validate_sql,
)
from .schemas import ChatRequest, ChatResponse, GraphResponse, NodeResponse, TraceResponse


def bootstrap_state(app: FastAPI) -> None:
    connection = rebuild_database(settings.data_dir, settings.sqlite_path)
    graph, value_index = build_graph(connection)
    app.state.connection = connection
    app.state.graph = graph
    app.state.value_index = value_index
    app.state.sql_generator = GroqSQLGenerator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    bootstrap_state(app)
    try:
        yield
    finally:
        connection: sqlite3.Connection | None = getattr(app.state, "connection", None)
        if connection is not None:
            connection.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/graph", response_model=GraphResponse)
def get_graph() -> dict[str, Any]:
    return serialize_overview_graph(app.state.graph)


@app.get("/graph/neighborhood", response_model=GraphResponse)
def get_graph_neighborhood(node_id: str = Query(...), depth: int = Query(1, ge=1, le=2)) -> dict[str, Any]:
    payload = neighborhood_subgraph(app.state.graph, node_id, depth)
    if not payload["nodes"]:
        raise HTTPException(status_code=404, detail="Node not found.")
    return payload


@app.get("/node/{node_id:path}", response_model=NodeResponse)
def get_node(node_id: str) -> dict[str, Any]:
    graph = app.state.graph
    if not graph.has_node(node_id):
        raise HTTPException(status_code=404, detail="Node not found.")
    data = graph.nodes[node_id]
    return {
        "id": node_id,
        "type": data["type"],
        "label": data.get("label", node_id),
        "properties": data["properties"],
    }


@app.get("/trace", response_model=TraceResponse)
def get_trace(node_id: str) -> dict[str, Any]:
    payload = trace_subgraph(app.state.graph, node_id)
    if not payload["nodes"]:
        raise HTTPException(status_code=404, detail="Node not found.")
    return payload


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest) -> dict[str, Any]:
    if not is_greeting_query(request.question) and not is_domain_query(request.question):
        raise HTTPException(status_code=400, detail=DOMAIN_MESSAGE)

    try:
        plan = await build_query_plan(request.question, app.state.sql_generator)
        if plan.sql is None:
            return {
                "question": request.question,
                "intent": plan.intent,
                "answer": plan.answer or "I can help with Order-to-Cash questions from this dataset.",
                "sql": "",
                "rows": [],
                "nodes_to_highlight": [],
                "trace_node_id": plan.trace_node_id,
            }

        validated_sql = validate_sql(plan.sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to generate SQL: {exc}") from exc

    try:
        rows = execute_query(app.state.connection, validated_sql)
    except sqlite3.Error as exc:
        raise HTTPException(status_code=400, detail=f"Query execution failed: {exc}") from exc

    trace_node_id = plan.trace_node_id or infer_trace_node_id(request.question, rows)

    return {
        "question": request.question,
        "intent": plan.intent,
        "answer": compose_answer(request.question, plan.intent, rows),
        "sql": validated_sql,
        "rows": summarize_rows(rows, max_rows=12),
        "nodes_to_highlight": collect_highlight_nodes(rows, app.state.value_index),
        "trace_node_id": trace_node_id,
    }
