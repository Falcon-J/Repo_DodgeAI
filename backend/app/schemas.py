from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EdgeResponse(BaseModel):
    source: str
    target: str
    type: str


class NodeResponse(BaseModel):
    id: str
    type: str
    label: str
    properties: dict[str, Any]


class GraphResponse(BaseModel):
    nodes: list[NodeResponse]
    edges: list[EdgeResponse]


class ChatRequest(BaseModel):
    question: str = Field(min_length=3)


class ChatResponse(BaseModel):
    question: str
    intent: str
    answer: str
    sql: str
    rows: list[dict[str, Any]]
    nodes_to_highlight: list[str]
    trace_node_id: str | None = None


class TraceResponse(BaseModel):
    nodes: list[NodeResponse]
    edges: list[EdgeResponse]
    path_types: list[str]
