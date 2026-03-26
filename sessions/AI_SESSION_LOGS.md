# AI-Assisted Development Session Log

**Tools:** Free Tier OpenAI Codex App (Windows), Free Tier GitHub Copilot (VS Code)

---

## How I Used AI Tools

I built this project end-to-end — SQLite ingestion pipeline, graph construction, hybrid query router, Groq LLM integration, and React frontend. AI assistance accelerated execution; every architecture call, bug diagnosis, and validation design was mine.

**Phase 0 — Structured planning before any code.** Before writing anything I summarized the full problem state into Codex Plan Mode: input format (multi-document JSONL), output requirements (graph visualization + chat over O2C entities), constraints (deterministic queries, injection-safe SQL, guardrails), and evaluation criteria. This produced a sequenced workstream — ingest → graph → query routing → LLM fallback → frontend — that meant I never had to rediscover scope mid-build. Every AI interaction that followed had a defined input and a success condition I'd set in advance.

**Phase 1 — Building with AI as a consistency check.** As I constructed each layer I used Copilot to cross-check API contracts between FastAPI and React and catch type or shape mismatches before they propagated. The data flow (JSONL → SQLite → Graph → Chat API) was mine to design; AI helped me verify I'd implemented what I intended.

**Phase 2 — Code review, then targeted fixes.** I reviewed my own code and identified the issues. I used AI to pressure-test my fix hypotheses and implement the minimal change cleanly — not to find bugs for me.

**Phase 3 — Acceptance matrix I designed and ran.** I structured the validation myself: 3 required queries, 5 in-domain paraphrases, 5 off-domain prompts, each returning status and evidence. AI helped format the output capture. The matrix design was what surfaced the direct-billing gap — informal testing had missed it entirely.

**Phase 4 — Documentation as a completeness check.** I wrote the submission artifacts. AI flagged gaps against the evaluation criteria I'd specified upfront.

---

## Key Workflows

**Front-loaded problem decomposition.** Investing time upfront to map inputs, constraints, and success criteria — before any implementation — meant I caught architecture questions at planning cost, not debugging cost. The hybrid routing split (14 deterministic intents + Groq fallback) came directly out of this phase, not from iteration.

**Understand before changing.** My consistent pattern throughout: describe what the code actually does, reason about the change's impact, then write the minimal patch. When I hit the trace algorithm issue, this is what stopped me from patching symptoms — I understood the directed/undirected distinction clearly enough to rethink the algorithm rather than add edge cases.

**Fix once, lock permanently.** Every bug got a regression test immediately after the fix — something that would have caught it originally. By the end, the test suite was doing quality control I didn't have to think about. Later refactors didn't reopen earlier bugs.

**Structured validation over spot checks.** Formalizing the acceptance matrix as a required-query × prompt-variant grid made coverage gaps visible rather than assumed. This is a habit I'd carry into any customer-facing deployment.

---

## What I Debugged and How

**Journal-to-billing mapping.** During graph construction review I noticed multi-line invoices were only partially linked — my join logic was single-row, so only the first billing item per invoice was getting an edge. I expanded the join to fan out across all billing items for a given invoice document. Wrote `test_journal_to_billing_expansion()` immediately to lock it. This had downstream impact on broken-flow detection accuracy.

**Trace algorithm.** I built a directed ancestor/descendant trace first — the obvious starting point. During integration testing I saw it was dropping lateral context that mattered for O2C flows. I stepped back, rethought the algorithm rather than patching it, and switched to an undirected 4-hop radius. That decision — rethink vs. patch — kept the implementation clean rather than accumulating special cases.

**Broken-flow detection coverage.** My initial flow view assumed every path included a delivery step. The acceptance matrix caught that direct SO→Billing paths weren't surfacing as broken — something informal testing had missed because I'd been testing the happy path variants. Extended the view logic, re-ran the matrix, confirmed all three broken-flow types detected.

**SQLite concurrency (local).** Concurrent test processes were hitting lock conflicts. I isolated each test to its own DB instance with explicit teardown. Not a production concern but worth fixing so local test output was trustworthy — a flaky test suite erodes confidence in the coverage that actually matters.

---
