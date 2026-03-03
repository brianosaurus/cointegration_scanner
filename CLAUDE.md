# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Solana arbitrage tracker — read-only, post-hoc analysis of on-chain arbitrage transactions. Pure Python 3, no compilation step.

**Stack:** Python 3 · gRPC (Geyser) · Protocol Buffers · SQLite (WAL mode) · asyncio

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Real-time block following
python tracker.py --follow

# Historical slot range scan
python tracker.py --slot-range 300000000-300001000

# Filter by wallet
python tracker.py --follow --signer <ADDRESS>

# Debug logging
python tracker.py --follow --verbose

# Regenerate protobuf stubs (if .proto files change)
# geyser_pb2.py, geyser_pb2_grpc.py, solana_storage_pb2.py, solana_storage_pb2_grpc.py
python -m grpc_tools.protoc ...
```

No test suite yet. No linter configured.

## Architecture

**Data flow:** gRPC block stream → transaction filtering → swap detection → arbitrage classification → SQLite storage + console display

Key modules:
- **tracker.py** — CLI entry point. `--follow` (real-time) or `--slot-range` (historical). Handles Ctrl+C gracefully, prints stats on exit.
- **block_fetcher.py** — gRPC streaming via Geyser. `follow_confirmed()` for real-time, `fetch_slot_range()` for history. Auto-reconnect with exponential backoff.
- **transaction_analyzer.py** — Core arbitrage detection. Extracts swap sequences, checks circular token flows, computes net profit/loss. Key dataclasses: `SwapLeg`, `ArbitrageTransaction`.
- **swap_detector.py** — Identifies swaps across 10+ DEX protocols (Raydium, Orca, Meteora, PumpSwap, Jupiter, Phoenix, OpenBook, Serum). Uses instruction discriminators (first 8 bytes) for protocol identification.
- **db.py** — SQLite layer. Tables: `arbitrage_transactions`, `swap_legs`, `scan_progress`. Supports resumable scans.
- **constants.py** — DEX program IDs, token mints, known bot wallets (170+), Jito tip accounts, swap discriminators.
- **config.py** — Loads `.env` via python-dotenv. Requires `GRPC_ENDPOINT` and `GRPC_TOKEN`.
- **grpc_utils.py** — Helpers for signer extraction, address parsing, bot filtering, Jito detection.
- **display.py** — Console formatting with Solscan links.

**Key design decisions:**
- Read-only: no signing, no transaction execution
- gRPC streaming over HTTP polling for real-time data
- Token-agnostic circular flow detection (not just stablecoins)
- Protobuf stubs are checked in (generated files: `*_pb2.py`, `*_pb2_grpc.py`)

## Workflow Orchestration

1. Plan Mode Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately – don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One task per subagent for focused execution

3. Self-Improvement Loop
- After ANY correction from the user: update `tasks/lessons .md` with the pattern
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant project

4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

5. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes – don't over-engineer
- Challenge your own work before presenting it

6. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests – then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how

Task Management

1. **Plan First**: Write plan to `tasks/todo .md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo .md`
6. **Capture Lessons**: Update `tasks/lessons .md` after corrections

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.

