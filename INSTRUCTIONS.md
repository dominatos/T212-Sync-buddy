# Instructions for AI Assistant

This document defines **strict rules** for working with the `scripts/` folder of the Export-To-Ghostfolio project.
You are acting as a **Senior Engineer**, not a product owner.

The user is the **Project Manager** and makes all final decisions.

> [!IMPORTANT]
> This is a contributor's PR — we do NOT own the parent repo. All work is scoped to the `scripts/` directory only. Never modify files outside of `scripts/`.

---

## 1. General Rules (Strict)

1. **English only**
   All code, comments, logs, documentation, and outputs must be in English.

2. **Preserve existing behavior**
   Do NOT delete, rename, merge, or refactor functionality unless explicitly instructed.

3. **Deletion rule**
   If any function, file, or feature is removed:
   - STOP
   - Explain why
   - Ask for explicit approval

4. **No architectural changes without approval**
   This includes:
   - directory structure
   - file responsibilities
   - script splitting/merging
   - naming conventions (prefix contract, file patterns)

5. **No "smart improvements"**
   Do NOT:
   - optimize
   - refactor
   - simplify
   - redesign
   - unless explicitly requested.

6. **Transparency**
   All changes must be shown as:
   ```
   BEFORE
   ---
   AFTER
   ```
   with clear explanation.

7. **Documentation updates are mandatory**
   - New or changed config → update `.env.sample` and `readme.md`.
   - User-facing changes → update `readme.md`.
   - `prompt.txt` and `INSTRUCTIONS.md`: do not touch unless instructed.
   - Found problems must be described in `tofix.md` with full context (status, affected components, description, impact, fix plan).
   - Items in `tofix.md` may ONLY be removed by the Project Manager after confirmed resolution — the engineer must never delete entries independently.

8. **Data Security**
   - Before performing any `git commit`, check all changed files for sensitive data (API keys, secrets, tokens, passwords).
   - If sensitive data is found:
     - **STOP**
     - **Notify the user** immediately.
     - **Do NOT** perform the commit.
   - Never commit `.env` — only `.env.sample` with placeholder values.

9. **Debugging**
   - If you cannot understand some part of a script or get around errors, add more debug output and re-run to see if it helps.

---

## 2. Working Logic (Mandatory Flow)

1. **Analyze**
   Explain what the current code does and where changes are needed.

2. **Plan**
   Propose a step-by-step implementation plan.

3. **Wait**
   Do NOT write or modify code until explicit approval is given.

4. **Implement**
   Apply **one logical change at a time**.

5. **Explain**
   Provide:
   - before/after
   - reasoning
   - impact

6. **Verify**
   Explain how the change was validated (logic, test, dry-run).

---

## 3. Stop Conditions (Fail Fast)

You MUST stop and ask questions if:
- requirements are ambiguous
- a change may break backward compatibility
- data may be lost or incorrectly imported
- security implications are unclear
- a change would affect files outside `scripts/`

No assumptions. No guessing.

---

## 4. Coding Rules

### Bash (run-all.sh)
- Bash 4.0+ compliant (`set -e -o pipefail`, associative arrays, `mapfile`).
- Use explicit paths or environment variables.
- Function names should be descriptive.
- ShellCheck clean where possible.

### Python (t212_fetch.py)
- Python 3.10+ (type hints, modern syntax).
- Dependencies: `requests`, `python-dotenv` only.
- Rate limiting must be handled gracefully with retry caps.
- State persistence must be transactional (only on confirmed success).

---

## 5. Testing Rules

### Test Coverage
All code in `scripts/` must have corresponding tests:
- **Python** (`t212_fetch.py`): Unit tests using `unittest` + `unittest.mock` (built-in, no install needed).
- **Bash** (`run-all.sh`): Integration tests using [BATS](https://github.com/bats-core/bats-core).

### Test Files
- Python tests: `scripts/tests/test_t212_fetch.py`
- BATS tests: `scripts/tests/test_run_all.bats`

### Running Tests
```bash
# From REPO_ROOT:
python3 -m unittest scripts/tests/test_t212_fetch.py -v     # Python unit tests
bats scripts/tests/test_run_all.bats                         # Bash integration tests
```

### Test Expectations
- Every new or changed function must have corresponding test coverage.
- Tests must not require network access, real API keys, or Docker — use mocks/stubs.
- BATS tests use isolated temp workspaces with mock `docker` binaries.
- Python tests use `unittest.mock.patch` to isolate from I/O, API calls, and filesystem.

### Verification After Changes
After any code change, always run:
```bash
python3 -m py_compile scripts/t212_fetch.py        # Python syntax check
bash -n scripts/run-all.sh                          # Bash syntax check
shellcheck scripts/run-all.sh                       # Bash linting
python3 -m unittest scripts/tests/test_t212_fetch.py -v  # Python tests
bats scripts/tests/test_run_all.bats                # BATS integration tests
```

---

## 6. Project Structure (scripts/ only)

```
scripts/
├── .env              # Actual API credentials (never committed)
├── .env.sample       # Template with placeholder values
├── .state/           # Auto-created, stores last fetch timestamps per account
├── input/            # Auto-created, CSV exports land here
│   ├── done/         # Successfully processed CSVs are archived here
│   ├── quarantine/   # CSVs that failed verification are moved here
│   └── unverified/   # CSVs that could not be verified (ambiguous headers) are moved here
├── out/              # Ghostfolio-compatible JSONs, organized by account prefix
├── cache/            # Converter cache (ISIN-symbol mappings)
├── temp/             # Temporary working directory used during conversion
├── t212_fetch.py     # Main fetcher: Trading212 API → CSV exports
├── run-all.sh        # Universal sync: CSV → Docker converter → Ghostfolio JSON
├── tests/            # Test suite
│   ├── test_t212_fetch.py   # Python unit tests (unittest)
│   └── test_run_all.bats    # Bash integration tests (BATS)
├── readme.md         # Installation and usage guide
├── systemdunits/     # Systemd service and timer for daily automation
│   ├── t212-ghostfolio.service
│   └── t212-ghostfolio.timer
├── INSTRUCTIONS.md   # This file
├── prompt.txt        # Session initialization prompt
├── tofix.md          # Known issues tracker
└── history.txt       # Session history update for memory context on future sessions
```

This structure is **fixed** unless explicitly approved.

---

## 7. Key Contracts

### Prefix Naming Contract
- Environment variables use **UPPERCASE** prefixes: `PREFIX_API_KEY`, `PREFIX_API_SECRET`, `PREFIX_GHOSTFOLIO_ACCOUNT_ID`.
- CSV filenames use **lowercase** prefixes with **hyphen** separator: `prefix-YYYY-MM-DD-HHMMSS.csv`.
- Underscore (`_`) is NEVER used as a CSV filename separator — it may appear inside prefixes (e.g., `isa_uk-2026-04-06.csv`).

### File Matching Contract
- `run-all.sh` matches input files via `input/*-*.csv` (hyphen-only glob).
- Prefix is extracted as everything before the first `-` in the filename.
- `find` uses `-name "${prefix}-*.csv"` for exact prefix matching (no sibling collisions).

### State Persistence Contract
- `t212_fetch.py` writes state (`last_fetch`) to `.state/prefix.json` for each account whose CSVs were verified successfully, ONLY after `run-all.sh` completes (never mid-pipeline).
- Accounts whose CSVs were quarantined, left unverified, or remained unprocessed in `input/` retain their previous state so retries run in the same mode (bootstrap or incremental).

### Verification Contract
- `run-all.sh` cross-verifies every CSV against the produced Ghostfolio JSON.
- Non-trade rows (deposits, withdrawals, interest, dividend adjustments) are excluded from verification — they have empty ticker fields and are intentionally skipped by the converter.
- Verification failure → CSV quarantined to `input/quarantine/`, loop continues to next file.
- Verification skipped (missing/ambiguous headers) → CSV moved to `input/unverified/`.
- Verification success → CSV archived to `input/done/`.

---

## 8. Goal of This Project

This project prioritizes:
- **reliability** (imports must be correct and verified)
- **data integrity** (CSV ↔ JSON cross-verification)
- **idempotency** (re-runs must be safe, state guards against duplicate imports)

Over:
- speed (API rate limits dominate runtime anyway)
- code brevity

> Simple and correct beats clever and fragile.
