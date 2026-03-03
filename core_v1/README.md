# AOM Outreach Core V1

Clean rebuild of the outreach system with a single-pass writer architecture.

## What this version does

- One `ResearchCard` per lead
- Writer only personalizes paragraph 1 (opener)
- Paragraphs 2 and 3 are fixed deterministic lines
- Minimal hard checks only (truth, formatting, fixed intent anchors)
- Skip weak/generic leads instead of rewrite loops
- Supports both new outreach and rewrite of existing drafts

## Setup

```bash
cd core_v1
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Make sure these files exist in `core_v1/`:
- `.env`
- `client_secret.json`
- `gmail_tokens.json`

If Gmail auth fails due token format, regenerate tokens:
```bash
python3 reauth_gmail.py
```

## Usage

### Debug writer (safe, no Gmail mutation)
```bash
python3 pipeline.py --mode rewrite --debug-writer --debug-writer-limit 3
```
When a lead fails, the pipeline prints the failed email body in terminal for fast review.

### Rewrite existing outreach drafts
```bash
python3 pipeline.py --mode rewrite --rewrite-limit 10
python3 pipeline.py --mode rewrite --rewrite-all
```

### New outreach from Apollo
```bash
python3 pipeline.py --mode new --new-source apollo --new-limit 25
```

### New outreach from CSV
```bash
python3 pipeline.py --mode new --new-source csv --new-csv /path/to/leads.csv
```

## Output

- Logs: `core_v1/logs/`
- Writer debug JSONL: `core_v1/logs/debug/`
- CSV exports: `core_v1/daily_exports/`
- Length check is warning-only (`>120` words logs a warning, does not auto-skip)

## Legacy Carryover

On first run, `core_v1` auto-seeds its `daily_exports` from the parent project's legacy `daily_exports` folder.
This preserves your historical contact list for rewrite matching (including your previous 36/37 leads).
