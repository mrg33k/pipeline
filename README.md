# Ahead of Market - Cold Outreach Pipeline

Automated cold outreach pipeline for Patrik Matheson at [Ahead of Market](https://aheadofmarket.com). Searches for local business leads via Apollo, writes personalized emails using AI, and creates Gmail drafts for manual review and sending.

## How It Works

1. **Apollo Search (free):** Runs targeted keyword searches across 10 industry categories in the Phoenix metro area. No credits consumed.
2. **LLM Filtering:** Uses `gpt-4.1-mini` to rank and select the top 25 prospects from the search results.
3. **Apollo Enrichment (25 credits):** Gets full contact details and email addresses for only the selected 25.
4. **Research Card Build:** Collects website/LinkedIn/review signals and normalizes one `ResearchCard` per lead.
5. **Intent-Anchor Rewrite Writing:** Uses `ResearchCard` + `IntentAnchorCard` to run one natural rewrite pass, then one targeted retry only if semantic anchors are missing.
6. **Minimal Hard Checks:** Enforces greeting/signoff formatting, `web/social` ask, non-assumptive intent, soft nearby/Zoom close, truthful source claims, and `<=100` words.
7. **Gmail Drafts:** Creates drafts in Gmail. You review and hit send manually.
8. **CSV Export:** Saves a daily CSV of all contacts and emails for record keeping.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API Keys

Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
```

Edit `.env`:
```
APOLLO_API_KEY=your_apollo_key_here
OPENAI_API_KEY=your_openai_key_here
```

### 3. Enable Gmail API

Go to the [Google Cloud Console](https://console.developers.google.com/apis/api/gmail.googleapis.com/overview?project=618386231675) and enable the Gmail API for your project. The OAuth credentials (`client_secret.json` and `gmail_tokens.json`) should already be in the project directory.

### 4. Run the Pipeline

```bash
# Full run: opens startup browser form, then search/filter/enrich/write/draft
python3 run_pipeline.py

# Dry run: everything except creating Gmail drafts
python3 run_pipeline.py --dry-run

# Limit to 10 emails
python3 run_pipeline.py --max 10

# Skip draft creation (same as dry-run)
python3 run_pipeline.py --skip-drafts

# Skip browser UI and run directly from CLI/defaults
python3 run_pipeline.py --no-ui
```

## Daily Usage

Run once per day:
```bash
python3 run_pipeline.py
```

The pipeline will:
- Skip anyone already in `contacts_history.json`
- Generate up to 25 new drafts
- Export a CSV to `daily_exports/`
- Log everything to `logs/`
- Let you adjust run-only AI directions, model, pages, and count in the startup browser form

## Credit Usage

| Service | Daily Cost | Notes |
|---------|-----------|-------|
| Apollo Search | 0 credits | People Search is free |
| Apollo Enrichment | 25 credits | 1 per person enriched |
| OpenAI (gpt-4.1-mini) | ~$0.02 | 1 filter call + 25 email calls |

Total Apollo cost: **25 credits/day** (well under the 100/day budget).

## File Structure

```
outreach_pipeline/
├── run_pipeline.py       # Main entry point
├── runtime_settings.py   # Per-run settings dataclass + validation
├── startup_ui.py         # Local browser UI for startup run configuration
├── config.py             # All settings and constants
├── apollo_client.py      # Apollo API (search + enrichment)
├── llm_filter.py         # LLM-based prospect ranking
├── research.py           # Context gatherer + normalized ResearchCard builder
├── email_writer.py       # Single-pass writer from ResearchCard + minimal hard checks
├── gmail_drafter.py      # Gmail draft creation
├── csv_export.py         # Daily CSV export
├── contacts_db.py        # Contact history tracking
├── live_test.py          # Live test script (5 contacts)
├── requirements.txt      # Python dependencies
├── .env.example          # API key template
├── client_secret.json    # Gmail OAuth credentials
├── gmail_tokens.json     # Gmail OAuth tokens
├── contacts_history.json # Auto-generated contact tracking
├── daily_exports/        # Auto-generated CSV exports
└── logs/                 # Auto-generated run logs
```

## Customization

Edit `config.py` to change:
- **Target titles** (`PERSON_TITLES`)
- **Target locations** (`ORGANIZATION_LOCATIONS`)
- **Industry keywords** (`INDUSTRY_KEYWORDS`)
- **Company size** (`EMPLOYEE_RANGES`)
- **Daily batch size** (`MAX_DAILY_EMAILS`)
- **Email signature** (`EMAIL_SIGNATURE_HTML`)

Edit `email_writer.py` to change:
- Writing style rules
- Banned words/phrases
- Email structure and tone

## Repo Notes

- Recommended dev flow: run `--debug-writer --debug-writer-limit 3` first, then run rewrite/full pipeline.
- Production drafting path is intentionally single-pass with minimal hard checks to reduce looped rewrites.
