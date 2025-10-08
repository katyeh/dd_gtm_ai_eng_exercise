## dd_gtm_ai_eng_exercise

An async pipeline that:

- Scrapes Digital Construction Week speaker pages
- Classifies each speaker’s company into Builder, Owner, Partner, Competitor, or Other (with heuristics + LLM fallback)
- Generates short, personalized outbound emails for selected target categories
- Writes results to a CSV for easy review or import

## Quick start

### 1) Requirements

- Python 3.11+
- macOS/Linux/Windows

### 2) Install

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

### 3) Configure environment

Create a `.env` file in the repo root with your OpenAI credentials (the app uses Structured Outputs via the OpenAI API):

```bash
echo "OPENAI_API_KEY=sk-..." > .env
echo "OPENAI_MODEL=gpt-4o-mini" >> .env   # optional; this is the default
echo "CONCURRENCY=8" >> .env              # optional HTTP concurrency default
```

### 4) Run

End-to-end (scrape → categorize → email) for the first 20 speakers and write `out/email_list.csv`:

```bash
python main.py --stage all --limit 20
```

Preview only (don’t write CSV; print a few examples):

```bash
python main.py --stage email --dry-run --limit 10
```

Target only Builders and Owners (default). Add Partners or Other as needed; Competitors are always excluded from emailing:

```bash
python main.py --stage email --targets "builders, owners, partners"
```

## CLI options

`main.py` exposes the following flags:

- `--limit INT` (default: 20): Max number of speakers to process from the index.
- `--http INT` (default: `$CONCURRENCY` or 8): HTTP concurrency (async semaphore).
- `--llm INT` (default: 6): Max concurrent LLM calls for categorize/email stages.
- `--stage {scrape,categorize,email,all}` (default: `all`): Which part of the pipeline to run.
- `--targets STRING` (default: `builders, owners`): Comma-separated list from {builders, owners, partners, competitors, other}. Note that competitors are filtered out at email time regardless.
- `--dry-run` (flag): Print a few email samples instead of writing CSV.

On success, the script prints a summary like:

```
✓ Wrote out/email_list.csv with N rows (Builder/Owner only).
```

## How it works

### Scrape (stage: `scrape`)

- Fetches the DCW speakers index at `https://www.digitalconstructionweek.com/all-speakers/`.
- Parses links to individual speaker pages, then fetches and extracts name/title/company/bio.
- Appends normalized records to `in/speakers_enriched.jsonl` for checkpointing and idempotency.
  - Code: `utils/http.py`, `utils/parsing.py`, `utils/pipeline.py` (`scrape_all`).

### Categorize (stage: `categorize`)

Company category is assigned via:

1. Heuristics: keyword-based detection on company/title/bio
2. LLM fallback: OpenAI Structured Outputs to `Categorization` schema

Results are appended to `in/speakers_categorized.jsonl` with metadata such as `decision_source` and `categorized_at`.

- Code: `utils/classify.py`, `utils/models.py`, `utils/pipeline.py` (`categorize_all`).

### Email generation (stage: `email`)

- Uses the assigned `company_category` and a single specific detail from bio/talk title to draft 1 concise email per selected target category.
- Outputs a CSV at `out/email_list.csv` with columns:
  - Speaker Name, Title, Company, Company Category, Email Subject, Email Body
- Code: `utils/emailgen.py`, `utils/models.py`, `utils/pipeline.py` (`generate_emails`).

## Data files

- Input checkpoints
  - `in/speakers_enriched.jsonl`: raw-enriched speakers from scraping
  - `in/speakers_categorized.jsonl`: speakers with assigned company category
- Output
  - `out/email_list.csv`: final email drafts for selected targets

## Targeting behavior

- Accepted targets: `builders`, `owners`, `partners`, `competitors`, `other` (comma-separated)
- Hard rule: emails are never generated for `Competitor`, even if included in `--targets`.

## Testing

Run the small test suite:

```bash
pytest -q
```

- Tests cover heuristic classification and schema generation.

## Implementation notes

- Concurrency
  - HTTP concurrency is controlled by `--http` (and an internal semaphore); defaults to `CONCURRENCY` env or 8.
  - LLM concurrency is controlled by `--llm` (default 6).
- Idempotency
  - Scraper and categorizer skip URLs already present in their respective checkpoint files.
- OpenAI usage
  - The app uses the OpenAI API with Structured Outputs to parse JSON directly into Pydantic models.
  - Configure `OPENAI_API_KEY` in `.env`. `OPENAI_MODEL` is optional; defaults to `gpt-4o-mini`.

## Troubleshooting

- 403 or HTML fetch issues: re-run with a smaller `--http`, or retry later.
- Empty CSV: ensure `--stage email` ran and categories matched your `--targets`.
- Rate limits: lower `--llm` concurrency.
- Encoding: files are written/read as UTF‑8.

## Repository layout

```
in/                      # input data
  speakers_enriched.jsonl
  speakers_categorized.jsonl
out/                     # generated outputs
  email_list.csv
utils/                   # core modules
  classify.py            # heuristics + LLM fallback
  emailgen.py            # personalized email drafting
  http.py                # robust async HTTP client
  io.py                  # JSONL append, CSV writer
  models.py              # Pydantic models and enums
  parsing.py             # HTML parsing for index + detail pages
  pipeline.py            # orchestrates stages and concurrency
main.py                  # CLI entrypoint
requirements.txt
```

## Notes

- Use responsibly. Respect robots and terms of the source site.
- License: not specified.
