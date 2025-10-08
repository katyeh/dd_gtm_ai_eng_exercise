import os, json, asyncio
import pytest

from utils.pipeline import Pipeline
from utils.models import Speaker, EmailDraft, CompanyCategory


class FakeEmailer:
    def __init__(self, sem, model=None):
        self.sem = sem
        self.model = model
    async def draft(self, sp: Speaker) -> EmailDraft:
        # Deterministic draft for testing
        return EmailDraft(subject=f"Subject for {sp.name}", body=f"Body for {sp.name}")


@pytest.mark.asyncio
async def test_email_checkpoint_and_csv_materialization(tmp_path, monkeypatch):
    # Use an isolated working directory with expected folder structure
    monkeypatch.chdir(tmp_path)
    os.makedirs("in", exist_ok=True)
    os.makedirs("out", exist_ok=True)

    # Seed speakers_enriched.jsonl with two speakers that will be categorized by heuristics
    enriched_path = tmp_path / "in" / "speakers_enriched.jsonl"
    speakers_seed = [
        {
            "name": "Alice",
            "title": "PM",
            "company": "Skanska",  # builder keyword triggers heuristic
            "bio": None,
            "talk_titles": [],
            "url": "https://example.com/speakers/alice",
            "company_category": None,
        },
        {
            "name": "Bob",
            "title": "Director",
            "company": "Network Rail",  # owner keyword triggers heuristic
            "bio": None,
            "talk_titles": [],
            "url": "https://example.com/speakers/bob",
            "company_category": None,
        },
    ]
    with open(enriched_path, "w", encoding="utf-8") as f:
        for rec in speakers_seed:
            f.write(json.dumps(rec) + "\n")

    # Monkeypatch Emailer class used by Pipeline to avoid real API calls
    import utils.pipeline as up
    monkeypatch.setattr(up, "Emailer", FakeEmailer)

    p = Pipeline(http_concurrency=2)

    # First run: should generate two emails, checkpoint to JSONL, and materialize CSV
    rows1 = await p.run(limit=None, llm_c=2, stage="email", targets=None, dry_run=False)
    # JSONL should have two unique lines
    emails_jsonl = tmp_path / "in" / "emails.jsonl"
    with open(emails_jsonl, "r", encoding="utf-8") as f:
        lines = [json.loads(x) for x in f if x.strip()]
    assert len(lines) == 2
    # CSV should contain header + 2 rows
    csv_path = tmp_path / "out" / "email_list.csv"
    with open(csv_path, "r", encoding="utf-8") as f:
        csv_lines = [x for x in f if x.strip()]
    assert len(csv_lines) == 3

    # Second run: should skip already checkpointed speakers; JSONL count unchanged; CSV still 2 rows
    rows2 = await p.run(limit=None, llm_c=2, stage="email", targets=None, dry_run=False)
    with open(emails_jsonl, "r", encoding="utf-8") as f:
        lines2 = [json.loads(x) for x in f if x.strip()]
    assert len(lines2) == 2
    with open(csv_path, "r", encoding="utf-8") as f:
        csv_lines2 = [x for x in f if x.strip()]
    assert len(csv_lines2) == 3

    # Ensure pipeline returns rows (from last materialization) with matching count
    assert len(rows1) == 2
    assert len(rows2) == 2
