import json
import pandas as pd
from typing import Any
import aiofiles
from .models import RowOut, Speaker
from typing import Iterable
import os

async def append_jsonl(path: str, obj: dict[str, Any]) -> None:
    # Ensure parent directory exists for first-run resilience
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    async with aiofiles.open(path, "a", encoding="utf-8") as f:
        await f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def load_jsonl(path: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []

def write_csv(path: str, rows: Iterable[RowOut]) -> None:
    """Write rows to CSV using pandas for cleaner deduplication."""
    # Convert RowOut objects to dicts for DataFrame
    new_data = [
        {
            "Speaker Name": r.speaker_name,
            "Speaker Title": r.speaker_title,
            "Speaker Company": r.speaker_company,
            "Company Category": r.company_category.value if hasattr(r.company_category, 'value') else str(r.company_category),
            "Email Subject": r.email_subject,
            "Email Body": r.email_body,
        }
        for r in rows
    ]
    
    if not new_data:
        return
    
    new_df = pd.DataFrame(new_data)
    
    # If file exists, load and merge with deduplication
    if os.path.exists(path) and os.path.getsize(path) > 0:
        existing_df = pd.read_csv(path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        combined_df = combined_df.drop_duplicates(
            subset=["Speaker Name", "Speaker Company"],
            keep="first"
        )
        combined_df.to_csv(path, index=False)
    else:
        new_df.to_csv(path, index=False)


def filter_speakers_missing_fields(speakers: list[Speaker], required_fields: list[str]) -> list[Speaker]:
    """Filter speakers that are missing any of the specified required fields.
    
    Useful for batch processing - identify speakers that need enrichment.
    
    Example:
        missing = filter_speakers_missing_fields(speakers, ["company", "title"])
        # Returns speakers where company OR title is None/empty
    """
    df = pd.DataFrame([s.model_dump() for s in speakers])
    mask = df[required_fields].isna().any(axis=1)
    missing_indices = df[mask].index.tolist()
    return [speakers[i] for i in missing_indices]