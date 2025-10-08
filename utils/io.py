import csv
import json, pandas as pd
from typing import Any
import aiofiles
from .models import RowOut
from typing import Iterable
import os

async def append_jsonl(path: str, obj: dict[str, Any]) -> None:
    async with aiofiles.open(path, "a", encoding="utf-8") as f:
        await f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def load_jsonl(path: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []


def write_csv(path: str, rows: Iterable[RowOut]) -> None:
    """Append rows to CSV by default, writing a header if the file is new/empty.

    Also de-duplicates against existing rows by (Speaker Name, Speaker Company).
    """
    # Determine whether we need to write the header
    need_header = True
    existing_keys: set[tuple[str | None, str | None]] = set()
    if os.path.exists(path) and os.path.getsize(path) > 0:
        need_header = False
        # Build a set of existing keys to avoid duplicates on append
        try:
            with open(path, "r", newline="", encoding="utf-8") as f_in:
                r = csv.DictReader(f_in)
                for rrow in r:
                    existing_keys.add((rrow.get("Speaker Name"), rrow.get("Speaker Company")))
        except Exception:
            existing_keys = set()

    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if need_header:
            w.writerow([
                "Speaker Name",
                "Speaker Title",
                "Speaker Company",
                "Company Category",
                "Email Subject",
                "Email Body",
            ])
        for row in rows:
            key = (row.speaker_name, row.speaker_company)
            if key in existing_keys:
                continue
            w.writerow([
                row.speaker_name,
                row.speaker_title,
                row.speaker_company,
                row.company_category,
                row.email_subject,
                row.email_body,
            ])