import asyncio, os
from dotenv import load_dotenv
from utils.logging_setup import setup_logging
from utils.pipeline import Pipeline

if __name__ == "__main__":
    import argparse
    setup_logging(); load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--http", type=int, default=int(os.getenv("CONCURRENCY", 8)))
    ap.add_argument("--llm", type=int, default=6) # controls the number of concurrent LLM requests
    ap.add_argument("--stage", default="all", choices=["scrape", "categorize", "email", "all"])
    ap.add_argument("--targets", default="builders, owners")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    async def run():
        p = Pipeline(http_concurrency=args.http)
        rows = await p.run(limit=args.limit, llm_c=args.llm, stage=args.stage, targets=args.targets, dry_run=args.dry_run)
        print(f"✓ Wrote out/email_list.csv with {len(rows)} rows (Builder/Owner only).")
    asyncio.run(run())
