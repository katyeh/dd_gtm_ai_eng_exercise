import os
import asyncio
from .http import Http
from .parsing import parse_index, parse_speaker_detail, BASE, SpeakerSeed, extract_session_links_from_speaker, parse_session_title
from .io import append_jsonl, load_jsonl, write_csv
from .models import Speaker, CompanyCategory, RowOut
from .emailgen import Emailer

INDEX = f"{BASE}/all-speakers/"

def _targets_from_str(s: str | None) -> set[CompanyCategory]:
    if not s:
        return {CompanyCategory.builder, CompanyCategory.owner}
    m = {
        "builders": CompanyCategory.builder,
        "owners": CompanyCategory.owner,
        "partners": CompanyCategory.partner,
        "competitors": CompanyCategory.competitor,
        "other": CompanyCategory.other,
    }
    out = {m[x.strip().lower()] for x in s.split(",") if x.strip().lower() in m}
    out.discard(CompanyCategory.competitor)  # never email competitors
    return out or {CompanyCategory.builder, CompanyCategory.owner}

async def _parse_detail_with_fallbacks(html: str, url: str, seed: SpeakerSeed) -> Speaker:
    sp = parse_speaker_detail(html, url, fallback_name=seed.name)
    # Seed any missing fields from the index card
    sp.title = sp.title or seed.title
    sp.company = sp.company or seed.company
    return sp

class Pipeline:
    def __init__(self, http_concurrency: int = 8):
        # Accept the CLI arg and create the HTTP semaphore
        self.http_sem = asyncio.Semaphore(http_concurrency)

    async def scrape_all(self, limit: int | None = None) -> list[Speaker]:
        async with Http(self.http_sem) as http:
            idx_html = await http.fetch(INDEX)
            seeds = parse_index(idx_html)
            if limit:
                seeds = seeds[:limit]

            done_urls = {d.get("url") for d in load_jsonl("in/speakers_enriched.jsonl")}
            out: list[Speaker] = []

            async def one(seed: SpeakerSeed):
                if seed.url in done_urls:
                    return
                html = await http.fetch(seed.url)
                sp = await _parse_detail_with_fallbacks(html, seed.url, seed)

                session_links = extract_session_links_from_speaker(html)
                if session_links:
                    session_html = await http.fetch(session_links[0])
                    title = parse_session_title(session_html)
                    if title:
                        sp.talk_titles.append(title)
                await append_jsonl("in/speakers_enriched.jsonl", sp.model_dump()) 
                out.append(sp)
                print(f"[scrape] {len(out)+len(done_urls)} processed", flush=True)

            await asyncio.gather(*(one(s) for s in seeds))
            return out

    async def categorize_all(self, speakers: list[Speaker], llm_concurrency: int = 6) -> list[Speaker]:
        """Assign CompanyCategory via heuristics → LLM; checkpoint results."""
        done = {d.get("url") for d in load_jsonl("in/speakers_categorized.jsonl")} # Skip reprocessing speakers that were categorized in a previous run
        sem = asyncio.Semaphore(llm_concurrency) # Limits how many LLM request run at once; prevents rate-limit spikes
        # local import avoids cyclic imports if any
        from .classify import Categorizer
        categorizer = Categorizer(sem, os.getenv("OPENAI_MODEL")) # Creates a categorizer that takes the semaphore so its LLM calls are throttled
        out: list[Speaker] = []

        async def one(sp: Speaker):
            if sp.url in done:
                out.append(Speaker(**sp.model_dump()))
                return
            sp2, decision_source = await categorizer.categorize(sp) # Perform the categorization
            rec = sp2.model_dump() # Convert the speaker object to a dictionary
            rec["decision_source"] = decision_source
            import time as _t
            rec["categorized_at"] = int(_t.time())
            await append_jsonl("in/speakers_categorized.jsonl", rec)
            out.append(sp2)
            print(f"[categorize] {len(out)+len(done)} processed", flush=True)

        await asyncio.gather(*(one(sp) for sp in speakers))
        return out

    async def generate_emails(
        self,
        speakers: list[Speaker],
        llm_concurrency: int = 6,
        target_set: set[CompanyCategory] | None = None,
    ) -> list[RowOut]:
        target_set = target_set or {CompanyCategory.builder, CompanyCategory.owner}

        # Hard guard: competitors never pass, even if target_set is wrong
        selected = [
            s for s in speakers
            if (s.company_category in target_set)
            and (s.company_category is not CompanyCategory.competitor)
        ]

        sem = asyncio.Semaphore(llm_concurrency)
        emailer = Emailer(sem, os.getenv("OPENAI_MODEL"))
        rows: list[RowOut] = []

        # Skip any speakers we already generated emails for in previous runs
        done_urls = {d.get("url") for d in load_jsonl("in/emails.jsonl")}

        async def one(sp: Speaker):
            if sp.url in done_urls:
                return
            try:
                d = await emailer.draft(sp)
                row = RowOut(
                    speaker_name=sp.name,
                    speaker_title=sp.title,
                    speaker_company=sp.company,
                    company_category=sp.company_category or CompanyCategory.other,
                    email_subject=d.subject,
                    email_body=d.body,
                )
                # Checkpoint immediately to JSONL for perfect resumability
                await append_jsonl("in/emails.jsonl", {
                    "url": sp.url,
                    "speaker_name": sp.name,
                    "speaker_title": sp.title,
                    "speaker_company": sp.company,
                    "company_category": (sp.company_category or CompanyCategory.other).value,
                    "email_subject": d.subject,
                    "email_body": d.body,
                })
                rows.append(row)
                print(f"[email] {len(rows)+len(done_urls)} processed", flush=True)
            except Exception as e:
                print(f"[email][error] {sp.url}: {e}", flush=True)

        await asyncio.gather(*(one(sp) for sp in selected))
        return rows

    async def run(
        self,
        limit: int | None,
        llm_c: int,
        stage: str = "all",
        targets: str | None = None,
        dry_run: bool = False,
    ) -> list[RowOut]:
        target_set = _targets_from_str(targets)
        rows: list[RowOut] = []

        # SCRAPE
        scraped: list[Speaker] = []
        if stage in ("scrape", "all"):
            scraped = await self.scrape_all(limit=limit)

        # CATEGORIZE
        categorized: list[Speaker] = []
        if stage in ("categorize", "email", "all"):
            if not scraped:
                scraped = [Speaker(**d) for d in load_jsonl("in/speakers_enriched.jsonl")]
                if limit:
                    scraped = scraped[:limit]
            categorized = await self.categorize_all(scraped, llm_concurrency=llm_c)

        # EMAIL
        if stage in ("email", "all"):
            rows_new = await self.generate_emails(categorized, llm_concurrency=llm_c, target_set=target_set)
            if dry_run:
                for r in rows_new[:3]:
                    print(f"\n[{r.speaker_name} | {r.speaker_company}] {r.email_subject}\n{r.email_body}\n")
                return rows_new
            # Re-materialize CSV from checkpoint for deterministic output
            data = load_jsonl("in/emails.jsonl")
            seen_urls = set()
            all_rows: list[RowOut] = []
            for d in data:
                u = d.get("url")
                if not u or u in seen_urls:
                    continue
                seen_urls.add(u)
                try:
                    all_rows.append(RowOut(
                        speaker_name=d.get("speaker_name"),
                        speaker_title=d.get("speaker_title"),
                        speaker_company=d.get("speaker_company"),
                        company_category=CompanyCategory(d.get("company_category", "Other")),
                        email_subject=d.get("email_subject", ""),
                        email_body=d.get("email_body", ""),
                    ))
                except Exception:
                    # Skip malformed rows silently; they can be inspected in JSONL
                    continue
            os.makedirs("out", exist_ok=True)
            write_csv("out/email_list.csv", all_rows)
            rows = all_rows

        return rows