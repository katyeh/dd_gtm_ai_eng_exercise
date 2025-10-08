from __future__ import annotations
import os, json, asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random
from openai import AsyncOpenAI
from .models import Speaker, CompanyCategory, Categorization

# Cheap heuristics first
_COMPETITOR = ("pix4d","propeller","openspace","matterport","trimble","bentley","navvis","skydio","dji","reconstruct","sitevision","contextcapture","synchro")
_BUILDER    = ("contractor","construction","builders","design-build","paving","mep","civil","earthworks","gc ")
_OWNER      = ("airport","rail","transit","authority","council","utility","energy","developer","reit","university","hospital","nhs","network rail","national highways")
_PARTNER    = ("systems","consulting","integrator","reseller","software","platform","oem","marketplace","implementation")

def _hay(sp: Speaker) -> str:
    return " ".join(filter(None, [sp.company or "", sp.title or "", sp.bio or ""])).lower()

def heuristic_category(sp: Speaker) -> CompanyCategory | None:
    h = _hay(sp)
    if any(k in h for k in _COMPETITOR): return CompanyCategory.competitor
    if any(k in h for k in _BUILDER):    return CompanyCategory.builder
    if any(k in h for k in _OWNER):      return CompanyCategory.owner
    if any(k in h for k in _PARTNER):    return CompanyCategory.partner
    return None

_SYSTEM = """You categorize AEC companies for sales targeting.

Definitions:
- Builder: contractors and engineering/design-build firms executing construction.
- Owner: organizations that commission/own/operate assets (developers, agencies, utilities, operators).
- Partner: complementary platforms/SIs/resellers/OEMs or software vendors that integrate with DroneDeploy.
- Competitor: vendors offering reality capture, drone mapping, 360 site documentation, or construction progress platforms that could substitute DroneDeploy.
- Other: associations, media, academia, NGOs.

Tie-breaker: if a company plausibly substitutes DroneDeploy, classify as Competitor.
Return ONLY JSON exactly matching the provided schema.
"""

class Categorizer:
    def __init__(self, sem: asyncio.Semaphore, model: str | None = None):
        self.sem = sem
        self.client = AsyncOpenAI()
        self.model = model or os.getenv("OPENAI_MODEL","gpt-4.1-mini")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(0.5,10)+wait_random(0,0.5))
    async def _llm(self, sp: Speaker) -> Categorization:
        payload = json.dumps({
            "name": sp.name, "title": sp.title, "company": sp.company,
            "bio": sp.bio, "talk_titles": sp.talk_titles
        }, ensure_ascii=False)
        async with self.sem:
            r = await self.client.responses.parse(
                model=self.model,
                input=[{"role":"system","content":_SYSTEM},
                       {"role":"user","content":payload}],
                text_format=Categorization,
            )
        # Prefer parsed output when available; fallback to validation if needed
        if isinstance(r.output_parsed, Categorization):
            return r.output_parsed
        return Categorization.model_validate(r.output_parsed)

    async def categorize(self, sp: Speaker) -> tuple[Speaker, str]:
        # 1) heuristics
        h = heuristic_category(sp)
        if h:
            sp.company_category = h
            return sp, "heuristic"
        # 2) LLM fallback
        sp.company_category = (await self._llm(sp)).company_category
        return sp, "llm"
