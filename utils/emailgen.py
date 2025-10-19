from __future__ import annotations
import os, json, re, asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random
from openai import AsyncOpenAI
from .models import Speaker, EmailDraft, CompanyCategory

SYSTEM = """
You are a senior construction GTM copywriter. Write concise, high-signal outbound for DCW speakers.

CONTEXT
- Booth: #42
- Demo length: 3–5 minutes
- Audience category: {category}            # Builder.GC | Builder.Specialty | Builder.Engineering/Design-Build | Owner
- Context provided in user JSON: name/title/company, full bio, sessions (title + description)

OBJECTIVE
Draft a single outbound email that invites the speaker to booth #42 for a short demo. Mention a speaker gift exactly once, in the final sentence.

PERSONA LOGIC (pick exactly 1 problem + 1 KPI)
- Builder.GC → problems: schedule risk | trade coordination | progress variance
  KPIs: % plan complete | days saved | RFI churn
- Builder.Specialty (MEP/civils/earthworks) → problems: production rates | layout QA | pay-app as-builts
  KPIs: installed quantities | punch closeout time
- Builder.Engineering/Design-Build → problems: design↔field alignment | model validation | change visibility
  KPIs: pre-pour issues caught | clashes resolved
- Owner (Developer/Operator) → problems: portfolio visibility | milestone certainty | handover quality
  KPIs: slippage | fewer site trips (phrase with a number, e.g., “from 4 to 2 per month”)

VALUE PROPS (choose 2–3 that fit; write plainly)
- Frequent drone + 360 capture for repeatable site evidence
- Quantify progress vs plan; surface variance by area/zone early
- As-builts for RFIs, pay apps, handover
- QA/QC visuals (pre-pour, MEP rough-in, civil quantities)
- Portfolio rollouts; standardised workflows

STYLE GUARDRAILS
- Subject: ≤ 50 chars preferred (≤ 60 hard cap). No colons. Avoid “Boost/Unlock/Discover”.
- Body: 3–4 sentences; 80–120 words; active, concrete; avoid corporate nouns.
- Mention booth #42 once.
- Mention the speaker gift exactly once, in the last sentence only.
- Use ONE hook: pick a specific detail from the provided context (sessions/bio), mirror its vocabulary, and map it to a specific workflow + single KPI.
- Only use “Digital Twin” if {category} is Engineering/Design-Build OR the chosen hook contains it.

HARD BANS (must not appear, case-insensitive)
- “you’ll appreciate”, “streamline progress tracking”, “Looking forward to connecting”
- “reduce site visits” (without a number), “combines drone and 360”
- “platform”, “solution”, “transform”, “revolutionise”
- Exclamation marks

VARIATION (do silently)
- Choose an opening that is role/KPI-led (not a compliment).
- Vary verbs; avoid filler (“workflow”, “leverage”) unless necessary.

SELF-CHECK (do silently before returning)
- Subject length ≤ 60.
- Body is 3–4 sentences and 80–120 words.
- Booth mentioned once.
- Gift mentioned once and only in the final sentence.
- Exactly one problem + one KPI are implied.
- No hard-ban phrases present.
- If any check fails, fix and re-check.

OUTPUT (JSON only, no prose)
Return exactly this JSON object and nothing else, conforming to the schema:

{
  "subject": string,
  "body": string
}

SCHEMA (for structured output)
type: object
additionalProperties: false
properties:
  subject:
    type: string
  body:
    type: string
required: ["subject", "body"]
"""


def _pick_specific_detail(sp: Speaker) -> str:
    """Choose one concrete detail from sessions, session descriptions, talk titles, or bio.
    Preference: first session description (most detailed), then first session title,
    then legacy lists (session_descriptions, talk_titles), then bio fragment.
    """
    # Prefer details from the normalized sessions list
    if getattr(sp, "sessions", None):
        s0 = sp.sessions[0]
        if getattr(s0, "description", None):
            desc = (s0.description or "").strip()
            desc = re.sub(r"\s+", " ", desc)
            if "." in desc[:200]:
                first_sentence = desc.split(".")[0] + "."
                return first_sentence[:200]
            return desc[:200]
        if getattr(s0, "title", None):
            t = (s0.title or "").strip()
            return re.sub(r"\s+", " ", t)[:120]

    # Back-compat: use legacy arrays if present
    if sp.session_descriptions:
        desc = (sp.session_descriptions[0] or "").strip()
        desc = re.sub(r"\s+", " ", desc)
        if "." in desc[:200]:
            first_sentence = desc.split(".")[0] + "."
            return first_sentence[:200]
        return desc[:200]
    if sp.talk_titles:
        t = (sp.talk_titles[0] or "").strip()
        return re.sub(r"\s+", " ", t)[:120]

    # Finally, fall back to a short, specific-looking snippet from the bio
    if sp.bio:
        bio = re.sub(r"\s+", " ", sp.bio.strip())
        # Try to capture a clause mentioning a project/work/package
        m = re.search(r"([^.]*\b(project|programme|package|data centre)\b[^.]*)\.\s*", bio, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:120]
        # Otherwise, use the first ~12 words as a compact detail
        words = bio.split()
        return " ".join(words[:12])

    # If nothing available, return empty and let the model rely on role/company
    return ""

class Emailer:
    def __init__(self, sem: asyncio.Semaphore, model: str | None = None):
        self.client = AsyncOpenAI()
        self.sem = sem
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(0.5, 10) + wait_random(0, 0.5))
    async def draft(self, sp: Speaker) -> EmailDraft:
        """Generate a subject + body for a single speaker"""
        schema = EmailDraft.model_json_schema()
        category_value = (sp.company_category.value if sp.company_category else "Builder")

        sys = (
            SYSTEM
            .replace("{category}", category_value)
        )
        payload = json.dumps({
            "name": sp.name,
            "title": sp.title,
            "company": sp.company,
            "company_category": category_value,
            "bio": (sp.bio if sp.bio else None),
            "sessions": [
                {"title": s.title, "description": s.description}
                for s in (sp.sessions or [])
            ],
            # Back-compat fields (not used for hook selection)
            "talk_titles": sp.talk_titles,
            "session_descriptions": getattr(sp, "session_descriptions", []),
        }, ensure_ascii=False)

        async with self.sem:
            r = await self.client.responses.parse(
                model=self.model,
                input=[{"role": "system", "content": sys},
                       {"role": "user", "content": payload}],
                text_format=EmailDraft,
            )
        if isinstance(r.output_parsed, EmailDraft):
            return r.output_parsed
        return EmailDraft.model_validate(r.output_parsed)
