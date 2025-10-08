# utils/emailgen.py
from __future__ import annotations
import os, json, re, asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random
from openai import AsyncOpenAI
from .models import Speaker, EmailDraft, CompanyCategory

SYSTEM = """
Act as a senior Construction GTM copywriter.

Goal
- Draft high-signal, non-generic outbound inviting DCW speakers to booth #42 for a 3–5 min demo.
- Mention a speaker gift exactly once, in the final sentence only.

Audience category: {category}

Personalisation
- Use exactly ONE concrete detail from bio or talk title: {specific_detail}. Tie it to a practical workflow the persona owns.
- No generic praise; open with a benefit tied to day-to-day work.

Persona mapping (pick 1 problem + 1 KPI only)
- Builder.GC: schedule risk, trade coordination, progress variance. KPIs: % plan complete OR days saved OR RFI churn.
- Builder.Specialty (MEP/civils/earthworks): production rates, layout QA, pay-app as-builts. KPIs: installed quantities OR punch closeout time.
- Builder.Engineering/Design-Build: design↔field alignment, model validation, change visibility. KPIs: pre-pour issues caught OR clashes resolved.
- Owner.Developer/Operator: portfolio visibility, milestone certainty, handover quality. KPIs: slippage OR fewer site trips (phrase specifically).

Value props (choose 2–3 max; write plainly)
- Frequent drone + 360 capture for repeatable site evidence
- Progress vs plan quantification; early variance by area/zone
- As-builts for RFIs, pay apps, handover
- QA/QC visuals (pre-pour, MEP rough-in, civil quantities)
- Portfolio rollouts; standardised workflows

Anti-patterns (hard ban): “you’ll appreciate”, “streamline progress tracking”, “Looking forward to connecting”, “reduce site visits” (without a number), “combines drone and 360”, “platform”, “solution”, “transform”, “revolutionise”, exclamation marks.

Tone & constraints
- Subject ≤ 50 chars preferred (≤ 60 hard cap). No colons. Avoid “Boost/Unlock/Discover”.
- Body: 3–4 sentences; 80–120 words; active, concrete.
- Include booth #42 exactly once.
- Mention ONE specific application relevant to the role.
- Only use “Digital Twin” if present in {specific_detail} or persona is Engineering/Design-Build.

Process (do this silently): Generate three variants internally, select the best, then discard the others.

Return ONLY the final choice as JSON:
{ "subject": string, "body": string }
"""


def _pick_specific_detail(sp: Speaker) -> str:
    """Choose one concrete detail from talk titles or bio, kept short.
    Preference: first talk title; otherwise, a concise bio fragment.
    """
    # Prefer a talk title if present
    if sp.talk_titles:
        t = (sp.talk_titles[0] or "").strip()
        return re.sub(r"\s+", " ", t)[:120]

    # Fall back to a short, specific-looking snippet from the bio
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
        specific_detail = _pick_specific_detail(sp)
        category_value = (sp.company_category.value if sp.company_category else "Builder")
        # Avoid Python .format on the whole template to preserve literal JSON braces
        sys = (
            SYSTEM
            .replace("{category}", category_value)
            .replace("{specific_detail}", specific_detail)
        )
        payload = json.dumps({
            "name": sp.name,
            "title": sp.title,
            "company": sp.company,
            "bio": (sp.bio[:600] if sp.bio else None),
            "talk_titles": sp.talk_titles,
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
