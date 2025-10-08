from .models import Speaker
import re
from bs4 import BeautifulSoup
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

def _clean(s: str | None) -> str | None:
    if not s: return None
    return re.sub(r"\s+", " ", s).strip() or None

# Base site URL used to construct absolute links and index URL
BASE = "https://www.digitalconstructionweek.com"

@dataclass
class SpeakerSeed:
    url: str
    name: str | None = None
    title: str | None = None
    company: str | None = None

def _extract_speaker_field(soup, label: str) -> str | None:
    """Extract a field from the speaker-details div by finding the strong tag with the label."""
    details_div = soup.select_one(".speaker-details")
    if not details_div:
        return None
    
    for p in details_div.find_all("p"):
        strong = p.find("strong")
        if strong and label.lower() in strong.get_text(strip=True).lower():
            # Get text after the strong tag
            text = p.get_text(" ", strip=True)
            # Remove the label part
            if ":" in text:
                text = text.split(":", 1)[1].strip()
            return _clean(text)
    return None

def _name_from_slug(href: str) -> str | None:
    try:
        path = urlparse(href).path.rstrip("/")
        slug = path.split("/")[-1]
        if not slug:
            return None
        parts = [p for p in slug.split("-") if p]
        if not parts:
            return None
        return " ".join(w.capitalize() for w in parts)
    except Exception:
        return None

def parse_index(html: str) -> list[SpeakerSeed]:
    """Extract speaker seeds (url, name, title, company) from the index page.

    Heuristics:
    - Find links containing '/speakers/' (exclude '/all-speakers/')
    - Derive name from the URL slug
    - Derive title/company from the link text pattern like 'Name Title at Company'
    """
    soup = BeautifulSoup(html, "html.parser")
    seeds: list[SpeakerSeed] = []
    seen: set[str] = set()
    for a in soup.select("a[href*='/speakers/']"):
        href = (a.get("href") or "").strip()
        if not href or "/all-speakers/" in href:
            continue
        url = urljoin(BASE + "/", href)
        if url in seen:
            continue
        seen.add(url)

        text = (a.get_text(" ", strip=True) or "").strip()
        # Start with name from slug
        name = _name_from_slug(url)
        title = None
        company = None

        # If the text begins with the name, strip it to parse the rest
        rem = text
        if name and rem.lower().startswith(name.lower()):
            rem = rem[len(name):].strip()
        # Common pattern: "Title at Company"
        if " at " in rem:
            left, right = rem.split(" at ", 1)
            title = _clean(left)
            company = _clean(right)

        seeds.append(SpeakerSeed(url=url, name=name, title=title, company=company))
    return seeds

def parse_speaker_detail(html: str, url: str, fallback_name: str | None = None) -> Speaker:
    soup = BeautifulSoup(html, "html.parser")

    # Extract fields from speaker-details div
    name = _extract_speaker_field(soup, "Name")
    title = _extract_speaker_field(soup, "Job Title")
    company = _extract_speaker_field(soup, "Company")
    
    # Use fallback name if needed
    if not name:
        name = fallback_name or "Unknown"
    
    bio = None
    bio_div = soup.select_one(".speaker-bio")
    if bio_div:
        bio = _clean(bio_div.get_text(" ", strip=True))

    return Speaker(name=name, title=title, company=company, bio=bio, talk_titles=[], url=url)

def extract_session_links_from_speaker(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a[href*='/sessions/']"):
        href = a.get("href")
        if not href:
            continue
        links.append(urljoin(BASE, href))
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            out.append(u)
    return out

def parse_session_title(html: str) -> str | None:
    """
    Extract a session title from the session page.
    """
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1, h2, .session-title")
    if h1:
        title = h1.get_text(strip=True)
        return title or None
    return None