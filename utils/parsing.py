from .models import Speaker
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def _clean(s: str | None) -> str | None:
    if not s: return None
    return re.sub(r"\s+", " ", s).strip() or None

# Base site URL used to construct absolute links and index URL
BASE = "https://www.digitalconstructionweek.com"

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

def parse_index(html: str) -> list[str]:
    """Extract speaker URLs from the index page.
    
    Returns a list of absolute URLs to individual speaker pages.
    """
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    
    for a in soup.select("a[href*='/speakers/']"):
        href = (a.get("href") or "").strip()
        if not href or "/all-speakers/" in href:
            continue
        url = urljoin(BASE + "/", href)
        if url not in seen:
            seen.add(url)
            urls.append(url)
    
    return urls

def parse_speaker_detail(html: str, url: str) -> Speaker:
    """Parse a speaker detail page to extract name, title, company, and bio.
    
    All fields are extracted from the .speaker-details div and .speaker-bio div.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Extract fields from speaker-details div
    name = _extract_speaker_field(soup, "Name") or "Unknown"
    title = _extract_speaker_field(soup, "Job Title")
    company = _extract_speaker_field(soup, "Company")
    
    # Extract bio from speaker-bio div
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