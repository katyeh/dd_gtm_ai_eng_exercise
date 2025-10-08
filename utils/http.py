import aiohttp, asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random

HEADERS = {
    "User-Agent": "dd-gtm-ai-eng-exercise/1.0 (+scraper)",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    # Force no compression to avoid zstd decode errors
    "Accept-Encoding": "identity",
}

class Http:
    def __init__(self, sem: asyncio.Semaphore, timeout: int = 20):
        self.sem = sem
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        # (Keep defaults; no need to change autodecompress when we force identity.)
        self.session = aiohttp.ClientSession(timeout=self.timeout, headers=HEADERS)
        return self

    async def __aexit__(self, *_):
        if self.session:
            await self.session.close()

    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(multiplier=0.5, max=10) + wait_random(0, 0.5))
    async def fetch(self, url: str) -> str:
        async with self.sem:
            assert self.session is not None
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.text()
