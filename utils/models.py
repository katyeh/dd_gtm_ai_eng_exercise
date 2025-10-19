from enum import Enum
from pydantic import BaseModel, Field

class CompanyCategory(str, Enum):
    builder="Builder"; owner="Owner"; partner="Partner"; competitor="Competitor"; other="Other"

class Session(BaseModel):
    title: str | None = None
    description: str | None = None
    url: str | None = None

class Speaker(BaseModel):
    name: str
    title: str | None = None
    company: str | None = None
    bio: str | None = None
    talk_titles: list[str] = Field(default_factory=list)
    session_descriptions: list[str] = Field(default_factory=list)
    sessions: list[Session] = Field(default_factory=list)
    url: str | None = None
    company_category: CompanyCategory | None = None

class Categorization(BaseModel):
    company_category: CompanyCategory
    reason: str

class EmailDraft(BaseModel):
    subject: str
    body: str

class RowOut(BaseModel):
    speaker_name: str
    speaker_title: str | None
    speaker_company: str | None
    company_category: CompanyCategory
    email_subject: str
    email_body: str
