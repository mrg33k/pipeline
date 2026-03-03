from dataclasses import dataclass, field
from typing import List


@dataclass
class Profile:
    first_name: str
    last_name: str
    email: str
    title: str = ""
    company_name: str = ""
    company_domain: str = ""
    company_industry: str = ""
    company_city: str = ""
    company_state: str = ""
    company_description: str = ""
    linkedin_url: str = ""
    homepage_snippet: str = ""
    linkedin_snippet: str = ""
    review_signals: List[str] = field(default_factory=list)
    source_draft_id: str = ""


@dataclass
class ResearchCard:
    industry_bucket: str
    source_truth: List[str]
    impact_core: str
    impact_subject: str
    proof_phrase: str
    meaning_line: str
    confidence: float
    quality: str
    opener_fact: str = ""
    opener_source_hint: str = ""


@dataclass
class WriteResult:
    subject: str
    body: str
    status: str
    skip_reason: str = ""
