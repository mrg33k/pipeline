from dataclasses import dataclass


@dataclass
class RunSettings:
    """Per-run settings resolved from config, CLI flags, and optional UI overrides."""

    max_emails: int
    pages: int
    dry_run: bool
    skip_drafts: bool
    openai_model: str
    email_system_prompt: str
    filter_extra_directions: str = ""
    rewrite_count: int = 10
    rewrite_confirmed: bool = False

    def normalized(self) -> "RunSettings":
        """Return a copy with trimmed string fields."""
        return RunSettings(
            max_emails=self.max_emails,
            pages=self.pages,
            dry_run=self.dry_run,
            skip_drafts=self.skip_drafts,
            openai_model=(self.openai_model or "").strip(),
            email_system_prompt=(self.email_system_prompt or "").strip(),
            filter_extra_directions=(self.filter_extra_directions or "").strip(),
            rewrite_count=self.rewrite_count,
            rewrite_confirmed=self.rewrite_confirmed,
        )

    def validate(self) -> None:
        """Raise ValueError when settings are outside supported ranges."""
        if not isinstance(self.max_emails, int) or not (1 <= self.max_emails <= 200):
            raise ValueError("max_emails must be an integer between 1 and 200")

        if not isinstance(self.pages, int) or not (1 <= self.pages <= 10):
            raise ValueError("pages must be an integer between 1 and 10")

        if not (self.openai_model or "").strip():
            raise ValueError("openai_model cannot be empty")

        if not (self.email_system_prompt or "").strip():
            raise ValueError("email_system_prompt cannot be empty")

        if not isinstance(self.rewrite_count, int) or not (1 <= self.rewrite_count <= 200):
            raise ValueError("rewrite_count must be an integer between 1 and 200")
