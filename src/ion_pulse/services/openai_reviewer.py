import json
from typing import Any

import httpx

from ion_pulse.core.config import get_settings
from ion_pulse.services.ai_reviews import AiReviewer, AiReviewResult, UnconfiguredAiReviewer

SYSTEM_PROMPT = (
    "You are the Ion Pulse editorial safety reviewer. Return JSON with decision "
    "(pass, needs_editor, or block_until_review), risk_categories (string array), "
    "reasons (short Russian explanation array), confidence (0 through 1), and "
    "age_rating (null, 0, 6, 12, 16, or 18). Use needs_editor when uncertain."
)


class OpenAiCompatibleReviewer:
    def __init__(self, *, api_base_url: str, api_key: str, model: str, rules_version: str) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.rules_version = rules_version

    async def review(
        self, *, title: str, summary: str, body: str, locale: str
    ) -> AiReviewResult:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{self.api_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": (
                                f"Locale: {locale}\nTitle: {title}\nSummary: {summary}\n\n"
                                f"Body:\n{body}"
                            ),
                        },
                    ],
                },
            )
            response.raise_for_status()
        payload = response.json()
        try:
            content = payload["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as error:
            raise ValueError("AI provider returned no completion content") from error
        return parse_review_result(content, self.model, self.rules_version)


def parse_review_result(content: str, model: str, rules_version: str) -> AiReviewResult:
    try:
        payload: Any = json.loads(content)
        decision = payload["decision"]
        risk_categories = payload["risk_categories"]
        reasons = payload["reasons"]
        confidence = float(payload["confidence"])
        age_rating = payload.get("age_rating")
    except (TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
        raise ValueError("AI provider returned an invalid review JSON") from error
    if decision not in {"pass", "needs_editor", "block_until_review"}:
        raise ValueError("AI provider returned an unknown review decision")
    if not isinstance(risk_categories, list) or not all(
        isinstance(item, str) for item in risk_categories
    ):
        raise ValueError("AI provider returned invalid risk categories")
    if not isinstance(reasons, list) or not all(isinstance(item, str) for item in reasons):
        raise ValueError("AI provider returned invalid review reasons")
    if not 0 <= confidence <= 1:
        raise ValueError("AI provider returned invalid confidence")
    if age_rating not in {None, 0, 6, 12, 16, 18}:
        raise ValueError("AI provider returned invalid age rating")
    return AiReviewResult(
        decision=decision,
        risk_categories=risk_categories,
        reasons=reasons,
        confidence=confidence,
        age_rating=age_rating,
        provider="openai_compatible",
        model=model,
        rules_version=rules_version,
    )


def configured_ai_reviewer() -> AiReviewer:
    settings = get_settings()
    if settings.ai_review_provider == "openai_compatible" and settings.ai_review_api_key:
        return OpenAiCompatibleReviewer(
            api_base_url=settings.ai_review_api_base_url,
            api_key=settings.ai_review_api_key,
            model=settings.ai_review_model,
            rules_version=settings.ai_review_rules_version,
        )
    return UnconfiguredAiReviewer()
