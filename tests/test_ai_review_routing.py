import asyncio
from types import SimpleNamespace
from uuid import uuid4

from ion_pulse.services.ai_reviews import route_completed_review
from ion_pulse.services.openai_reviewer import parse_review_result
from ion_pulse.services.openai_translator import parse_translation


class Session:
    def __init__(self, author: object, existing_job: object | None = None) -> None:
        self.author = author
        self.existing_job = existing_job
        self.added: list[object] = []

    async def get(self, _model: object, _identifier: object) -> object:
        return self.author

    async def scalar(self, _statement: object) -> object | None:
        return self.existing_job

    def add(self, item: object) -> None:
        self.added.append(item)


def test_safe_verified_author_review_publishes_once_and_enqueues_translation() -> None:
    author = SimpleNamespace(roles=[SimpleNamespace(code="author")])
    session = Session(author)
    publication = SimpleNamespace(
        id=uuid4(),
        author_id=uuid4(),
        source_locale="ru",
        status="editorial_review",
        published_at=None,
    )
    source = SimpleNamespace(source_revision=3)
    review = SimpleNamespace(decision="pass", source_revision=3)

    asyncio.run(route_completed_review(session, publication, source, review))

    assert publication.status == "published"
    assert publication.published_at is not None
    assert len(session.added) == 1


def test_safe_non_author_review_stays_in_editorial_queue() -> None:
    session = Session(SimpleNamespace(roles=[]))
    publication = SimpleNamespace(
        id=uuid4(),
        author_id=uuid4(),
        source_locale="ru",
        status="editorial_review",
        published_at=None,
    )

    asyncio.run(
        route_completed_review(
            session,
            publication,
            SimpleNamespace(source_revision=1),
            SimpleNamespace(decision="pass", source_revision=1),
        )
    )

    assert publication.status == "editorial_review"
    assert not session.added


def test_openai_review_result_requires_known_structured_values() -> None:
    result = parse_review_result(
        '{"decision":"needs_editor","risk_categories":["violence"],'
        '"reasons":["Нужна ручная оценка контекста"],"confidence":0.8,"age_rating":16}',
        "test-model",
        "test-rules",
    )

    assert result.decision == "needs_editor"
    assert result.provider == "openai_compatible"


def test_openai_translation_result_requires_all_content_fields() -> None:
    translated = parse_translation(
        '{"title":"English title","summary":"English summary","body":"English body"}'
    )

    assert translated.title == "English title"
