import pytest
from pydantic import ValidationError

from ion_pulse.schemas.publications import (
    DraftCreate,
    DraftUpdate,
    JournalIssueCreate,
    PublishedPublicationRead,
)


def draft_payload() -> dict[str, object]:
    return {
        "category_slug": "reviews",
        "source_locale": "ru",
        "title": "Достаточно длинный заголовок",
        "summary": "Достаточно длинный анонс нового игрового материала.",
        "body": "Достаточно длинный текст материала для проверки схемы публикации." * 2,
    }


def test_draft_defaults_to_article_without_review_metadata() -> None:
    draft = DraftCreate.model_validate(draft_payload())

    assert draft.content_type == "article"
    assert draft.game_id is None
    assert draft.review_score is None


def test_draft_accepts_review_metadata() -> None:
    payload = draft_payload() | {
        "content_type": "review",
        "game_id": "c2370b06-d76f-4a1d-bb14-ca64e83cd0d8",
        "review_score": 8.5,
    }

    draft = DraftCreate.model_validate(payload)

    assert draft.content_type == "review"
    assert draft.review_score == 8.5


def test_draft_accepts_digest_type() -> None:
    draft = DraftCreate.model_validate(draft_payload() | {"content_type": "digest"})

    assert draft.content_type == "digest"


@pytest.mark.parametrize(
    "payload",
    [
        draft_payload() | {"content_type": "video"},
        draft_payload() | {"review_score": 10.1},
    ],
)
def test_draft_rejects_unknown_type_and_invalid_review_score(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        DraftCreate.model_validate(payload)


def test_draft_update_requires_content_type() -> None:
    with pytest.raises(ValidationError, match="content_type"):
        DraftUpdate.model_validate(draft_payload())


def test_journal_issue_rejects_reversed_period() -> None:
    with pytest.raises(ValidationError, match="Journal period end"):
        JournalIssueCreate.model_validate(
            {
                "title": "Выпуск недели",
                "period_start": "2026-07-27T00:00:00Z",
                "period_end": "2026-07-20T00:00:00Z",
            }
        )


def test_published_material_exposes_author_identity() -> None:
    publication = PublishedPublicationRead.model_validate(
        {
            "id": "c2370b06-d76f-4a1d-bb14-ca64e83cd0d8",
            "author_id": "4f99d326-1d10-4e08-9c8a-3cde4721e940",
            "author_name": "Ion Author",
            "category_slug": "reviews",
            "content_type": "article",
            "game_id": None,
            "review_score": None,
            "source_locale": "ru",
            "locale": "ru",
            "translation_available": True,
            "title": "Достаточно длинный заголовок",
            "summary": "Достаточно длинный анонс нового игрового материала.",
            "body": "Достаточно длинный текст материала для проверки схемы публикации.",
            "published_at": "2026-07-29T12:00:00Z",
        }
    )

    assert publication.author_name == "Ion Author"
