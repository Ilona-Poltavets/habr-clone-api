from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.domain.publications import PublicationStatus, translation_target_locale
from ion_pulse.domain.roles import RoleCode
from ion_pulse.models.identity import User
from ion_pulse.models.publications import (
    Publication,
    PublicationAiReview,
    PublicationLocalization,
    TranslationJob,
)


@dataclass(frozen=True, slots=True)
class AiReviewResult:
    decision: str
    risk_categories: list[str]
    reasons: list[str]
    confidence: float
    age_rating: int | None
    provider: str
    model: str
    rules_version: str


class AiReviewer(Protocol):
    async def review(
        self, *, title: str, summary: str, body: str, locale: str
    ) -> AiReviewResult: ...


class AiReviewerNotConfigured(RuntimeError):
    """Raised when review processing starts before an AI provider is configured."""


class UnconfiguredAiReviewer:
    async def review(self, *, title: str, summary: str, body: str, locale: str) -> AiReviewResult:
        raise AiReviewerNotConfigured("No AI review provider is configured")


async def route_completed_review(
    session: AsyncSession,
    publication: Publication,
    source: PublicationLocalization,
    review: PublicationAiReview,
) -> None:
    """Autopublish only a current, explicitly safe review by a verified author."""
    if review.decision != "pass" or review.source_revision != source.source_revision:
        return
    author = await session.get(User, publication.author_id)
    if (
        author is None
        or RoleCode.AUTHOR.value not in {role.code for role in author.roles}
        or publication.status != PublicationStatus.EDITORIAL_REVIEW.value
    ):
        return
    publication.status = PublicationStatus.PUBLISHED.value
    publication.published_at = datetime.now(UTC)
    target_locale = translation_target_locale(publication.source_locale).value
    already_enqueued = await session.scalar(
        select(TranslationJob.id)
        .where(
            TranslationJob.publication_id == publication.id,
            TranslationJob.target_locale == target_locale,
        )
        .limit(1)
    )
    if already_enqueued is None:
        session.add(TranslationJob(publication_id=publication.id, target_locale=target_locale))


async def process_next_ai_review(session: AsyncSession, reviewer: AiReviewer) -> bool:
    review = await session.scalar(
        select(PublicationAiReview)
        .where(PublicationAiReview.status == "pending")
        .order_by(PublicationAiReview.created_at)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    if review is None:
        return False
    review.status = "reviewing"
    review.attempts += 1
    await session.commit()
    publication = await session.get(Publication, review.publication_id)
    source = None
    if publication is not None:
        source = await session.scalar(
            select(PublicationLocalization).where(
                PublicationLocalization.publication_id == publication.id,
                PublicationLocalization.locale == publication.source_locale,
            )
        )
    if publication is None or source is None:
        review.status = "failed"
        review.last_error = "Publication source no longer exists"
        await session.commit()
        return True
    try:
        result = await reviewer.review(
            title=source.title,
            summary=source.summary,
            body=source.body,
            locale=publication.source_locale,
        )
    except Exception as error:
        review.status = "failed"
        review.last_error = str(error)[:2000]
        await session.commit()
        return True
    review.status = "completed"
    review.decision = result.decision
    review.risk_categories = result.risk_categories
    review.reasons = result.reasons
    review.confidence = result.confidence
    review.age_rating = result.age_rating
    review.provider = result.provider
    review.model = result.model
    review.rules_version = result.rules_version
    review.last_error = None
    review.completed_at = datetime.now(UTC)
    await route_completed_review(session, publication, source, review)
    await session.commit()
    return True


async def requeue_failed_ai_reviews(session: AsyncSession, max_attempts: int = 3) -> None:
    await session.execute(
        update(PublicationAiReview)
        .where(PublicationAiReview.status == "failed", PublicationAiReview.attempts < max_attempts)
        .values(status="pending", last_error=None)
    )
    await session.commit()
