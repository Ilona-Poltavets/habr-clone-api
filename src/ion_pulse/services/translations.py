from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.domain.publications import translation_target_locale
from ion_pulse.models.publications import Publication, PublicationLocalization, TranslationJob


@dataclass(frozen=True, slots=True)
class TranslatedContent:
    title: str
    summary: str
    body: str


class Translator(Protocol):
    async def translate(
        self,
        *,
        title: str,
        summary: str,
        body: str,
        source_locale: str,
        target_locale: str,
    ) -> TranslatedContent: ...


class TranslationProviderNotConfigured(RuntimeError):
    """Raised when a worker is started before an AI provider is configured."""


class UnconfiguredTranslator:
    async def translate(
        self,
        *,
        title: str,
        summary: str,
        body: str,
        source_locale: str,
        target_locale: str,
    ) -> TranslatedContent:
        raise TranslationProviderNotConfigured("No translation provider is configured")


async def process_next_translation_job(session: AsyncSession, translator: Translator) -> bool:
    job = await session.scalar(
        select(TranslationJob)
        .where(TranslationJob.status == "pending")
        .order_by(TranslationJob.created_at)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    if job is None:
        return False

    job.status = "translating"
    job.attempts += 1
    await session.commit()

    publication = await session.get(Publication, job.publication_id)
    if publication is None:
        job.status = "failed"
        job.last_error = "Publication no longer exists"
        await session.commit()
        return True
    source = await session.scalar(
        select(PublicationLocalization).where(
            PublicationLocalization.publication_id == publication.id,
            PublicationLocalization.locale == publication.source_locale,
        )
    )
    if source is None:
        job.status = "failed"
        job.last_error = "Source localization does not exist"
        await session.commit()
        return True

    try:
        translated = await translator.translate(
            title=source.title,
            summary=source.summary,
            body=source.body,
            source_locale=publication.source_locale,
            target_locale=job.target_locale,
        )
    except Exception as error:
        job.status = "failed"
        job.last_error = str(error)[:2000]
        await session.commit()
        return True

    target = await session.scalar(
        select(PublicationLocalization).where(
            PublicationLocalization.publication_id == publication.id,
            PublicationLocalization.locale == job.target_locale,
        )
    )
    if target is None:
        session.add(
            PublicationLocalization(
                publication_id=publication.id,
                locale=job.target_locale,
                title=translated.title,
                summary=translated.summary,
                body=translated.body,
                origin="ai",
                translation_status="ready",
                source_revision=source.source_revision,
            )
        )
    elif target.origin != "human":
        target.title = translated.title
        target.summary = translated.summary
        target.body = translated.body
        target.origin = "ai"
        target.translation_status = "ready"
        target.source_revision = source.source_revision

    job.status = "ready"
    job.last_error = None
    await session.commit()
    return True


def expected_target_locale(source_locale: str) -> str:
    return translation_target_locale(source_locale).value
