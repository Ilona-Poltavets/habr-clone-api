from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.domain.publications import PublicationStatus, translation_target_locale
from ion_pulse.models.publications import Publication, TranslationJob


async def publish_due_publications(session: AsyncSession) -> int:
    """Publish scheduled materials and enqueue their translation exactly once."""
    now = datetime.now(UTC)
    publications = (
        await session.scalars(
            select(Publication)
            .where(
                Publication.status == PublicationStatus.SCHEDULED.value,
                Publication.scheduled_at <= now,
            )
            .with_for_update(skip_locked=True)
        )
    ).all()
    for publication in publications:
        publication.status = PublicationStatus.PUBLISHED.value
        publication.published_at = now
        publication.scheduled_at = None
        session.add(
            TranslationJob(
                publication_id=publication.id,
                target_locale=translation_target_locale(publication.source_locale).value,
            )
        )
    if publications:
        await session.commit()
    return len(publications)
