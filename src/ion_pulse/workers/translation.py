import asyncio

from ion_pulse.db.session import async_session_factory, engine
from ion_pulse.services.ai_reviews import process_next_ai_review, requeue_failed_ai_reviews
from ion_pulse.services.openai_reviewer import configured_ai_reviewer
from ion_pulse.services.openai_translator import configured_translator
from ion_pulse.services.scheduling import publish_due_publications
from ion_pulse.services.translations import (
    process_next_translation_job,
    requeue_failed_translation_jobs,
)


async def run_once() -> bool:
    async with async_session_factory() as session:
        await publish_due_publications(session)
        await requeue_failed_ai_reviews(session)
        await requeue_failed_translation_jobs(session)
        review_processed = await process_next_ai_review(session, configured_ai_reviewer())
        translation_processed = await process_next_translation_job(session, configured_translator())
        return review_processed or translation_processed


async def main() -> None:
    await run_once()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
