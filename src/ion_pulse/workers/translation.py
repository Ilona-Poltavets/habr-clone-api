import asyncio

from ion_pulse.db.session import async_session_factory, engine
from ion_pulse.services.translations import UnconfiguredTranslator, process_next_translation_job


async def run_once() -> bool:
    async with async_session_factory() as session:
        return await process_next_translation_job(session, UnconfiguredTranslator())


async def main() -> None:
    await run_once()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
