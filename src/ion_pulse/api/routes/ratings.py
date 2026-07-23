from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.domain.publications import PublicationStatus
from ion_pulse.models.identity import User
from ion_pulse.models.publications import Publication, PublicationRating
from ion_pulse.schemas.ratings import PublicationRatingRead, PublicationRatingUpdate

router = APIRouter(prefix="/publications")


@router.put("/{publication_id}/rating", response_model=PublicationRatingRead)
async def set_publication_rating(
    publication_id: str,
    payload: PublicationRatingUpdate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> PublicationRatingRead:
    publication = await session.get(Publication, publication_id)
    if publication is None or publication.status != PublicationStatus.PUBLISHED.value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Published publication not found"
        )
    rating = await session.get(
        PublicationRating, {"publication_id": publication.id, "user_id": user.id}
    )
    if rating is None:
        rating = PublicationRating(
            publication_id=publication.id, user_id=user.id, value=payload.value
        )
        session.add(rating)
    else:
        rating.value = payload.value
    await session.commit()
    return PublicationRatingRead(value=rating.value)


@router.delete("/{publication_id}/rating", status_code=status.HTTP_204_NO_CONTENT)
async def remove_publication_rating(
    publication_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> Response:
    rating = await session.get(
        PublicationRating, {"publication_id": publication_id, "user_id": user.id}
    )
    if rating is not None:
        await session.delete(rating)
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
