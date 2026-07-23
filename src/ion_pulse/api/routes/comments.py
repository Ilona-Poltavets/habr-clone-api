# ruff: noqa: E501
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.domain.publications import PublicationStatus
from ion_pulse.models.identity import User
from ion_pulse.models.publications import Publication, PublicationComment
from ion_pulse.schemas.comments import CommentCreate, CommentRead

router = APIRouter(prefix="/publications")


def to_comment(comment: PublicationComment) -> CommentRead:
    return CommentRead.model_validate(comment, from_attributes=True)


@router.get("/{publication_id}/comments", response_model=list[CommentRead])
async def list_comments(
    publication_id: str, session: Annotated[AsyncSession, Depends(get_db_session)]
) -> list[CommentRead]:
    comments = (
        await session.scalars(
            select(PublicationComment)
            .where(
                PublicationComment.publication_id == publication_id,
                PublicationComment.is_hidden.is_(False),
            )
            .order_by(PublicationComment.created_at)
        )
    ).all()
    return [to_comment(comment) for comment in comments]


@router.post("/{publication_id}/comments", response_model=CommentRead, status_code=status.HTTP_201_CREATED)
async def create_comment(
    publication_id: str,
    payload: CommentCreate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> CommentRead:
    publication = await session.get(Publication, publication_id)
    if publication is None or publication.status != PublicationStatus.PUBLISHED.value:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Published publication not found")
    if payload.parent_id is not None:
        parent = await session.get(PublicationComment, payload.parent_id)
        if parent is None or parent.publication_id != publication.id or parent.parent_id is not None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid comment parent")
    comment = PublicationComment(publication_id=publication.id, author_id=user.id, **payload.model_dump())
    session.add(comment)
    await session.commit()
    await session.refresh(comment)
    return to_comment(comment)
