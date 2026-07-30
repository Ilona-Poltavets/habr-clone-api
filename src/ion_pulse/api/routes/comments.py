from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.domain.publications import PublicationStatus
from ion_pulse.domain.roles import RoleCode
from ion_pulse.models.identity import User
from ion_pulse.models.publications import CommentModerationAction, Publication, PublicationComment
from ion_pulse.schemas.comments import CommentCreate, CommentRead, CommentVisibilityUpdate
from ion_pulse.services.rate_limits import enforce_rate_limit

router = APIRouter(prefix="/publications")


def to_comment(comment: PublicationComment) -> CommentRead:
    return CommentRead.model_validate(comment, from_attributes=True)


def require_moderation_access(user: User) -> None:
    allowed_roles = {RoleCode.MODERATOR.value, RoleCode.ADMINISTRATOR.value}
    if not allowed_roles.intersection(role.code for role in user.roles):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Moderator role required")


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


@router.post(
    "/{publication_id}/comments", response_model=CommentRead, status_code=status.HTTP_201_CREATED
)
async def create_comment(
    publication_id: str,
    payload: CommentCreate,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> CommentRead:
    enforce_rate_limit("comment", str(user.id), limit=20, window_seconds=3600)
    publication = await session.get(Publication, publication_id)
    if publication is None or publication.status != PublicationStatus.PUBLISHED.value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Published publication not found"
        )
    if payload.parent_id is not None:
        parent = await session.get(PublicationComment, payload.parent_id)
        if (
            parent is None
            or parent.publication_id != publication.id
            or parent.parent_id is not None
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid comment parent"
            )
    comment = PublicationComment(
        publication_id=publication.id, author_id=user.id, **payload.model_dump()
    )
    session.add(comment)
    await session.commit()
    await session.refresh(comment)
    return to_comment(comment)


@router.patch("/comments/{comment_id}/visibility", response_model=CommentRead)
async def update_comment_visibility(
    comment_id: str,
    payload: CommentVisibilityUpdate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> CommentRead:
    require_moderation_access(user)
    comment = await session.get(PublicationComment, comment_id)
    if comment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Comment not found")
    if comment.is_hidden != payload.is_hidden:
        session.add(
            CommentModerationAction(
                comment_id=comment.id,
                moderator_id=user.id,
                action="hide" if payload.is_hidden else "restore",
            )
        )
    comment.is_hidden = payload.is_hidden
    await session.commit()
    await session.refresh(comment)
    return to_comment(comment)
