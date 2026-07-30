from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.api.routes.comments import require_moderation_access
from ion_pulse.core.security import verify_password
from ion_pulse.db.session import get_db_session
from ion_pulse.models.identity import User, UserSanction, UserSanctionAppeal, UserSession
from ion_pulse.models.publications import PublicationComment
from ion_pulse.schemas.comments import ModeratedCommentRead
from ion_pulse.schemas.moderation import (
    SanctionAppealCreate,
    SanctionAppealDecision,
    SanctionAppealRead,
    SuspensionCreate,
    SuspensionLift,
    UserSanctionRead,
)

router = APIRouter(prefix="/moderation")


@router.get("/comments/hidden", response_model=list[ModeratedCommentRead])
async def list_hidden_comments(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    moderator: Annotated[User, Depends(get_current_user)],
) -> list[ModeratedCommentRead]:
    require_moderation_access(moderator)
    comments = (
        await session.scalars(
            select(PublicationComment)
            .where(PublicationComment.is_hidden.is_(True))
            .order_by(PublicationComment.created_at.desc())
            .limit(100)
        )
    ).all()
    return [
        ModeratedCommentRead.model_validate(comment, from_attributes=True) for comment in comments
    ]


@router.post(
    "/sanctions/{sanction_id}/appeals",
    response_model=SanctionAppealRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_sanction_appeal(
    sanction_id: str,
    payload: SanctionAppealCreate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> SanctionAppealRead:
    sanction = await session.get(UserSanction, sanction_id)
    user = None if sanction is None else await session.get(User, sanction.user_id)
    if (
        sanction is None
        or user is None
        or user.email != payload.email.lower()
        or not verify_password(payload.password, user.password_hash)
        or sanction.action != "suspend"
        or user.suspended_until is None
        or user.suspended_until <= datetime.now(UTC)
    ):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sanction not found")
    if await session.scalar(
        select(UserSanctionAppeal).where(UserSanctionAppeal.sanction_id == sanction.id)
    ):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Appeal already exists")
    appeal = UserSanctionAppeal(sanction_id=sanction.id, user_id=user.id, reason=payload.reason)
    session.add(appeal)
    await session.commit()
    await session.refresh(appeal)
    return SanctionAppealRead.model_validate(appeal, from_attributes=True)


@router.post(
    "/sanctions/appeals/current",
    response_model=SanctionAppealRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_current_sanction_appeal(
    payload: SanctionAppealCreate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> SanctionAppealRead:
    """Let a suspended user appeal without exposing an internal sanction identifier."""
    user = await session.scalar(select(User).where(User.email == payload.email.lower()))
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Active sanction not found"
        )
    sanction = await session.scalar(
        select(UserSanction)
        .where(
            UserSanction.user_id == user.id,
            UserSanction.action == "suspend",
            UserSanction.expires_at > datetime.now(UTC),
        )
        .order_by(UserSanction.created_at.desc())
    )
    if sanction is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Active sanction not found"
        )
    if await session.scalar(
        select(UserSanctionAppeal).where(UserSanctionAppeal.sanction_id == sanction.id)
    ):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Appeal already exists")
    appeal = UserSanctionAppeal(sanction_id=sanction.id, user_id=user.id, reason=payload.reason)
    session.add(appeal)
    await session.commit()
    await session.refresh(appeal)
    return SanctionAppealRead.model_validate(appeal, from_attributes=True)


@router.get("/appeals", response_model=list[SanctionAppealRead])
async def list_sanction_appeals(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    moderator: Annotated[User, Depends(get_current_user)],
) -> list[SanctionAppealRead]:
    require_moderation_access(moderator)
    appeals = (
        await session.scalars(
            select(UserSanctionAppeal)
            .where(UserSanctionAppeal.status == "submitted")
            .order_by(UserSanctionAppeal.created_at)
        )
    ).all()
    return [SanctionAppealRead.model_validate(appeal, from_attributes=True) for appeal in appeals]


@router.patch("/appeals/{appeal_id}", response_model=SanctionAppealRead)
async def decide_sanction_appeal(
    appeal_id: str,
    payload: SanctionAppealDecision,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    moderator: Annotated[User, Depends(get_current_user)],
) -> SanctionAppealRead:
    require_moderation_access(moderator)
    appeal = await session.get(UserSanctionAppeal, appeal_id)
    if appeal is None or appeal.status != "submitted":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Open appeal not found")
    appeal.status = payload.status
    appeal.review_note = payload.review_note
    appeal.reviewed_by_user_id = moderator.id
    appeal.reviewed_at = datetime.now(UTC)
    if payload.status == "approved":
        user = await session.get(User, appeal.user_id)
        if user is not None:
            user.suspended_until = None
        session.add(
            UserSanction(
                user_id=appeal.user_id,
                moderator_id=moderator.id,
                action="lift",
                reason="Appeal approved",
            )
        )
    await session.commit()
    await session.refresh(appeal)
    return SanctionAppealRead.model_validate(appeal, from_attributes=True)


@router.post(
    "/users/{user_id}/suspensions",
    response_model=UserSanctionRead,
    status_code=status.HTTP_201_CREATED,
)
async def suspend_user(
    user_id: str,
    payload: SuspensionCreate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    moderator: Annotated[User, Depends(get_current_user)],
) -> UserSanctionRead:
    require_moderation_access(moderator)
    if user_id == str(moderator.id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot suspend yourself"
        )
    if payload.expires_at <= datetime.now(UTC):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Suspension expiry must be in the future",
        )
    user = await session.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    user.suspended_until = payload.expires_at
    sanction = UserSanction(
        user_id=user.id,
        moderator_id=moderator.id,
        action="suspend",
        reason=payload.reason,
        expires_at=payload.expires_at,
    )
    session.add(sanction)
    await session.execute(delete(UserSession).where(UserSession.user_id == user.id))
    await session.commit()
    await session.refresh(sanction)
    return UserSanctionRead.model_validate(sanction, from_attributes=True)


@router.delete("/users/{user_id}/suspensions", status_code=status.HTTP_204_NO_CONTENT)
async def lift_user_suspension(
    user_id: str,
    payload: SuspensionLift,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    moderator: Annotated[User, Depends(get_current_user)],
) -> Response:
    require_moderation_access(moderator)
    user = await session.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if user.suspended_until is not None:
        user.suspended_until = None
        session.add(
            UserSanction(
                user_id=user.id,
                moderator_id=moderator.id,
                action="lift",
                reason=payload.reason,
            )
        )
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
