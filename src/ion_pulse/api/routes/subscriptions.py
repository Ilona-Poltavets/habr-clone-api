from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.models.identity import AuthorSubscription, User
from ion_pulse.schemas.subscriptions import AuthorSubscriptionRead

router = APIRouter(prefix="/subscriptions")


@router.get("/authors", response_model=list[AuthorSubscriptionRead])
async def list_author_subscriptions(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> list[AuthorSubscriptionRead]:
    rows = await session.execute(
        select(AuthorSubscription, User)
        .join(User, User.id == AuthorSubscription.author_id)
        .where(AuthorSubscription.subscriber_id == user.id)
        .order_by(AuthorSubscription.created_at.desc())
    )
    return [
        AuthorSubscriptionRead(
            author_id=subscription.author_id,
            display_name=author.display_name,
            subscribed_at=subscription.created_at,
        )
        for subscription, author in rows
    ]


@router.post("/authors/{author_id}", status_code=status.HTTP_204_NO_CONTENT)
async def subscribe_to_author(
    author_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> Response:
    if author_id == str(user.id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot subscribe to yourself",
        )
    author = await session.get(User, author_id)
    if author is None or not author.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Author not found")
    existing = await session.get(
        AuthorSubscription,
        {"subscriber_id": user.id, "author_id": author_id},
    )
    if existing is None:
        session.add(AuthorSubscription(subscriber_id=user.id, author_id=author_id))
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete("/authors/{author_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unsubscribe_from_author(
    author_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> Response:
    subscription = await session.get(
        AuthorSubscription,
        {"subscriber_id": user.id, "author_id": author_id},
    )
    if subscription is not None:
        await session.delete(subscription)
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
