from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.domain.roles import RoleCode
from ion_pulse.models.identity import AuthorSubscription, User
from ion_pulse.models.publications import Game, GameSubscription
from ion_pulse.schemas.subscriptions import (
    AuthorSubscriptionRead,
    GameCreate,
    GameRead,
    GameSubscriptionRead,
)

router = APIRouter(prefix="/subscriptions")


def require_game_management_access(user: User) -> None:
    allowed_roles = {RoleCode.CONTENT_MANAGER.value, RoleCode.ADMINISTRATOR.value}
    if not allowed_roles.intersection(role.code for role in user.roles):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Game management role required"
        )


@router.get("/games/catalog", response_model=list[GameRead])
async def list_games(session: Annotated[AsyncSession, Depends(get_db_session)]) -> list[GameRead]:
    games = (await session.scalars(select(Game).order_by(Game.title))).all()
    return [GameRead.model_validate(game, from_attributes=True) for game in games]


@router.post("/games/catalog", response_model=GameRead, status_code=status.HTTP_201_CREATED)
async def create_game(
    payload: GameCreate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> GameRead:
    require_game_management_access(user)
    if await session.scalar(select(Game).where(Game.slug == payload.slug)) is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Game slug already exists")
    game = Game(**payload.model_dump())
    session.add(game)
    await session.commit()
    await session.refresh(game)
    return GameRead.model_validate(game, from_attributes=True)


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


@router.get("/games", response_model=list[GameSubscriptionRead])
async def list_game_subscriptions(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> list[GameSubscriptionRead]:
    rows = await session.execute(
        select(GameSubscription, Game)
        .join(Game, Game.id == GameSubscription.game_id)
        .where(GameSubscription.subscriber_id == user.id)
        .order_by(GameSubscription.created_at.desc())
    )
    return [
        GameSubscriptionRead(
            id=game.id,
            slug=game.slug,
            title=game.title,
            subscribed_at=subscription.created_at,
        )
        for subscription, game in rows
    ]


@router.post("/games/{game_id}", status_code=status.HTTP_204_NO_CONTENT)
async def subscribe_to_game(
    game_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> Response:
    game = await session.get(Game, game_id)
    if game is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Game not found")
    existing = await session.get(GameSubscription, {"subscriber_id": user.id, "game_id": game.id})
    if existing is None:
        session.add(GameSubscription(subscriber_id=user.id, game_id=game.id))
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete("/games/{game_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unsubscribe_from_game(
    game_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> Response:
    subscription = await session.get(
        GameSubscription, {"subscriber_id": user.id, "game_id": game_id}
    )
    if subscription is not None:
        await session.delete(subscription)
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
