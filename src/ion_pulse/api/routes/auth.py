import logging
from datetime import UTC, datetime, timedelta
from secrets import token_urlsafe
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ion_pulse.core.config import get_settings
from ion_pulse.core.security import (
    create_session_token,
    hash_password,
    hash_password_reset_token,
    hash_session_token,
    verify_password,
)
from ion_pulse.db.session import get_db_session
from ion_pulse.models.identity import AccountDeletionAudit, PasswordResetToken, User, UserSession
from ion_pulse.models.publications import Publication, PublicationLocalization
from ion_pulse.schemas.auth import (
    AccountDeletionRequest,
    AuthenticatedUser,
    LoginRequest,
    PasswordResetConfirm,
    PasswordResetRequest,
    ProfileUpdateRequest,
    RegisterRequest,
)
from ion_pulse.services.password_reset import deliver_password_reset
from ion_pulse.services.rate_limits import enforce_rate_limit

router = APIRouter(prefix="/auth")
logger = logging.getLogger(__name__)


def client_address(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def to_authenticated_user(user: User) -> AuthenticatedUser:
    return AuthenticatedUser(
        id=user.id,
        email=user.email,
        display_name=user.display_name,
        roles=[role.code for role in user.roles],
    )


def set_session_cookie(response: Response, token: str) -> None:
    settings = get_settings()
    response.set_cookie(
        key=settings.session_cookie_name,
        value=token,
        httponly=True,
        secure=settings.session_cookie_secure,
        samesite="lax",
        max_age=settings.session_lifetime_hours * 60 * 60,
        path="/",
    )


async def create_session(user: User, session: AsyncSession) -> str:
    settings = get_settings()
    token = create_session_token()
    session.add(
        UserSession(
            user_id=user.id,
            token_hash=hash_session_token(token),
            expires_at=datetime.now(UTC) + timedelta(hours=settings.session_lifetime_hours),
        )
    )
    await session.commit()
    return token


@router.post("/register", response_model=AuthenticatedUser, status_code=status.HTTP_201_CREATED)
async def register(
    payload: RegisterRequest,
    response: Response,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AuthenticatedUser:
    enforce_rate_limit("register", client_address(request), limit=5, window_seconds=3600)
    email = payload.email.lower()
    existing_user = await session.scalar(select(User).where(User.email == email))
    if existing_user is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")

    user = User(
        email=email,
        display_name=payload.display_name,
        password_hash=hash_password(payload.password),
    )
    session.add(user)
    await session.flush()
    token = await create_session(user, session)
    set_session_cookie(response, token)
    return to_authenticated_user(user)


@router.post("/login", response_model=AuthenticatedUser)
async def login(
    payload: LoginRequest,
    response: Response,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AuthenticatedUser:
    enforce_rate_limit("login", client_address(request), limit=10, window_seconds=900)
    user = await session.scalar(select(User).where(User.email == payload.email.lower()))
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    if (
        not user.is_active
        or (user.suspended_until is not None and user.suspended_until > datetime.now(UTC))
        or not verify_password(payload.password, user.password_hash)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    token = await create_session(user, session)
    set_session_cookie(response, token)
    return to_authenticated_user(user)


@router.post("/password-reset-requests", status_code=status.HTTP_204_NO_CONTENT)
async def request_password_reset(
    payload: PasswordResetRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    """Create a short-lived reset token without disclosing whether the email exists."""
    enforce_rate_limit("password-reset", client_address(request), limit=5, window_seconds=3600)
    user = await session.scalar(select(User).where(User.email == payload.email.lower()))
    if user is not None and user.is_active:
        now = datetime.now(UTC)
        await session.execute(
            delete(PasswordResetToken).where(
                PasswordResetToken.user_id == user.id,
                PasswordResetToken.used_at.is_(None),
            )
        )
        token = token_urlsafe(32)
        session.add(
            PasswordResetToken(
                user_id=user.id,
                token_hash=hash_password_reset_token(token),
                expires_at=now + timedelta(minutes=get_settings().password_reset_lifetime_minutes),
            )
        )
        await session.commit()
        try:
            await deliver_password_reset(user.email, token)
        except Exception:  # Delivery failures must not disclose registered emails.
            logger.exception("Could not deliver password recovery message")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/password-resets", status_code=status.HTTP_204_NO_CONTENT)
async def reset_password(
    payload: PasswordResetConfirm,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    now = datetime.now(UTC)
    reset = await session.scalar(
        select(PasswordResetToken)
        .where(
            PasswordResetToken.token_hash == hash_password_reset_token(payload.token),
            PasswordResetToken.used_at.is_(None),
            PasswordResetToken.expires_at > now,
        )
        .with_for_update()
    )
    if reset is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired reset link"
        )
    user = await session.get(User, reset.user_id, with_for_update=True)
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired reset link"
        )
    user.password_hash = hash_password(payload.password)
    reset.used_at = now
    await session.execute(delete(UserSession).where(UserSession.user_id == user.id))
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def get_current_user(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> User:
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie_name)
    if token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    user_session = await session.scalar(
        select(UserSession)
        .options(selectinload(UserSession.user).selectinload(User.roles))
        .where(
            UserSession.token_hash == hash_session_token(token),
            UserSession.expires_at > datetime.now(UTC),
        )
    )
    if (
        user_session is None
        or not user_session.user.is_active
        or (
            user_session.user.suspended_until is not None
            and user_session.user.suspended_until > datetime.now(UTC)
        )
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return user_session.user


@router.get("/me", response_model=AuthenticatedUser)
async def get_me(user: Annotated[User, Depends(get_current_user)]) -> AuthenticatedUser:
    return to_authenticated_user(user)


@router.patch("/me", response_model=AuthenticatedUser)
async def update_me(
    payload: ProfileUpdateRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> AuthenticatedUser:
    existing_user = await session.scalar(
        select(User).where(User.display_name == payload.display_name, User.id != user.id)
    )
    if existing_user is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Display name already taken",
        )
    user.display_name = payload.display_name
    await session.commit()
    return to_authenticated_user(user)


@router.get("/me/export")
async def export_my_data(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> JSONResponse:
    publications = (
        await session.scalars(select(Publication).where(Publication.author_id == user.id))
    ).all()
    publication_ids = [publication.id for publication in publications]
    localizations: list[PublicationLocalization] = []
    if publication_ids:
        localizations = list(
            (
                await session.scalars(
                    select(PublicationLocalization).where(
                        PublicationLocalization.publication_id.in_(publication_ids)
                    )
                )
            ).all()
        )
    return JSONResponse(
        {
            "profile": {
                "id": str(user.id),
                "email": user.email,
                "display_name": user.display_name,
                "roles": [role.code for role in user.roles],
                "created_at": user.created_at.isoformat() if user.created_at else None,
            },
            "publications": [
                {
                    "id": str(publication.id),
                    "source_locale": publication.source_locale,
                    "status": publication.status,
                    "created_at": publication.created_at.isoformat()
                    if publication.created_at
                    else None,
                }
                for publication in publications
            ],
            "localizations": [
                {
                    "publication_id": str(localization.publication_id),
                    "locale": localization.locale,
                    "title": localization.title,
                    "summary": localization.summary,
                    "body": localization.body,
                    "origin": localization.origin,
                }
                for localization in localizations
            ],
        },
        headers={"Content-Disposition": 'attachment; filename="ion-pulse-data.json"'},
    )


@router.delete("/me", status_code=status.HTTP_204_NO_CONTENT)
async def delete_my_account(
    payload: AccountDeletionRequest,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> Response:
    if not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid password")
    user.is_active = False
    user.email = f"deleted-{user.id}@deleted.invalid"
    user.display_name = f"deleted_{user.id.hex[:12]}"
    user.password_hash = hash_password(token_urlsafe(32))
    session.add(AccountDeletionAudit(user_id=user.id, reason=payload.reason))
    await session.execute(delete(UserSession).where(UserSession.user_id == user.id))
    await session.commit()
    response.delete_cookie(get_settings().session_cookie_name, path="/")
    return response


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie_name)
    if token is not None:
        await session.execute(
            delete(UserSession).where(UserSession.token_hash == hash_session_token(token))
        )
        await session.commit()
    response.delete_cookie(settings.session_cookie_name, path="/")
    return response
