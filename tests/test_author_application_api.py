from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.main import app
from ion_pulse.models.identity import Role, User


@pytest.mark.asyncio
async def test_administrator_cannot_decide_author_application_without_reason() -> None:
    administrator = User(
        id=uuid4(),
        email="admin@example.test",
        display_name="Administrator",
        password_hash="not-used",
        roles=[Role(id=1, code="administrator", name="Administrator")],
    )

    async def override_user() -> User:
        return administrator

    async def override_db_session():
        yield None

    app.dependency_overrides[get_current_user] = override_user
    app.dependency_overrides[get_db_session] = override_db_session
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.patch(
                f"/api/v1/author-applications/{uuid4()}",
                json={"status": "approved"},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "review_note"]


@pytest.mark.asyncio
async def test_administrator_cannot_decide_author_application_with_blank_reason() -> None:
    administrator = User(
        id=uuid4(),
        email="admin@example.test",
        display_name="Administrator",
        password_hash="not-used",
        roles=[Role(id=1, code="administrator", name="Administrator")],
    )

    async def override_user() -> User:
        return administrator

    async def override_db_session():
        yield None

    app.dependency_overrides[get_current_user] = override_user
    app.dependency_overrides[get_db_session] = override_db_session
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.patch(
                f"/api/v1/author-applications/{uuid4()}",
                json={"status": "approved", "review_note": "   "},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "review_note"]
