from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.domain.publications import PublicationStatus
from ion_pulse.domain.roles import RoleCode
from ion_pulse.models.identity import User
from ion_pulse.models.publications import (
    Category,
    JournalIssue,
    JournalIssuePublication,
    Publication,
    PublicationLocalization,
)
from ion_pulse.schemas.publications import (
    DigestItemRead,
    JournalIssueCreate,
    JournalIssuePublicationsUpdate,
    JournalIssueRead,
)

router = APIRouter(prefix="/journal")


def require_journal_access(user: User) -> None:
    roles = {RoleCode.EDITOR.value, RoleCode.MODERATOR.value, RoleCode.ADMINISTRATOR.value}
    if not roles.intersection(role.code for role in user.roles):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Journal role required")


def to_issue(issue: JournalIssue) -> JournalIssueRead:
    return JournalIssueRead.model_validate(issue, from_attributes=True)


@router.get("/issues", response_model=list[JournalIssueRead])
async def list_published_issues(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[JournalIssueRead]:
    issues = (
        await session.scalars(
            select(JournalIssue)
            .where(JournalIssue.status == "published")
            .order_by(JournalIssue.published_at.desc())
        )
    ).all()
    return [to_issue(issue) for issue in issues]


@router.get("/issues/{issue_id}/publications", response_model=list[DigestItemRead])
async def list_issue_publications(
    issue_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    locale: str = "ru",
) -> list[DigestItemRead]:
    if locale not in {"ru", "en"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported locale"
        )
    issue = await session.get(JournalIssue, issue_id)
    if issue is None or issue.status != "published":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Published issue not found"
        )
    materials = (
        await session.scalars(
            select(JournalIssuePublication)
            .where(JournalIssuePublication.issue_id == issue.id)
            .order_by(JournalIssuePublication.position)
        )
    ).all()
    if materials and all(material.snapshot is not None for material in materials):
        result: list[DigestItemRead] = []
        for material in materials:
            snapshot = material.snapshot
            if snapshot is None:
                continue
            localizations = snapshot.get("localizations")
            source_locale = snapshot.get("source_locale")
            category_slug = snapshot.get("category_slug")
            if (
                not isinstance(localizations, dict)
                or not isinstance(source_locale, str)
                or not isinstance(category_slug, str)
            ):
                break
            localized = localizations.get(locale) or localizations.get(source_locale)
            if not isinstance(localized, dict):
                break
            title = localized.get("title")
            summary = localized.get("summary")
            if not isinstance(title, str) or not isinstance(summary, str):
                break
            result.append(
                DigestItemRead(
                    id=material.publication_id,
                    category_slug=category_slug,
                    title=title,
                    summary=summary,
                )
            )
        if len(result) == len(materials):
            return result
    source = aliased(PublicationLocalization)
    requested = aliased(PublicationLocalization)
    rows = await session.execute(
        select(JournalIssuePublication, Publication, Category, source, requested)
        .join(Publication, Publication.id == JournalIssuePublication.publication_id)
        .join(Category, Category.id == Publication.category_id)
        .join(
            source,
            (source.publication_id == Publication.id)
            & (source.locale == Publication.source_locale),
        )
        .outerjoin(
            requested,
            (requested.publication_id == Publication.id)
            & (requested.locale == locale)
            & (requested.translation_status == "ready"),
        )
        .where(JournalIssuePublication.issue_id == issue.id)
        .order_by(JournalIssuePublication.position)
    )
    return [
        DigestItemRead(
            id=publication.id,
            category_slug=category.slug,
            title=(localized or original).title,
            summary=(localized or original).summary,
        )
        for _, publication, category, original, localized in rows
    ]


def can_manage_issue(issue: JournalIssue, user: User) -> bool:
    return issue.editor_id == user.id or RoleCode.ADMINISTRATOR.value in {
        role.code for role in user.roles
    }


@router.post("/issues", response_model=JournalIssueRead, status_code=status.HTTP_201_CREATED)
async def create_issue(
    payload: JournalIssueCreate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> JournalIssueRead:
    require_journal_access(user)
    issue = JournalIssue(editor_id=user.id, **payload.model_dump())
    session.add(issue)
    await session.commit()
    await session.refresh(issue)
    return to_issue(issue)


@router.put("/issues/{issue_id}/publications", status_code=status.HTTP_204_NO_CONTENT)
async def replace_issue_publications(
    issue_id: UUID,
    payload: JournalIssuePublicationsUpdate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> None:
    require_journal_access(user)
    issue = await session.get(JournalIssue, issue_id)
    if issue is None or not can_manage_issue(issue, user) or issue.status != "draft":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Editable issue not found"
        )
    if len(set(payload.publication_ids)) != len(payload.publication_ids):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Items must be unique"
        )
    found = (
        await session.scalars(
            select(Publication.id).where(
                Publication.id.in_(payload.publication_ids),
                Publication.status == PublicationStatus.PUBLISHED.value,
            )
        )
    ).all()
    if len(found) != len(payload.publication_ids):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Items must be published"
        )
    await session.execute(
        delete(JournalIssuePublication).where(JournalIssuePublication.issue_id == issue.id)
    )
    session.add_all(
        [
            JournalIssuePublication(issue_id=issue.id, publication_id=item, position=index)
            for index, item in enumerate(payload.publication_ids, 1)
        ]
    )
    await session.commit()


@router.post("/issues/{issue_id}/publish", response_model=JournalIssueRead)
async def publish_issue(
    issue_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> JournalIssueRead:
    require_journal_access(user)
    issue = await session.get(JournalIssue, issue_id)
    if issue is None or not can_manage_issue(issue, user) or issue.status != "draft":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Draft issue not found")
    if (
        await session.scalar(
            select(JournalIssuePublication.issue_id)
            .where(JournalIssuePublication.issue_id == issue.id)
            .limit(1)
        )
        is None
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Issue must contain publications",
        )
    materials = (
        await session.scalars(
            select(JournalIssuePublication)
            .where(JournalIssuePublication.issue_id == issue.id)
            .order_by(JournalIssuePublication.position)
        )
    ).all()
    for material in materials:
        publication = await session.get(Publication, material.publication_id)
        if publication is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Material missing")
        category = await session.get(Category, publication.category_id)
        localizations = (
            await session.scalars(
                select(PublicationLocalization).where(
                    PublicationLocalization.publication_id == publication.id,
                    PublicationLocalization.translation_status == "ready",
                )
            )
        ).all()
        material.snapshot = {
            "category_slug": category.slug if category is not None else "uncategorized",
            "source_locale": publication.source_locale,
            "localizations": {
                localization.locale: {
                    "title": localization.title,
                    "summary": localization.summary,
                    "body": localization.body,
                }
                for localization in localizations
            },
        }
    issue.status = "published"
    issue.published_at = datetime.now(UTC)
    await session.commit()
    await session.refresh(issue)
    return to_issue(issue)
