from enum import StrEnum


class PublicationStatus(StrEnum):
    DRAFT = "draft"
    SUBMITTED = "submitted"
    EDITORIAL_REVIEW = "editorial_review"
    CHANGES_REQUESTED = "changes_requested"
    SCHEDULED = "scheduled"
    PUBLISHED = "published"
    ARCHIVED = "archived"
    REJECTED = "rejected"


class ContentLocale(StrEnum):
    RUSSIAN = "ru"
    ENGLISH = "en"


def translation_target_locale(source_locale: ContentLocale | str) -> ContentLocale:
    locale = ContentLocale(source_locale)
    return ContentLocale.ENGLISH if locale is ContentLocale.RUSSIAN else ContentLocale.RUSSIAN


class EditorialDecision(StrEnum):
    SCHEDULE = "schedule"
    PUBLISH = "publish"
    REJECT = "reject"
    REQUEST_CHANGES = "request_changes"


def resolve_editorial_decision(decision: EditorialDecision | str) -> PublicationStatus:
    try:
        editorial_decision = EditorialDecision(decision)
    except ValueError as error:
        raise ValueError("Unsupported editorial decision") from error

    return {
        EditorialDecision.SCHEDULE: PublicationStatus.SCHEDULED,
        EditorialDecision.PUBLISH: PublicationStatus.PUBLISHED,
        EditorialDecision.REJECT: PublicationStatus.REJECTED,
        EditorialDecision.REQUEST_CHANGES: PublicationStatus.CHANGES_REQUESTED,
    }[editorial_decision]
