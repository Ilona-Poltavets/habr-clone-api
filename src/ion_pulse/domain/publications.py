from enum import StrEnum


class PublicationStatus(StrEnum):
    DRAFT = "draft"
    SUBMITTED = "submitted"
    EDITORIAL_REVIEW = "editorial_review"
    CHANGES_REQUESTED = "changes_requested"
    PUBLISHED = "published"
    REJECTED = "rejected"


class ContentLocale(StrEnum):
    RUSSIAN = "ru"
    ENGLISH = "en"


class EditorialDecision(StrEnum):
    PUBLISH = "publish"
    REJECT = "reject"
    REQUEST_CHANGES = "request_changes"


def resolve_editorial_decision(decision: EditorialDecision | str) -> PublicationStatus:
    try:
        editorial_decision = EditorialDecision(decision)
    except ValueError as error:
        raise ValueError("Unsupported editorial decision") from error

    return {
        EditorialDecision.PUBLISH: PublicationStatus.PUBLISHED,
        EditorialDecision.REJECT: PublicationStatus.REJECTED,
        EditorialDecision.REQUEST_CHANGES: PublicationStatus.CHANGES_REQUESTED,
    }[editorial_decision]
