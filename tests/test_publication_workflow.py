import pytest

from ion_pulse.domain.publications import (
    EditorialDecision,
    PublicationStatus,
    resolve_editorial_decision,
)


@pytest.mark.parametrize(
    ("decision", "expected_status"),
    [
        (EditorialDecision.PUBLISH, PublicationStatus.PUBLISHED),
        (EditorialDecision.REJECT, PublicationStatus.REJECTED),
        (EditorialDecision.REQUEST_CHANGES, PublicationStatus.CHANGES_REQUESTED),
    ],
)
def test_editorial_decision_resolves_to_expected_publication_status(
    decision: EditorialDecision,
    expected_status: PublicationStatus,
) -> None:
    assert resolve_editorial_decision(decision) is expected_status


def test_unknown_editorial_decision_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported editorial decision"):
        resolve_editorial_decision("archive")
