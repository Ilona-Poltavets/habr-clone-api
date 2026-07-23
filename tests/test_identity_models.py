import ion_pulse.models  # noqa: F401
from ion_pulse.db.base import Base
from ion_pulse.domain.roles import RoleCode


def test_identity_schema_has_multi_role_relationship() -> None:
    assert {"users", "roles", "user_roles", "user_sessions"} <= set(Base.metadata.tables)

    user_roles = Base.metadata.tables["user_roles"]
    assert {column.name for column in user_roles.primary_key.columns} == {"user_id", "role_id"}


def test_editorial_and_translation_tables_preserve_publication_history() -> None:
    assert {"publication_editorial_reviews", "translation_jobs"} <= set(Base.metadata.tables)

    editorial_reviews = Base.metadata.tables["publication_editorial_reviews"]
    foreign_key = next(iter(editorial_reviews.foreign_keys))
    assert foreign_key.ondelete == "RESTRICT"

    translation_jobs = Base.metadata.tables["translation_jobs"]
    assert {column.name for column in translation_jobs.columns} >= {
        "publication_id",
        "target_locale",
        "status",
        "attempts",
    }


def test_assignable_role_codes_match_product_model() -> None:
    assert {role.value for role in RoleCode} == {
        "author",
        "editor",
        "moderator",
        "content_manager",
        "administrator",
    }
