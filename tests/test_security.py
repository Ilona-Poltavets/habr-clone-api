import pytest

from ion_pulse.core.security import (
    create_session_token,
    hash_password,
    hash_password_reset_token,
    hash_session_token,
    verify_password,
)
from ion_pulse.services.rate_limits import enforce_rate_limit, reset_rate_limits


def test_passwords_are_hashed_and_verified() -> None:
    password_hash = hash_password("a secure test password")

    assert password_hash != "a secure test password"
    assert verify_password("a secure test password", password_hash)
    assert not verify_password("wrong password", password_hash)


def test_session_tokens_are_random_and_only_the_hash_is_persistable() -> None:
    first = create_session_token()
    second = create_session_token()

    assert first != second
    assert hash_session_token(first) == hash_session_token(first)
    assert hash_session_token(first) != first


def test_reset_tokens_use_a_separate_hash_namespace() -> None:
    token = create_session_token()

    assert hash_password_reset_token(token) != hash_session_token(token)


def test_rate_limit_rejects_requests_beyond_the_window_capacity() -> None:
    reset_rate_limits()
    enforce_rate_limit("comment", "user-1", limit=2, window_seconds=60)
    enforce_rate_limit("comment", "user-1", limit=2, window_seconds=60)

    with pytest.raises(Exception, match="Too many requests"):
        enforce_rate_limit("comment", "user-1", limit=2, window_seconds=60)

    reset_rate_limits()
