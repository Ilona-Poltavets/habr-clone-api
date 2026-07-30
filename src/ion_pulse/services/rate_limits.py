from collections import defaultdict, deque
from time import monotonic

from fastapi import HTTPException, status

_requests: dict[tuple[str, str], deque[float]] = defaultdict(deque)


def enforce_rate_limit(scope: str, key: str, *, limit: int, window_seconds: float) -> None:
    """Apply a process-local sliding window limit for abuse-prone actions.

    The function intentionally has no persistent storage: deployments with multiple
    workers should put the API behind an edge limiter as documented infrastructure.
    """
    now = monotonic()
    bucket = _requests[(scope, key)]
    cutoff = now - window_seconds
    while bucket and bucket[0] <= cutoff:
        bucket.popleft()
    if len(bucket) >= limit:
        retry_after = max(1, int(window_seconds - (now - bucket[0])))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests. Try again later.",
            headers={"Retry-After": str(retry_after)},
        )
    bucket.append(now)


def reset_rate_limits() -> None:
    """Clear process-local state for deterministic tests."""
    _requests.clear()
