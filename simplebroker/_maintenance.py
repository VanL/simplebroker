"""Backend-neutral policy for opportunistic automatic maintenance."""


class MaintenanceSchedule:
    """Track committed message activity between maintenance checks."""

    def __init__(self, interval: int) -> None:
        self._interval = max(1, int(interval))
        self._completed = 0

    def record(self, completed: int) -> bool:
        """Record committed activity and return whether a check is due."""
        if completed < 0:
            raise ValueError("completed must be non-negative")
        if completed == 0:
            return False
        self._completed += completed
        return self._completed >= self._interval

    def mark_check_succeeded(self) -> None:
        """Consume completed intervals while preserving the remainder."""
        self._completed %= self._interval


def vacuum_is_eligible(
    *, claimed_count: int, total_count: int, threshold: float
) -> bool:
    """Return whether claimed-message cleanup should run."""
    if total_count == 0:
        return False
    return (claimed_count >= total_count * threshold) or claimed_count > 10_000
