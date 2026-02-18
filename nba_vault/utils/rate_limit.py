"""Rate limiting and retry logic for API requests."""

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

from nba_vault.utils.config import get_settings

logger = structlog.get_logger(__name__)
T = TypeVar("T")


class RateLimiter:
    """Token bucket rate limiter for API requests."""

    def __init__(self, rate: int, per: float = 60.0):
        """
        Initialize rate limiter.

        Args:
            rate: Number of requests allowed.
            per: Time period in seconds (default: 60).
        """
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.time()

    def acquire(self, block: bool = True) -> bool:
        """
        Acquire permission to make a request.

        Args:
            block: If True, block until request is allowed.

        Returns:
            True if request is allowed, False otherwise.
        """
        current = time.time()
        time_passed = current - self.last_check
        self.last_check = current

        # Refill allowance based on time passed
        self.allowance += time_passed * (self.rate / self.per)

        self.allowance = min(self.allowance, self.rate)

        if self.allowance < 1.0:
            if not block:
                return False

            # Calculate wait time with jitter
            sleep_time = (self.per - self.allowance * (self.per / self.rate)) / 2
            jitter = random.uniform(0.8, 1.2)  # ±20% jitter  # noqa: S311
            sleep_time *= jitter

            logger.debug(
                "Rate limit reached, sleeping",
                sleep_time=sleep_time,
                allowance=self.allowance,
            )
            time.sleep(sleep_time)
            self.allowance = 0.0
        else:
            self.allowance -= 1.0

        return True


class AsyncRateLimiter:
    """Async-friendly token bucket rate limiter using asyncio."""

    def __init__(self, rate: int, per: float = 60.0):
        """
        Initialize async rate limiter.

        Args:
            rate: Number of requests allowed.
            per: Time period in seconds (default: 60).
        """
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = 0.0

    async def acquire(self, block: bool = True) -> bool:
        """
        Acquire permission to make a request (async version).

        Args:
            block: If True, block until request is allowed.

        Returns:
            True if request is allowed, False otherwise.
        """
        current = asyncio.get_event_loop().time()
        time_passed = current - self.last_check
        self.last_check = current

        # Refill allowance based on time passed
        self.allowance += time_passed * (self.rate / self.per)

        self.allowance = min(self.allowance, self.rate)

        if self.allowance < 1.0:
            if not block:
                return False

            # Calculate wait time with jitter
            sleep_time = (self.per - self.allowance * (self.per / self.rate)) / 2
            jitter = random.uniform(0.8, 1.2)  # ±20% jitter  # noqa: S311
            sleep_time *= jitter

            logger.debug(
                "Rate limit reached, sleeping async",
                sleep_time=sleep_time,
                allowance=self.allowance,
            )
            await asyncio.sleep(sleep_time)  # NON-BLOCKING
            self.allowance = 0.0
        else:
            self.allowance -= 1.0

        return True


def retry_with_backoff(
    func,
    max_attempts: int | None = None,
    base_delay: int | None = None,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """
    Retry a function with exponential backoff and jitter.

    Args:
        func: Function to retry.
        max_attempts: Maximum number of attempts. If None, uses settings.
        base_delay: Base delay in seconds. If None, uses settings.
        exceptions: Exception types to catch and retry on.

    Returns:
        Return value of func on success.

    Raises:
        The last exception if all attempts fail.
    """
    settings = get_settings()
    max_attempts = max_attempts or settings.nba_api_retry_attempts
    base_delay = base_delay or settings.nba_api_retry_delay

    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt == max_attempts:
                logger.error(
                    "All retry attempts failed",
                    func=func.__name__,
                    attempts=attempt,
                    error=str(e),
                )
                raise

            # Calculate exponential backoff with jitter
            delay = base_delay * (2 ** (attempt - 1))
            jitter = random.uniform(0.8, 1.2)  # noqa: S311
            actual_delay = delay * jitter

            logger.warning(
                "Request failed, retrying",
                func=func.__name__,
                attempt=attempt,
                max_attempts=max_attempts,
                delay=actual_delay,
                error=str(e),
            )
            time.sleep(actual_delay)

    # Should never reach here, but mypy needs it
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry logic exhausted without result")


async def retry_with_backoff_async[T](
    func: Callable[[], Awaitable[T]],
    max_attempts: int | None = None,
    base_delay: int | None = None,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """
    Retry an async function with exponential backoff and jitter.

    Args:
        func: Async function to retry.
        max_attempts: Maximum number of attempts. If None, uses settings.
        base_delay: Base delay in seconds. If None, uses settings.
        exceptions: Exception types to catch and retry on.

    Returns:
        Return value of func on success.

    Raises:
        The last exception if all attempts fail.
    """
    settings = get_settings()
    max_attempts = max_attempts or settings.nba_api_retry_attempts
    base_delay = base_delay or settings.nba_api_retry_delay

    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await func()
        except exceptions as e:
            last_exception = e
            if attempt == max_attempts:
                logger.error(
                    "All retry attempts failed",
                    func=getattr(func, "__name__", str(func)),
                    attempts=attempt,
                    error=str(e),
                )
                raise

            # Calculate exponential backoff with jitter
            delay = base_delay * (2 ** (attempt - 1))
            jitter = random.uniform(0.8, 1.2)  # noqa: S311
            actual_delay = delay * jitter

            logger.warning(
                "Request failed, retrying async",
                func=getattr(func, "__name__", str(func)),
                attempt=attempt,
                max_attempts=max_attempts,
                delay=actual_delay,
                error=str(e),
            )
            await asyncio.sleep(actual_delay)  # NON-BLOCKING

    # Should never reach here, but mypy needs it
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry logic exhausted without result")
