"""Progress tracking for backfill mode with ETA calculation."""

from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timedelta
from multiprocessing import Value
from multiprocessing.sharedctypes import Synchronized
from typing import Deque, Dict, List, Optional, Tuple

from .logging_config import get_logger
from .models import Progress

logger = get_logger(__name__)


class BackfillProgressTracker:
    """Tracks backfill progress across multiple worker processes.

    Calculates total messages to process at startup, tracks processed count
    via shared memory, and estimates time to completion based on the
    processing rate over the last 3 minutes.
    """

    # Rolling window duration for ETA calculation
    RATE_WINDOW_SECONDS = 180  # 3 minutes
    # How often to sample progress for rate calculation
    SAMPLE_INTERVAL_SECONDS = 5

    def __init__(self) -> None:
        self._total_to_process: Synchronized[int] = Value('q', 0)
        self._total_processed: Synchronized[int] = Value('q', 0)
        self._total_tables: Synchronized[int] = Value('i', 0)
        self._tables_complete: Synchronized[int] = Value('i', 0)
        self._start_time: Optional[datetime] = None
        # Rolling window of (timestamp, processed_count) samples for rate calculation
        self._rate_samples: Deque[Tuple[float, int]] = deque()
        self._last_sample_time: float = 0

    def set_total_to_process(self, total: int) -> None:
        """Set the total number of messages to process (called at startup)."""
        with self._total_to_process.get_lock():
            self._total_to_process.value = total
        self._start_time = datetime.now()
        logger.info(f"Backfill progress: {total:,} total offset range to process")

    def set_total_tables(self, total: int) -> None:
        """Set the total number of tables being backfilled."""
        with self._total_tables.get_lock():
            self._total_tables.value = total

    def get_tables_complete_counter(self) -> Synchronized[int]:
        """Get the shared counter for workers to increment on completion."""
        return self._tables_complete

    def get_shared_counter(self) -> Synchronized[int]:
        """Get the shared counter for workers to increment."""
        return self._total_processed

    @staticmethod
    def increment_processed(counter: Synchronized[int], count: int = 1) -> None:
        """Increment the processed count (called from worker processes)."""
        with counter.get_lock():
            counter.value += count

    def get_total_to_process(self) -> int:
        """Get the total number of messages to process."""
        return self._total_to_process.value

    def get_total_processed(self) -> int:
        """Get the current number of processed messages."""
        return self._total_processed.value

    def get_completion_percentage(self) -> float:
        """Get the completion percentage (0-100)."""
        total = self.get_total_to_process()
        if total == 0:
            return 100.0
        return (self.get_total_processed() / total) * 100

    def _update_rate_samples(self) -> None:
        """Add a new sample and prune old ones outside the window."""
        now = time.time()

        # Only sample at intervals to avoid excessive overhead
        if now - self._last_sample_time < self.SAMPLE_INTERVAL_SECONDS:
            return

        processed = self.get_total_processed()
        self._rate_samples.append((now, processed))
        self._last_sample_time = now

        # Prune samples older than the window
        cutoff = now - self.RATE_WINDOW_SECONDS
        while self._rate_samples and self._rate_samples[0][0] < cutoff:
            self._rate_samples.popleft()

    def get_messages_per_second(self) -> Optional[float]:
        """Get the average processing rate over the last 3 minutes.

        Returns None if not enough data is available.
        """
        self._update_rate_samples()

        if len(self._rate_samples) < 2:
            return None

        oldest_time, oldest_count = self._rate_samples[0]
        newest_time, newest_count = self._rate_samples[-1]

        elapsed = newest_time - oldest_time
        if elapsed <= 0:
            return None

        messages_processed = newest_count - oldest_count
        return messages_processed / elapsed

    def get_estimated_time_remaining(self) -> Optional[timedelta]:
        """Get the estimated time remaining based on recent processing rate.

        Returns None if not enough data is available for estimation.
        """
        rate = self.get_messages_per_second()
        if rate is None or rate <= 0:
            return None

        remaining = self.get_total_to_process() - self.get_total_processed()
        if remaining <= 0:
            return timedelta(seconds=0)

        seconds_remaining = remaining / rate
        return timedelta(seconds=int(seconds_remaining))

    def get_estimated_completion_time(self) -> Optional[datetime]:
        """Get the estimated completion time.

        Returns None if not enough data is available for estimation.
        """
        eta = self.get_estimated_time_remaining()
        if eta is None:
            return None
        return datetime.now() + eta

    def format_progress_report(self) -> str:
        """Format a human-readable progress report string."""
        total = self.get_total_to_process()
        processed = self.get_total_processed()
        pct = self.get_completion_percentage()
        rate = self.get_messages_per_second()
        eta = self.get_estimated_time_remaining()

        total_tables = self._total_tables.value
        tables_done = self._tables_complete.value

        parts = [
            f"Progress: {processed:,}/{total:,} ({pct:.1f}%)"
        ]

        if total_tables > 0:
            parts.append(f"{tables_done}/{total_tables} tables complete")

        if rate is not None:
            parts.append(f"Rate: {rate:,.0f} offsets/s")

        if eta is not None:
            # Format ETA nicely
            total_seconds = int(eta.total_seconds())
            if total_seconds < 60:
                eta_str = f"{total_seconds}s"
            elif total_seconds < 3600:
                minutes = total_seconds // 60
                seconds = total_seconds % 60
                eta_str = f"{minutes}m {seconds}s"
            else:
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                eta_str = f"{hours}h {minutes}m"
            parts.append(f"ETA: {eta_str}")
        elif processed > 0:
            parts.append("ETA: calculating...")

        return " | ".join(parts)


def calculate_total_messages_to_process(
    high_watermarks: Dict[str, Dict[int, int]],
    progress_by_topic: Dict[str, List[Progress]]
) -> int:
    """Calculate total messages to consume across all topics/partitions.

    Args:
        high_watermarks: topic -> partition -> high watermark offset
        progress_by_topic: topic -> list of Progress records with last handled offsets

    Returns:
        Total number of messages to process
    """
    total = 0

    for topic, partition_watermarks in high_watermarks.items():
        # Build a map of partition -> last handled offset from progress
        progress_records = progress_by_topic.get(topic, [])
        last_offset_by_partition: Dict[int, int] = {
            p.source_topic_partition: p.last_handled_message_offset for p in progress_records
        }

        for partition, high_watermark in partition_watermarks.items():
            # Start offset is either last_handled + 1 or 0 (beginning)
            last_handled = last_offset_by_partition.get(partition, -1)
            start_offset = last_handled + 1

            # Messages to consume = high_watermark - start_offset
            # high_watermark is the next offset to be assigned, so messages available
            # are from start_offset to high_watermark - 1
            messages_available = max(0, high_watermark - start_offset)
            total += messages_available

            logger.debug(f"Topic {topic} partition {partition}: "
                        f"start={start_offset}, high={high_watermark}, available={messages_available}")

    return total
