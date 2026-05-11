from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Iterable, List


@dataclass(frozen=True)
class ScheduledProcess:
    process_id: int
    start_event: int
    end_event: int
    event_count: int
    mode: str
    stages: tuple[str, ...]


def build_schedule_plan(total_events: int, threshold_k: int) -> List[ScheduledProcess]:
    if total_events < 0:
        raise ValueError("N must be >= 0")
    if threshold_k <= 0:
        raise ValueError("K must be > 0")
    if total_events == 0:
        return []

    processes: List[ScheduledProcess] = []
    next_event = 1
    process_id = 1

    if total_events < threshold_k:
        processes.append(
            ScheduledProcess(
                process_id=1,
                start_event=1,
                end_event=total_events,
                event_count=total_events,
                mode="EXTRACT_ONLY",
                stages=("Extract",),
            )
        )
        return processes

    full_processes = total_events // threshold_k
    remainder = total_events % threshold_k

    for _ in range(full_processes):
        end_event = next_event + threshold_k - 1
        processes.append(
            ScheduledProcess(
                process_id=process_id,
                start_event=next_event,
                end_event=end_event,
                event_count=threshold_k,
                mode="FULL_ETL",
                stages=("Extract", "Transform", "Load"),
            )
        )
        next_event = end_event + 1
        process_id += 1

    if remainder:
        end_event = next_event + remainder - 1
        processes.append(
            ScheduledProcess(
                process_id=process_id,
                start_event=next_event,
                end_event=end_event,
                event_count=remainder,
                mode="EXTRACT_ONLY",
                stages=("Extract",),
            )
        )

    return processes


def expected_process_count(total_events: int, threshold_k: int) -> int:
    if total_events <= 0:
        return 0
    if total_events < threshold_k:
        return 1
    return ceil(total_events / threshold_k)


def iter_process_messages(processes: Iterable[ScheduledProcess]) -> Iterable[str]:
    for process in processes:
        yield (
            f"Process {process.process_id}: events {process.start_event}-{process.end_event} "
            f"({process.event_count} events) -> {process.mode} [{', '.join(process.stages)}]"
        )
