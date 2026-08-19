from __future__ import annotations

from collections.abc import Callable

from app.core.exceptions import UnknownTaskError
from app.tasks.base import TaskHandler

_TASKS: dict[str, TaskHandler] = {}


def task(name: str) -> Callable[[TaskHandler], TaskHandler]:
    def decorator(handler: TaskHandler) -> TaskHandler:
        if name in _TASKS:
            raise RuntimeError(f"Task {name!r} is already registered")
        _TASKS[name] = handler
        return handler

    return decorator


def get_task(name: str) -> TaskHandler:
    try:
        return _TASKS[name]
    except KeyError as exc:
        raise UnknownTaskError(name) from exc


def registered_tasks() -> tuple[str, ...]:
    return tuple(sorted(_TASKS))


def clear_registry() -> None:
    """Test helper; production code should not mutate the registry."""
    _TASKS.clear()
