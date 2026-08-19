# Import built-in tasks for registration side effects.
from app.tasks import examples as examples
from app.tasks.registry import get_task, registered_tasks, task

__all__ = ["get_task", "registered_tasks", "task", "examples"]
