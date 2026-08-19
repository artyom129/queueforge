from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(prog="queueforge")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("worker", help="Run a QueueForge worker")
    sub.add_parser("scheduler", help="Run the delayed-job scheduler")
    sub.add_parser("outbox", help="Run the transactional outbox publisher")
    args = parser.parse_args()

    if args.command == "worker":
        from app.workers.worker import main as run
    elif args.command == "scheduler":
        from app.scheduler.scheduler import main as run
    else:
        from app.queue.outbox import main as run
    run()


if __name__ == "__main__":
    main()
