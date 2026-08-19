from __future__ import annotations

import asyncio

import httpx


async def main() -> None:
    jobs = [
        {"type": "echo", "payload": {"message": "hello from QueueForge"}},
        {
            "type": "unstable_task",
            "payload": {"fail_until_attempt": 2},
            "max_retries": 3,
        },
        {"type": "slow_task", "payload": {"seconds": 2}, "timeout_seconds": 10},
    ]
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        for payload in jobs:
            response = await client.post("/api/v1/jobs", json=payload)
            response.raise_for_status()
            print(response.json())


if __name__ == "__main__":
    asyncio.run(main())
