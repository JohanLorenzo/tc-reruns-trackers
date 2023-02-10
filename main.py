#!/usr/bin/env python3

import argparse
import asyncio
import os
import taskcluster.aio
import logging


queue = taskcluster.aio.Queue({"rootUrl": os.environ["TASKCLUSTER_ROOT_URL"]})
log = logging.getLogger(__name__)


async def async_main(task_group_id):
    task_group_ids = await find_parent_task_group_id(task_group_id)
    list_of_list_of_tasks = await asyncio.gather(
        *[
            get_all_tasks_in_task_group(task_group_id)
            for task_group_id in task_group_ids
        ]
    )
    tasks = [task for list_of_tasks in list_of_list_of_tasks for task in list_of_tasks]
    log.info(f"Found {len(tasks)} tasks among all task groups")
    print_rerun_tasks(tasks)


async def find_parent_task_group_id(task_group_id):
    task_group_ids = set([task_group_id])
    breakpoint()
    task_id = task_group_id
    while True:
        task = await queue.task(task_id)
        task_group_ids.update(
            set(
                task.get("extra", {})
                .get("action", {})
                .get("context", {})
                .get("input", {})
                .get("previous_graph_ids", [])
            )
        )
        if task["taskGroupId"] == task_id:
            break
        else:
            task_id = task["taskGroupId"]
            task_group_ids.add(task_id)
    return list(task_group_ids)


async def get_all_tasks_in_task_group(task_group_id):
    log.info(f"Looking up all tasks in task group {task_group_id}...")
    tasks = []
    continuation_token = ""
    while True:
        query = {"continuationToken": continuation_token} if continuation_token else {}
        task_group = await queue.listTaskGroup(
            task_group_id,
            query=query,
        )
        tasks.extend(task_group["tasks"])
        continuation_token = task_group.get("continuationToken")
        if continuation_token:
            log.info(
                f"Still querying task group {task_group_id}... ({len(tasks)} tasks found so far)"
            )
        else:
            break

    return tasks


def print_rerun_tasks(tasks):
    rerun_tasks = {
        task["status"]["taskId"]: {
            "last_scheduled": task["status"]["runs"][-1]["scheduled"],
            "task_name": task["task"]["metadata"]["name"],
        }
        for task in tasks
        if len(task.get("status", {}).get("runs", [])) > 1
    }
    pretty_tasks = "\n  ".join(
        [
            f"{task['last_scheduled']} {task_id} {task['task_name']}"
            for task_id, task in sorted(
                rerun_tasks.items(), key=lambda t: t[1]["last_scheduled"]
            )
        ]
    )
    log.warning(
        f"Found {len(rerun_tasks)} rerun tasks:\n"
        "  [    LAST SCHEDULED    ] [      TASK ID       ] [       TASK NAME       ]\n"
        f"  {pretty_tasks}"
    )


def _init_logging(config):
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.DEBUG if config.verbose else logging.INFO,
    )
    logging.getLogger("taskcluster").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description="Track TC reruns")
    parser.add_argument("task_group_id")
    parser.add_argument("--verbose", action="store_true")
    config = parser.parse_args()
    _init_logging(config)

    asyncio.run(async_main(config.task_group_id))


__name__ == "__main__" and main()
