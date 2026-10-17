#!/usr/bin/env python3
"""
a2a-mailbox Python Report Responder Hook / Worker Demo.

Can be triggered automatically via `.env` lifecycle hooks:
  A2A_HOOK_AFTER_ARRIVE="python3 ./examples/report_responder.py"
or via the background worker CLI:
  a2a-mailbox work --once --exec ./examples/report_responder.py

Every time a mail arrives in `a2a-mailbox`, this script reads the incoming A2A
task/message, prints a formatted report to STDOUT, attaches a Markdown report
artifact, and marks the task as TASK_STATE_COMPLETED.
"""

import base64
import datetime
import json
import os
import sys


def main() -> int:
    raw_stdin = ""
    if not sys.stdin.isatty():
        raw_stdin = sys.stdin.read().strip()

    payload = {}
    if raw_stdin:
        try:
            payload = json.loads(raw_stdin)
        except json.JSONDecodeError:
            payload = {}

    event = os.environ.get("A2A_HOOK_EVENT") or payload.get("event") or "worker_exec"
    task_id = os.environ.get("A2A_TASK_ID") or payload.get("taskId") or "unknown-task"
    ctx_id = os.environ.get("A2A_CONTEXT_ID") or payload.get("contextId") or "unknown-context"
    text = os.environ.get("A2A_MESSAGE_TEXT") or payload.get("text") or ""

    task_obj = payload.get("task") or {}
    if not task_obj and os.environ.get("A2A_TASK_JSON"):
        try:
            task_obj = json.loads(os.environ["A2A_TASK_JSON"])
        except json.JSONDecodeError:
            task_obj = {}

    history = task_obj.get("history") or []
    turn_count = len(history) if history else 1
    metadata = task_obj.get("metadata") or {}
    record_kind = metadata.get("kind") or "message"

    # Extract attached file parts from the incoming message / history
    attached_files = []
    for msg in history:
        for p in (msg.get("parts") or []):
            fname = p.get("filename") or ""
            mtype = p.get("mediaType") or "application/octet-stream"
            raw_b64 = p.get("raw") or ""
            url = p.get("url") or ""
            if raw_b64:
                try:
                    byte_len = len(base64.b64decode(raw_b64))
                except Exception:
                    byte_len = len(raw_b64)
                attached_files.append(f"{fname or 'inline-file'} ({mtype}, {byte_len} bytes)")
            elif url:
                attached_files.append(f"{fname or url} ({url})")

    words = len(text.split())
    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    # 1. Present the incoming mail as a structured report on STDOUT
    report_lines = [
        "╭────────────────────────────────────────────────────────────────────╮",
        "│                📬 A2A-MAILBOX INCOMING MAIL REPORT                 │",
        "├────────────────────────────────────────────────────────────────────┤",
        f"│ Timestamp (UTC) : {now_utc:<48} │",
        f"│ Trigger Event   : {event:<48} │",
        f"│ Record Kind     : {record_kind + ' -> promoted to task':<48} │",
        f"│ Task ID         : {task_id:<48} │",
        f"│ Context ID      : {ctx_id:<48} │",
        f"│ Conversation #  : Turn {turn_count:<43} │",
        f"│ Word Count      : {words:<48} │",
        f"│ Attached Files  : {len(attached_files):<48} │",
    ]
    for fdesc in attached_files:
        report_lines.append(f"│   📎 {fdesc:<61} │")
    report_lines.extend([
        "├────────────────────────────────────────────────────────────────────┤",
        "│ Message Content :                                                  │",
    ])
    for line in (text.splitlines() or ["(empty)"]):
        report_lines.append(f"│   {line:<64} │")
    report_lines.extend([
        "├────────────────────────────────────────────────────────────────────┤",
        "│ Action Taken    : Report printed to STDOUT & Task COMPLETED        │",
        "╰────────────────────────────────────────────────────────────────────╯",
    ])
    report_box = "\n".join(report_lines)
    print(report_box, flush=True)

    # 2. Build a Markdown report file attachment and finish the work (TASK_STATE_COMPLETED)
    md_report = (
        f"# A2A Mailbox Report — `{task_id}`\n\n"
        f"- **Context ID**: `{ctx_id}`\n"
        f"- **Processed At**: `{now_utc}`\n"
        f"- **Trigger**: `{event}`\n"
        f"- **Word Count**: `{words}`\n\n"
        f"## Incoming Message\n\n> {text}\n"
    )
    b64_report = base64.b64encode(md_report.encode("utf-8")).decode("ascii")

    result = {
        "state": "completed",
        "workerId": "python-report-responder",
        "text": f"Report generated and printed to STDOUT for task {task_id} ({words} words).",
        "files": [
            {
                "filename": f"report-{task_id}.md",
                "mediaType": "text/markdown",
                "raw": b64_report,
            }
        ],
    }

    if os.environ.get("A2A_HOOK_EVENT"):
        print("A2A_HOOK_RESULT: " + json.dumps(result), flush=True)
    else:
        print(json.dumps(result), flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
