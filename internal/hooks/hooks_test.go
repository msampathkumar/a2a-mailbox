package hooks_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/msampathkumar/a2a-mailbox/internal/hooks"
	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

func TestLoadDotEnv_ParsesMailboxWorkerAndHooksSections(t *testing.T) {
	t.Parallel()

	envContent := `
# ==========================================
# [a2a-mailbox]
# ==========================================
A2A_MAILBOX_HOST=127.0.0.1
A2A_MAILBOX_PORT=8099
A2A_MAILBOX_BACKEND="db"

# ==========================================
# [workers]
# ==========================================
A2A_WORKER_ID=py-worker-01
A2A_WORKER_STATE=completed

# ==========================================
# [hooks]
# ==========================================
A2A_HOOK_BEFORE_ARRIVE=./examples/validate.py
A2A_HOOK_AFTER_ARRIVE=./examples/report_responder.py
`
	envPath := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(envPath, []byte(envContent), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	vals, err := hooks.LoadDotEnv(envPath)
	if err != nil {
		t.Fatalf("LoadDotEnv() error = %v", err)
	}
	if vals["A2A_MAILBOX_PORT"] != "8099" || vals["A2A_MAILBOX_BACKEND"] != "db" {
		t.Fatalf("unexpected mailbox env values: %+v", vals)
	}
	if vals["A2A_WORKER_ID"] != "py-worker-01" {
		t.Fatalf("unexpected worker env values: %+v", vals)
	}
	if vals["A2A_HOOK_AFTER_ARRIVE"] != "./examples/report_responder.py" {
		t.Fatalf("unexpected hook env values: %+v", vals)
	}
}

func TestHookedBackend_AllSixLifecycleHooks_And_PythonAutoReport(t *testing.T) {
	t.Parallel()

	pythonBin, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found on PATH")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}

	// Create a Python hook script that logs every lifecycle event to STDOUT
	// and on `after_arrive` prints a formatted report and finishes the task!
	pyScript := filepath.Join(tmpDir, "hook.py")
	pyCode := `#!` + pythonBin + `
import json
import os
import sys

event = os.environ.get("A2A_HOOK_EVENT", "")
task_id = os.environ.get("A2A_TASK_ID", "")
ctx_id = os.environ.get("A2A_CONTEXT_ID", "")
text = os.environ.get("A2A_MESSAGE_TEXT", "")

print(f"[HOOK:{event}] task={task_id} ctx={ctx_id} text={text}")

if event == "after_arrive":
    print("========================================")
    print("       INCOMING A2A MAIL REPORT         ")
    print("========================================")
    print(f"Task ID:    {task_id}")
    print(f"Context ID: {ctx_id}")
    print(f"Content:    {text}")
    print("========================================")
    result = {
        "state": "completed",
        "workerId": "python-report-hook",
        "text": f"Python report generated for: {text}",
    }
    print("A2A_HOOK_RESULT: " + json.dumps(result))
`
	if err := os.WriteFile(pyScript, []byte(pyCode), 0o700); err != nil {
		t.Fatalf("WriteFile(hook.py): %v", err)
	}

	var hookStdout bytes.Buffer
	hb := hooks.NewBackend(db, hooks.Config{
		BeforeArrive: pyScript,
		AfterArrive:  pyScript,
		BeforeRead:   pyScript,
		AfterRead:    pyScript,
		BeforeUpdate: pyScript,
		AfterUpdate:  pyScript,
		Out:          &hookStdout,
	})

	// 1. RegisterMessage triggers `before_arrive` and `after_arrive` (which also runs RespondTask -> `before_update` & `after_update`)
	task, err := hb.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-hook-1",
		Text:      "Analyze Q4 revenue metrics",
	})
	if err != nil {
		t.Fatalf("RegisterMessage() error = %v", err)
	}
	if task.Status.State != "TASK_STATE_COMPLETED" {
		t.Fatalf("expected after_arrive hook to complete the task, got state=%q", task.Status.State)
	}

	// 2. GetTask triggers `before_read` and `after_read`
	readTask, err := hb.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if readTask.Status.State != "TASK_STATE_COMPLETED" {
		t.Fatalf("GetTask state = %q, want TASK_STATE_COMPLETED", readTask.Status.State)
	}

	outStr := hookStdout.String()
	for _, want := range []string{
		"[HOOK:before_arrive]",
		"[HOOK:after_arrive]",
		"INCOMING A2A MAIL REPORT",
		"Analyze Q4 revenue metrics",
		"[HOOK:before_update]",
		"[HOOK:after_update]",
		"[HOOK:before_read]",
		"[HOOK:after_read]",
	} {
		if !strings.Contains(outStr, want) {
			t.Fatalf("hook stdout missing %q:\n%s", want, outStr)
		}
	}
}
