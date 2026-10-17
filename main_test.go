package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

func TestRun_PluginInfoAndVersion(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"info"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("run(info) exit = %d, stderr = %s", code, errOut.String())
	}
	var info pluginInfo
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
		t.Fatalf("decode info error = %v", err)
	}
	if info.Name != "mailbox" || info.Version != "0.1.0" {
		t.Fatalf("info = %+v, want name=mailbox version=0.1.0", info)
	}

	out.Reset()
	code = run(context.Background(), []string{"--version"}, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "a2a-mailbox 0.1.0") {
		t.Fatalf("run(--version) = %d, out = %q", code, out.String())
	}
}

func TestRun_ListShowAndReplyCommands(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}
	task, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		Text: "Approve purchase order #77",
	})
	if err != nil {
		t.Fatalf("RegisterMessage() error = %v", err)
	}

	// 1. list
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"list", "--db", dbPath}, &out, &errOut); code != 0 {
		t.Fatalf("list exit = %d, stderr = %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), task.ID) || !strings.Contains(out.String(), task.ContextID) {
		t.Fatalf("list output missing task/context ID:\n%s", out.String())
	}

	// 2. reply
	out.Reset()
	if code := run(context.Background(), []string{"reply", "--db", dbPath, "--state", "completed", task.ID, "PO #77 approved."}, &out, &errOut); code != 0 {
		t.Fatalf("reply exit = %d, stderr = %s", code, errOut.String())
	}

	// 3. show
	out.Reset()
	if code := run(context.Background(), []string{"show", "--db", dbPath, task.ID}, &out, &errOut); code != 0 {
		t.Fatalf("show exit = %d, stderr = %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "PO #77 approved.") || !strings.Contains(out.String(), "TASK_STATE_COMPLETED") {
		t.Fatalf("show output missing reply or completed state:\n%s", out.String())
	}
}

func TestRun_Claim_ReplyWithFile_Clarify_And_WorkOnce(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_SUBMITTED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}
	task, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		Text: "Generate security report for cluster-9",
	})
	if err != nil {
		t.Fatalf("RegisterMessage() error = %v", err)
	}

	var out, errOut bytes.Buffer

	// 1. Responder claims the task
	if code := run(context.Background(), []string{"claim", "--db", dbPath, "--worker", "sec-worker-1", task.ID, "Analyzing cluster-9"}, &out, &errOut); code != 0 {
		t.Fatalf("claim exit = %d, stderr = %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "TASK_STATE_WORKING") {
		t.Fatalf("claim output missing TASK_STATE_WORKING:\n%s", out.String())
	}

	// 2. Responder asks requester for additional info (`--state input-required`) and attaches a checklist file
	checklistPath := filepath.Join(tmpDir, "checklist.md")
	if err := os.WriteFile(checklistPath, []byte("- [ ] Confirm namespace\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	out.Reset()
	if code := run(context.Background(), []string{
		"reply", "--db", dbPath,
		"--worker", "sec-worker-1",
		"--state", "input-required",
		"--file", checklistPath,
		task.ID, "Please confirm target namespace in checklist.md",
	}, &out, &errOut); code != 0 {
		t.Fatalf("reply(input-required) exit = %d, stderr = %s", code, errOut.String())
	}

	// 3. Requester replies on the same taskId with the answer
	if _, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		TaskID:    task.ID,
		ContextID: task.ContextID,
		Text:      "Target namespace is prod-payments",
	}); err != nil {
		t.Fatalf("RegisterMessage(follow-up) error = %v", err)
	}

	// 4. Run `a2a-mailbox work --once` to process actionable tasks automatically!
	reportFile := filepath.Join(tmpDir, "cluster-9-audit.txt")
	if err := os.WriteFile(reportFile, []byte("AUDIT PASS: prod-payments\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	out.Reset()
	if code := run(context.Background(), []string{
		"work", "--once",
		"--db", dbPath,
		"--worker", "sec-worker-auto",
		"--state", "completed",
		"--file", reportFile,
		"--reply", "Automated audit finished for prod-payments.",
	}, &out, &errOut); code != 0 {
		t.Fatalf("work --once exit = %d, stderr = %s", code, errOut.String())
	}

	// 5. Verify final task has both attached files (`checklist.md` and `cluster-9-audit.txt`) and is COMPLETED
	finalTask, err := db.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if finalTask.Status.State != "TASK_STATE_COMPLETED" {
		t.Fatalf("final state = %q, want TASK_STATE_COMPLETED", finalTask.Status.State)
	}
	var artifactNames []string
	for _, a := range finalTask.Artifacts {
		artifactNames = append(artifactNames, a.Name)
	}
	joined := strings.Join(artifactNames, ",")
	if !strings.Contains(joined, "checklist.md") || !strings.Contains(joined, "cluster-9-audit.txt") {
		t.Fatalf("expected checklist.md and cluster-9-audit.txt in artifacts, got %v", artifactNames)
	}
}

func TestRun_DotEnvAndPythonReportResponderDemo(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not found on PATH")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	scriptAbs, err := filepath.Abs(filepath.Join("examples", "report_responder.py"))
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}

	// Create a `.env` file with [a2a-mailbox], [workers], and [hooks] sections
	envFile := filepath.Join(tmpDir, ".env")
	envContent := strings.Join([]string{
		"# [a2a-mailbox]",
		"A2A_MAILBOX_DB_PATH=" + dbPath,
		"# [workers]",
		"A2A_WORKER_ID=py-report-worker",
		"A2A_WORKER_EXEC=python3 " + scriptAbs,
		"# [hooks]",
		"A2A_HOOK_AFTER_READ=python3 " + scriptAbs,
	}, "\n") + "\n"
	if err := os.WriteFile(envFile, []byte(envContent), 0o600); err != nil {
		t.Fatalf("WriteFile(.env): %v", err)
	}

	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}
	task, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		Text: "Summarize Q4 latency and throughput benchmarks across 12 regions",
	})
	if err != nil {
		t.Fatalf("RegisterMessage() error = %v", err)
	}

	// Run `a2a-mailbox work --once --env-file <envFile>` — uses A2A_MAILBOX_DB_PATH and A2A_WORKER_EXEC from .env!
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"work", "--once", "--env-file", envFile}, &out, &errOut); code != 0 {
		t.Fatalf("work --once --env-file exit = %d, stderr = %s", code, errOut.String())
	}

	if !strings.Contains(out.String(), "A2A-MAILBOX INCOMING MAIL REPORT") || !strings.Contains(out.String(), "Summarize Q4 latency") {
		t.Fatalf("expected Python report box on STDOUT, got:\n%s", out.String())
	}

	finalTask, err := db.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if finalTask.Status.State != "TASK_STATE_COMPLETED" {
		t.Fatalf("finalTask state = %q, want TASK_STATE_COMPLETED", finalTask.Status.State)
	}
}
