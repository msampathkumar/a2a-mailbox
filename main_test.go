package main

import (
	"bytes"
	"context"
	"encoding/json"
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
