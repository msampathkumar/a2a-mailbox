package evals_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/msampathkumar/a2a-mailbox/internal/server"
	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

func TestEval1_DBBackend_With_Real_A2A_CLI(t *testing.T) {
	t.Parallel()

	a2aPath, err := exec.LookPath("a2a")
	if err != nil {
		t.Skipf("a2a CLI not found on PATH: %v", err)
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}

	var srv *httptest.Server
	srv = httptest.NewServer(server.NewHandler(server.Config{
		Name:    "A2A DB Mailbox",
		Version: "0.1.0",
		Backend: db,
	}))
	t.Cleanup(srv.Close)

	// 1. Verify `a2a card get` works against the live mailbox server.
	cardOut := runCmd(t, "", a2aPath, "card", "get", srv.URL, "-o", "json")
	if !strings.Contains(cardOut, "A2A DB Mailbox") || !strings.Contains(cardOut, "HTTP+JSON") {
		t.Fatalf("unexpected agent card output:\n%s", cardOut)
	}

	// 2. Send a message using real `a2a send`.
	sendOut := runCmd(t, "", a2aPath, "send", "-a", srv.URL, "Deploy service v2 to staging", "-o", "json")
	task := decodeSendTask(t, sendOut)
	if task.ID == "" || task.ContextID == "" {
		t.Fatalf("expected non-empty task.ID and task.ContextID, got %+v", task)
	}
	if !strings.Contains(strings.ToUpper(task.Status.State), "INPUT") {
		t.Fatalf("state = %q, want INPUT_REQUIRED", task.Status.State)
	}

	// 3. Reply and complete the task via the DBBackend (simulating `a2a-mailbox reply`).
	if _, err := db.ReplyTask(context.Background(), task.ID, "Staging deployment v2 complete.", "TASK_STATE_COMPLETED"); err != nil {
		t.Fatalf("ReplyTask() error = %v", err)
	}

	// 4. Verify `a2a task get` returns the completed task and reply message.
	getOut := runCmd(t, "", a2aPath, "task", "get", "-a", srv.URL, task.ID, "-o", "json")
	if !strings.Contains(strings.ToUpper(getOut), "COMPLETED") || !strings.Contains(getOut, "Staging deployment v2 complete.") {
		t.Fatalf("a2a task get missing completed state or reply:\n%s", getOut)
	}

	// 5. Verify `a2a task list` lists the task under its context.
	listOut := runCmd(t, "", a2aPath, "task", "list", "-a", srv.URL, "--context", task.ContextID, "-o", "json")
	if !strings.Contains(listOut, task.ID) {
		t.Fatalf("a2a task list missing task ID %s:\n%s", task.ID, listOut)
	}
}

func TestEval2_TKBackend_With_Real_TK_And_A2A_CLI(t *testing.T) {
	t.Parallel()

	a2aPath, err := exec.LookPath("a2a")
	if err != nil {
		t.Skipf("a2a CLI not found on PATH: %v", err)
	}
	tkPath, err := exec.LookPath("tk")
	if err != nil {
		t.Skipf("tk CLI not found on PATH: %v", err)
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}
	tkBackend, err := store.NewTKBackend(db, tkPath, tmpDir)
	if err != nil {
		t.Fatalf("NewTKBackend() error = %v", err)
	}

	srv := httptest.NewServer(server.NewHandler(server.Config{
		Name:    "A2A TK Mailbox",
		Version: "0.1.0",
		Backend: tkBackend,
	}))
	t.Cleanup(srv.Close)

	// 1. Send an A2A message via real `a2a send`.
	sendOut := runCmd(t, "", a2aPath, "send", "-a", srv.URL, "Investigate production alert #404", "-o", "json")
	task := decodeSendTask(t, sendOut)
	ticketID, _ := task.Metadata["tkTicketId"].(string)
	if ticketID == "" {
		t.Fatalf("expected tkTicketId in task metadata, got %+v", task.Metadata)
	}

	// 2. Verify `tk show <ticketID>` contains the A2A Task ID, Context ID, and message text.
	tkShow := runCmd(t, tmpDir, tkPath, "show", ticketID)
	if !strings.Contains(tkShow, task.ID) || !strings.Contains(tkShow, "Investigate production alert #404") {
		t.Fatalf("tk show missing A2A task details:\n%s", tkShow)
	}

	// 3. Close the ticket directly using `tk close <ticketID>`.
	runCmd(t, tmpDir, tkPath, "close", ticketID)

	// 4. Query the A2A server via `a2a task get` and verify the task automatically synced to COMPLETED!
	getOut := runCmd(t, "", a2aPath, "task", "get", "-a", srv.URL, task.ID, "-o", "json")
	if !strings.Contains(strings.ToUpper(getOut), "COMPLETED") {
		t.Fatalf("expected task state to sync to COMPLETED after tk close, got:\n%s", getOut)
	}
}

func TestEval3_PubSubBackend_With_Real_A2A_CLI(t *testing.T) {
	t.Parallel()

	a2aPath, err := exec.LookPath("a2a")
	if err != nil {
		t.Skipf("a2a CLI not found on PATH: %v", err)
	}

	var mu sync.Mutex
	var publishedPayloads [][]byte

	pubsubSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Messages []struct {
				Data string `json:"data"`
			} `json:"messages"`
		}
		_ = json.Unmarshal(body, &req)
		for _, m := range req.Messages {
			decoded, err := base64.StdEncoding.DecodeString(m.Data)
			if err == nil {
				mu.Lock()
				publishedPayloads = append(publishedPayloads, decoded)
				mu.Unlock()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"messageIds":["pubsub-eval-msg-99"]}`))
	}))
	t.Cleanup(pubsubSrv.Close)

	tmpDir := t.TempDir()
	db, err := store.NewDBBackend(filepath.Join(tmpDir, "mailbox.json"), "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}
	psBackend, err := store.NewPubSubBackend(db, pubsubSrv.URL, "prod-project", "a2a-inbox", "eval-token")
	if err != nil {
		t.Fatalf("NewPubSubBackend() error = %v", err)
	}

	srv := httptest.NewServer(server.NewHandler(server.Config{
		Name:    "A2A PubSub Mailbox",
		Version: "0.1.0",
		Backend: psBackend,
	}))
	t.Cleanup(srv.Close)

	sendOut := runCmd(t, "", a2aPath, "send", "-a", srv.URL, "Stream order #888 to BigQuery", "-o", "json")
	if !strings.Contains(sendOut, "pubsub-eval-msg-99") {
		t.Fatalf("expected pubsubMessageId in task response:\n%s", sendOut)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(publishedPayloads) != 1 {
		t.Fatalf("publishedPayloads count = %d, want 1", len(publishedPayloads))
	}
	if !strings.Contains(string(publishedPayloads[0]), "Stream order #888 to BigQuery") {
		t.Fatalf("published payload missing message text: %s", string(publishedPayloads[0]))
	}
}

func TestEval4_DualPlugin_A2ASelect_And_A2AMailbox_Together(t *testing.T) {
	t.Parallel()

	selectPath, err := exec.LookPath("a2a-select")
	if err != nil {
		t.Skipf("a2a-select binary not found on PATH: %v", err)
	}
	tkPath, err := exec.LookPath("tk")
	if err != nil {
		t.Skipf("tk CLI not found on PATH: %v", err)
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}
	tkBackend, err := store.NewTKBackend(db, tkPath, tmpDir)
	if err != nil {
		t.Fatalf("NewTKBackend() error = %v", err)
	}

	srv := httptest.NewServer(server.NewHandler(server.Config{
		Name:    "Mailbox Ticket Agent",
		Version: "0.1.0",
		Backend: tkBackend,
	}))
	t.Cleanup(srv.Close)

	// Save the Mailbox Agent Card to a local file for `a2a-select`.
	cardResp, err := http.Get(srv.URL + "/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("fetching agent card: %v", err)
	}
	cardBytes, _ := io.ReadAll(cardResp.Body)
	_ = cardResp.Body.Close()

	cardsDir := filepath.Join(tmpDir, "cards")
	sessionsFile := filepath.Join(tmpDir, "sessions.json")
	cardFile := filepath.Join(cardsDir, "mailbox-agent.json")
	if err := os.MkdirAll(cardsDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(cardFile, cardBytes, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Drive `a2a-select` interactively against the live `a2a-mailbox` server for 2 turns, then exit!
	// Input sequence:
	//   Turn 1: "Provision 4 H100 GPUs"
	//   Turn 2: "Please place them in us-central1-a"
	//   Turn 3: "/exit"
	userInput := "Provision 4 H100 GPUs\nPlease place them in us-central1-a\n/exit\n"
	cmd := exec.Command(selectPath,
		"--cards-dir", cardsDir,
		"--sessions-file", sessionsFile,
		"--card", cardFile,
	)
	cmd.Stdin = strings.NewReader(userInput)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("a2a-select failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	outStr := stdout.String()
	if !strings.Contains(outStr, "Queued in A2A Mailbox -> tk ticket") {
		t.Fatalf("expected a2a-select output to show tk ticket ack from a2a-mailbox, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "Queued follow-up in A2A Mailbox & updated tk ticket") {
		t.Fatalf("expected a2a-select second turn to append to same tk ticket, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "=== Continue This Conversation Later ===") {
		t.Fatalf("expected resume instructions on exit, got:\n%s", outStr)
	}

	// Verify the underlying `tk` ticket has both turns recorded!
	tasks, err := tkBackend.ListTasks(context.Background(), "", "")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("ListTasks() = %d tasks (err=%v), want 1", len(tasks), err)
	}
	ticketID, _ := tasks[0].Metadata["tkTicketId"].(string)
	tkShow := runCmd(t, tmpDir, tkPath, "show", ticketID)
	if !strings.Contains(tkShow, "Provision 4 H100 GPUs") || !strings.Contains(tkShow, "Please place them in us-central1-a") {
		t.Fatalf("tk ticket %s missing multi-turn conversation:\n%s", ticketID, tkShow)
	}
}

func decodeSendTask(t *testing.T, raw string) store.Task {
	t.Helper()
	var env struct {
		Task *store.Task `json:"task"`
	}
	if err := json.Unmarshal([]byte(raw), &env); err == nil && env.Task != nil && env.Task.ID != "" {
		return *env.Task
	}
	var task store.Task
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		t.Fatalf("failed to decode task JSON: %v\nraw:\n%s", err, raw)
	}
	return task
}

func runCmd(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %v failed: %v\nstdout: %s\nstderr: %s", name, args, err, stdout.String(), stderr.String())
	}
	return stdout.String()
}
