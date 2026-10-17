package store_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

func TestDBBackend_RegisterContinueReplyAndList(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}

	// 1. Register new incoming request -> assigns taskID and contextID
	t1, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-1",
		Text:      "Please provision a staging cluster",
	})
	if err != nil {
		t.Fatalf("RegisterMessage(1) error = %v", err)
	}
	if t1.ID == "" || t1.ContextID == "" {
		t.Fatalf("t1 IDs = (%q, %q), want non-empty", t1.ID, t1.ContextID)
	}
	if t1.Status.State != "TASK_STATE_INPUT_REQUIRED" {
		t.Fatalf("t1.Status.State = %q, want TASK_STATE_INPUT_REQUIRED", t1.Status.State)
	}

	// 2. Continue the same task -> appends to History
	t2, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-2",
		TaskID:    t1.ID,
		ContextID: t1.ContextID,
		Text:      "Region: ap-northeast-1",
	})
	if err != nil {
		t.Fatalf("RegisterMessage(2) error = %v", err)
	}
	if t2.ID != t1.ID || len(t2.History) != 2 {
		t.Fatalf("t2 = %+v, want same ID and 2 history messages", t2)
	}

	// 3. Operator replies and completes the task
	completed, err := db.ReplyTask(context.Background(), t1.ID, "Cluster staged in ap-northeast-1", "TASK_STATE_COMPLETED")
	if err != nil {
		t.Fatalf("ReplyTask() error = %v", err)
	}
	if completed.Status.State != "TASK_STATE_COMPLETED" {
		t.Fatalf("completed.Status.State = %q, want TASK_STATE_COMPLETED", completed.Status.State)
	}

	// 4. Attempting to reuse a completed taskID must fail per A2A protocol rules
	if _, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-3",
		TaskID:    t1.ID,
		ContextID: t1.ContextID,
		Text:      "Can I reopen this completed task?",
	}); err == nil {
		t.Fatal("RegisterMessage on completed taskID succeeded, want error")
	}

	// 5. Starting a new task under the same ContextID succeeds!
	t3, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-4",
		ContextID: t1.ContextID,
		Text:      "Now configure DNS for the cluster",
	})
	if err != nil {
		t.Fatalf("RegisterMessage(new task in same context) error = %v", err)
	}
	if t3.ID == t1.ID || t3.ContextID != t1.ContextID {
		t.Fatalf("t3 = %+v, want new task ID in same context %q", t3, t1.ContextID)
	}

	list, err := db.ListTasks(context.Background(), t1.ContextID, "")
	if err != nil || len(list) != 2 {
		t.Fatalf("ListTasks() = %+v, err = %v; want 2 tasks", list, err)
	}
}

func TestTKBackend_CreatesTicketAndSyncsStatus(t *testing.T) {
	t.Parallel()

	tkPath, err := exec.LookPath("tk")
	if err != nil {
		t.Skip("skipping TKBackend test: tk binary not on PATH")
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

	// Register a task -> creates a ticket in `.tickets/`
	task, err := tkBackend.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-tk-1",
		Text:      "Audit warehouse robotics firmware",
	})
	if err != nil {
		t.Fatalf("tkBackend.RegisterMessage() error = %v", err)
	}
	ticketID, _ := task.Metadata["tkTicketId"].(string)
	if strings.TrimSpace(ticketID) == "" {
		t.Fatalf("task.Metadata[tkTicketId] is empty: %+v", task.Metadata)
	}

	// Simulate an external worker or human closing the ticket directly with `tk close <ticketID>`!
	cmd := exec.Command(tkPath, "close", ticketID)
	cmd.Dir = tmpDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("tk close %s error = %v: %s", ticketID, err, string(out))
	}

	// Now GetTask should sync with `tk` and report TASK_STATE_COMPLETED!
	updated, err := tkBackend.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("tkBackend.GetTask() error = %v", err)
	}
	if updated.Status.State != "TASK_STATE_COMPLETED" {
		t.Fatalf("updated.Status.State after tk close = %q, want TASK_STATE_COMPLETED", updated.Status.State)
	}
}

func TestPubSubBackend_PublishesEventEnvelope(t *testing.T) {
	t.Parallel()

	var publishedData []byte
	var publishedAttrs map[string]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Data       string            `json:"data"`
				Attributes map[string]string `json:"attributes"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Messages) == 0 {
			http.Error(w, "bad pubsub request", http.StatusBadRequest)
			return
		}
		publishedData, _ = base64.StdEncoding.DecodeString(body.Messages[0].Data)
		publishedAttrs = body.Messages[0].Attributes
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"messageIds": ["pubsub-msg-1001"]}`))
	}))
	t.Cleanup(srv.Close)

	dbPath := filepath.Join(t.TempDir(), "mailbox.json")
	db, _ := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	ps, err := store.NewPubSubBackend(db, srv.URL, "demo-project", "a2a-mailbox-topic", "")
	if err != nil {
		t.Fatalf("NewPubSubBackend() error = %v", err)
	}

	task, err := ps.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-ps-1",
		Text:      "Process batch invoice #901",
	})
	if err != nil {
		t.Fatalf("ps.RegisterMessage() error = %v", err)
	}
	if publishedAttrs["taskId"] != task.ID || publishedAttrs["contextId"] != task.ContextID {
		t.Fatalf("publishedAttrs = %+v, want taskId=%s and contextId=%s", publishedAttrs, task.ID, task.ContextID)
	}
	if !strings.Contains(string(publishedData), "Process batch invoice #901") {
		t.Fatalf("publishedData = %s, want message text", string(publishedData))
	}
}

func TestResponder_Claim_Clarify_FileAttachments_And_Complete(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_SUBMITTED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}

	// 1. Requester submits initial task
	t1, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-req-1",
		Text:      "Generate Q3 compliance report",
	})
	if err != nil {
		t.Fatalf("RegisterMessage(1) error = %v", err)
	}
	if awaiting, _ := t1.Metadata["awaitingWorker"].(bool); !awaiting {
		t.Fatalf("expected awaitingWorker=true on new task, got %+v", t1.Metadata)
	}

	// 2. Responder claims task -> TASK_STATE_WORKING
	tWorking, err := db.RespondTask(context.Background(), store.RespondRequest{
		TaskID:   t1.ID,
		State:    "TASK_STATE_WORKING",
		WorkerID: "worker-compliance-01",
		Text:     "Claimed task; scanning Q3 ledger...",
	})
	if err != nil {
		t.Fatalf("RespondTask(working) error = %v", err)
	}
	if tWorking.Status.State != "TASK_STATE_WORKING" {
		t.Fatalf("state = %q, want TASK_STATE_WORKING", tWorking.Status.State)
	}
	if gotWorker, _ := tWorking.Metadata["workerId"].(string); gotWorker != "worker-compliance-01" {
		t.Fatalf("workerId = %q, want worker-compliance-01", gotWorker)
	}

	// 3. Responder needs additional info from requester -> TASK_STATE_INPUT_REQUIRED + template file attachment
	templateB64 := base64.StdEncoding.EncodeToString([]byte("region: required\naudit_scope: full\n"))
	tClarify, err := db.RespondTask(context.Background(), store.RespondRequest{
		TaskID:   t1.ID,
		State:    "TASK_STATE_INPUT_REQUIRED",
		WorkerID: "worker-compliance-01",
		Text:     "Which region should Q3 cover? Please fill out the attached scope.yaml.",
		Files: []store.FileAttachment{
			{
				Filename:  "scope.yaml",
				MediaType: "application/x-yaml",
				Raw:       templateB64,
			},
		},
	})
	if err != nil {
		t.Fatalf("RespondTask(input-required) error = %v", err)
	}
	if tClarify.Status.State != "TASK_STATE_INPUT_REQUIRED" {
		t.Fatalf("state = %q, want TASK_STATE_INPUT_REQUIRED", tClarify.Status.State)
	}
	if awaiting, _ := tClarify.Metadata["awaitingWorker"].(bool); awaiting {
		t.Fatalf("expected awaitingWorker=false while waiting for requester input, got %+v", tClarify.Metadata)
	}

	// 4. Requester replies on the same taskId with text + filled file part
	filledB64 := base64.StdEncoding.EncodeToString([]byte("region: eu-west-1\naudit_scope: full\n"))
	tReplied, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-req-2",
		TaskID:    t1.ID,
		ContextID: t1.ContextID,
		Text:      "Here is the completed scope.yaml for eu-west-1",
		Parts: []store.Part{
			{Text: "Here is the completed scope.yaml for eu-west-1"},
			{Filename: "scope-filled.yaml", MediaType: "application/x-yaml", Raw: filledB64},
		},
	})
	if err != nil {
		t.Fatalf("RegisterMessage(2) error = %v", err)
	}
	if awaiting, _ := tReplied.Metadata["awaitingWorker"].(bool); !awaiting {
		t.Fatalf("expected awaitingWorker=true after requester provides requested info, got %+v", tReplied.Metadata)
	}

	// 5. Responder finishes work and attaches the generated report artifact -> TASK_STATE_COMPLETED
	reportB64 := base64.StdEncoding.EncodeToString([]byte("# Q3 Compliance Report (eu-west-1)\nStatus: PASS\n"))
	tDone, err := db.RespondTask(context.Background(), store.RespondRequest{
		TaskID:   t1.ID,
		State:    "TASK_STATE_COMPLETED",
		WorkerID: "worker-compliance-01",
		Text:     "Q3 compliance audit complete. Report attached.",
		Files: []store.FileAttachment{
			{
				Filename:  "q3-compliance-report.md",
				MediaType: "text/markdown",
				Raw:       reportB64,
			},
		},
	})
	if err != nil {
		t.Fatalf("RespondTask(completed) error = %v", err)
	}
	if tDone.Status.State != "TASK_STATE_COMPLETED" {
		t.Fatalf("state = %q, want TASK_STATE_COMPLETED", tDone.Status.State)
	}
	foundReport := false
	for _, art := range tDone.Artifacts {
		if art.Name == "q3-compliance-report.md" && len(art.Parts) > 0 && art.Parts[0].Raw == reportB64 {
			foundReport = true
		}
	}
	if !foundReport {
		t.Fatalf("expected q3-compliance-report.md artifact on completed task, got %+v", tDone.Artifacts)
	}
}

func TestSimpleMessageWithFileParts_PromotesToTask_AndSavesFiles(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	db, err := store.NewDBBackend(dbPath, "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}

	csvData := "id,amount\n101,450.00\n102,890.50\n"
	csvB64 := base64.StdEncoding.EncodeToString([]byte(csvData))

	// 1. Incoming request is a simple message with both a text part and a file part
	msgRecord, err := db.RegisterMessage(context.Background(), store.IncomingMessage{
		MessageID: "msg-simple-1",
		Text:      "Please reconcile this invoice batch",
		Parts: []store.Part{
			{Text: "Please reconcile this invoice batch"},
			{
				Filename:  "invoices.csv",
				MediaType: "text/csv",
				Raw:       csvB64,
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterMessage() error = %v", err)
	}
	if kind, _ := msgRecord.Metadata["kind"].(string); kind != "message" {
		t.Fatalf("initial Metadata[kind] = %q, want %q", kind, "message")
	}

	// 2. When a worker claims or responds to the message, it automatically becomes a task (kind="task")
	promoted, err := db.RespondTask(context.Background(), store.RespondRequest{
		TaskID:   msgRecord.ID,
		State:    "TASK_STATE_WORKING",
		WorkerID: "reconcile-worker",
		Text:     "Promoted message to active reconciliation task",
	})
	if err != nil {
		t.Fatalf("RespondTask() error = %v", err)
	}
	if kind, _ := promoted.Metadata["kind"].(string); kind != "task" {
		t.Fatalf("promoted Metadata[kind] = %q, want %q", kind, "task")
	}

	// 3. Save all file parts (from incoming message and artifacts) to a directory
	outDir := filepath.Join(tmpDir, "extracted")
	savedPaths, err := store.SaveTaskFiles(promoted, outDir)
	if err != nil {
		t.Fatalf("SaveTaskFiles() error = %v", err)
	}
	if len(savedPaths) == 0 {
		t.Fatal("expected SaveTaskFiles to extract invoices.csv, got 0 files")
	}
}
