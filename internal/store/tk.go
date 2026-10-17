package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// TKBackend wraps a DBBackend and mirrors every task into a local `tk` (.tickets/) repository.
type TKBackend struct {
	db     *DBBackend
	tkPath string
	tkDir  string
}

// NewTKBackend validates the `tk` binary and initializes `.tickets` in tkDir if needed.
func NewTKBackend(db *DBBackend, tkBin, tkDir string) (*TKBackend, error) {
	if db == nil {
		return nil, errors.New("db backend is required")
	}
	bin := strings.TrimSpace(tkBin)
	if bin == "" {
		bin = "tk"
	}
	if strings.ContainsAny(bin, ";|&$`<> \t\n") {
		return nil, fmt.Errorf("invalid characters in tk binary %q", bin)
	}
	resolved, err := exec.LookPath(bin)
	if err != nil {
		return nil, fmt.Errorf("resolving tk binary %q: %w", bin, err)
	}
	absBin, err := filepath.Abs(resolved)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(tkDir) == "" {
		tkDir = "."
	}
	absDir, err := filepath.Abs(tkDir)
	if err != nil {
		return nil, err
	}
	cleanDir := filepath.Clean(absDir)
	if err := os.MkdirAll(cleanDir, 0o700); err != nil {
		return nil, err
	}

	b := &TKBackend{
		db:     db,
		tkPath: filepath.Clean(absBin),
		tkDir:  cleanDir,
	}
	if err := b.ensureInitialized(context.Background()); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *TKBackend) Name() string { return "tk" }

func (b *TKBackend) ensureInitialized(ctx context.Context) error {
	ticketsDir := filepath.Join(b.tkDir, ".tickets")
	if info, err := os.Stat(ticketsDir); err == nil && info.IsDir() {
		return nil
	}
	_, err := b.execTK(ctx, "init")
	return err
}

// RegisterMessage stores the task in DB and creates or updates a corresponding `tk` ticket.
func (b *TKBackend) RegisterMessage(ctx context.Context, msg IncomingMessage) (*Task, error) {
	isContinuation := strings.TrimSpace(msg.TaskID) != ""
	task, err := b.db.RegisterMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	if isContinuation {
		if ticketID, ok := task.Metadata["tkTicketId"].(string); ok && ticketID != "" {
			note := fmt.Sprintf("[A2A Follow-up | Context %s] %s", task.ContextID, msg.Text)
			_, _ = b.execTK(ctx, "add-note", ticketID, note)
			ack := fmt.Sprintf("Queued follow-up in A2A Mailbox & updated tk ticket %s (task=%s, context=%s)", ticketID, task.ID, task.ContextID)
			return b.db.UpdateMetadata(task.ID, map[string]any{"backend": "tk", "tkTicketId": ticketID}, ack, "")
		}
	}

	title := summarizeTitle(msg.Text)
	desc := fmt.Sprintf("A2A Task ID: %s\nA2A Context ID: %s\n\nMessage:\n%s", task.ID, task.ContextID, msg.Text)
	tags := fmt.Sprintf("a2a,context:%s", task.ContextID)

	out, err := b.execTK(ctx, "create", title, "-d", desc, "--external-ref", task.ID, "--tags", tags)
	if err != nil {
		return nil, fmt.Errorf("creating tk ticket: %w", err)
	}
	ticketID := strings.TrimSpace(out)
	ack := fmt.Sprintf("Queued in A2A Mailbox -> tk ticket %s (task=%s, context=%s)", ticketID, task.ID, task.ContextID)
	return b.db.UpdateMetadata(task.ID, map[string]any{
		"backend":    "tk",
		"tkTicketId": ticketID,
	}, ack, "")
}

// GetTask retrieves the task and synchronizes its status with the underlying `tk` ticket.
func (b *TKBackend) GetTask(ctx context.Context, taskID string) (*Task, error) {
	task, err := b.db.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	return b.syncFromTK(ctx, task)
}

// ListTasks lists tasks and synchronizes their statuses from `tk`.
func (b *TKBackend) ListTasks(ctx context.Context, contextID, status string) ([]*Task, error) {
	tasks, err := b.db.ListTasks(ctx, contextID, "")
	if err != nil {
		return nil, err
	}
	wantState := NormalizeProtocolState(status)
	var out []*Task
	for _, t := range tasks {
		synced, err := b.syncFromTK(ctx, t)
		if err != nil {
			synced = t
		}
		if wantState != "" && synced.Status.State != wantState {
			continue
		}
		out = append(out, synced)
	}
	return out, nil
}

// ReplyTask records a reply on the task and updates the `tk` ticket status/notes.
func (b *TKBackend) ReplyTask(ctx context.Context, taskID, replyText, newState string) (*Task, error) {
	task, err := b.db.ReplyTask(ctx, taskID, replyText, newState)
	if err != nil {
		return nil, err
	}
	if ticketID, ok := task.Metadata["tkTicketId"].(string); ok && ticketID != "" {
		_, _ = b.execTK(ctx, "add-note", ticketID, fmt.Sprintf("[A2A Reply] %s", replyText))
		switch task.Status.State {
		case "TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED":
			_, _ = b.execTK(ctx, "close", ticketID)
		case "TASK_STATE_WORKING":
			_, _ = b.execTK(ctx, "status", ticketID, "in_progress")
		}
	}
	return task, nil
}

// CancelTask cancels the task and closes its `tk` ticket.
func (b *TKBackend) CancelTask(ctx context.Context, taskID string) (*Task, error) {
	return b.ReplyTask(ctx, taskID, "Task canceled in A2A Mailbox", "TASK_STATE_CANCELED")
}

func (b *TKBackend) syncFromTK(ctx context.Context, task *Task) (*Task, error) {
	ticketID, _ := task.Metadata["tkTicketId"].(string)
	if strings.TrimSpace(ticketID) == "" {
		return task, nil
	}
	out, err := b.execTK(ctx, "query")
	if err != nil {
		return task, nil
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var item struct {
			ID          string `json:"id"`
			ExternalRef string `json:"external-ref"`
			Status      string `json:"status"`
		}
		if json.Unmarshal([]byte(line), &item) != nil {
			continue
		}
		if item.ID == ticketID || item.ExternalRef == task.ID {
			var mappedState string
			switch strings.ToLower(strings.TrimSpace(item.Status)) {
			case "closed":
				mappedState = "TASK_STATE_COMPLETED"
			case "in_progress":
				mappedState = "TASK_STATE_WORKING"
			}
			if mappedState != "" && mappedState != task.Status.State {
				msg := fmt.Sprintf("Synced from tk ticket %s (status=%s)", ticketID, item.Status)
				updated, err := b.db.UpdateMetadata(task.ID, nil, msg, mappedState)
				if err == nil {
					return updated, nil
				}
			}
			break
		}
	}
	return task, nil
}

func (b *TKBackend) execTK(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, b.tkPath, args...)
	cmd.Dir = b.tkDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg != "" {
			return "", fmt.Errorf("tk %s: %s", args[0], errMsg)
		}
		return "", fmt.Errorf("tk %s: %w", args[0], err)
	}
	return stdout.String(), nil
}

func summarizeTitle(text string) string {
	clean := strings.TrimSpace(strings.ReplaceAll(text, "\n", " "))
	if clean == "" {
		return "A2A Mailbox Task"
	}
	if len(clean) > 72 {
		return clean[:69] + "..."
	}
	return clean
}
