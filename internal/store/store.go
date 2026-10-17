package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ErrTaskNotFound indicates a requested task ID does not exist.
var ErrTaskNotFound = errors.New("task not found")

// ErrTaskTerminal indicates a task has already reached a terminal state and cannot accept new turns.
var ErrTaskTerminal = errors.New("task is in a terminal state and cannot be restarted")

// Part represents a message or artifact content part.
type Part struct {
	Text string `json:"text,omitempty"`
}

// Message represents an A2A v1.0 Message.
type Message struct {
	MessageID string `json:"messageId"`
	TaskID    string `json:"taskId,omitempty"`
	ContextID string `json:"contextId,omitempty"`
	Role      string `json:"role"`
	Parts     []Part `json:"parts"`
}

// Artifact represents an A2A v1.0 Artifact.
type Artifact struct {
	ArtifactID string `json:"artifactId"`
	Name       string `json:"name,omitempty"`
	Parts      []Part `json:"parts"`
}

// TaskStatus represents an A2A v1.0 TaskStatus.
type TaskStatus struct {
	State     string     `json:"state"`
	Message   *Message   `json:"message,omitempty"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

// Task represents an A2A v1.0 Task stored in the mailbox.
type Task struct {
	ID        string         `json:"id"`
	ContextID string         `json:"contextId"`
	Status    TaskStatus     `json:"status"`
	Artifacts []Artifact     `json:"artifacts,omitempty"`
	History   []Message      `json:"history,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// IncomingMessage holds a normalized request sent to the mailbox.
type IncomingMessage struct {
	MessageID string
	TaskID    string
	ContextID string
	Text      string
	Metadata  map[string]any
}

// Backend defines the storage and queue contract for `a2a-mailbox`.
type Backend interface {
	Name() string
	RegisterMessage(ctx context.Context, msg IncomingMessage) (*Task, error)
	GetTask(ctx context.Context, taskID string) (*Task, error)
	ListTasks(ctx context.Context, contextID, status string) ([]*Task, error)
	ReplyTask(ctx context.Context, taskID, replyText, newState string) (*Task, error)
	CancelTask(ctx context.Context, taskID string) (*Task, error)
}

// DBBackend persists mailbox tasks in a local JSON database file.
type DBBackend struct {
	mu       sync.Mutex
	filePath string
	ackState string
}

type dbFile struct {
	Tasks []*Task `json:"tasks"`
}

// NewDBBackend initializes a local file-backed mailbox store.
func NewDBBackend(filePath, defaultAckState string) (*DBBackend, error) {
	if strings.TrimSpace(filePath) == "" {
		return nil, errors.New("database file path is required")
	}
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("resolving db path: %w", err)
	}
	cleanPath := filepath.Clean(absPath)
	if err := os.MkdirAll(filepath.Dir(cleanPath), 0o700); err != nil {
		return nil, fmt.Errorf("creating db directory: %w", err)
	}
	state := NormalizeProtocolState(defaultAckState)
	if state == "" {
		state = "TASK_STATE_INPUT_REQUIRED"
	}
	return &DBBackend{
		filePath: cleanPath,
		ackState: state,
	}, nil
}

func (db *DBBackend) Name() string { return "db" }

// RegisterMessage records an incoming message, creating a new task or appending to an existing non-terminal task.
func (db *DBBackend) RegisterMessage(_ context.Context, in IncomingMessage) (*Task, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks, err := db.loadLocked()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	msgID := strings.TrimSpace(in.MessageID)
	if msgID == "" {
		msgID = newID("msg")
	}

	if strings.TrimSpace(in.TaskID) != "" {
		for _, t := range tasks {
			if t.ID == strings.TrimSpace(in.TaskID) {
				if IsTerminalProtocolState(t.Status.State) {
					return nil, fmt.Errorf("%w: %s (%s)", ErrTaskTerminal, t.ID, t.Status.State)
				}
				userMsg := Message{
					MessageID: msgID,
					TaskID:    t.ID,
					ContextID: t.ContextID,
					Role:      "ROLE_USER",
					Parts:     []Part{{Text: in.Text}},
				}
				t.History = append(t.History, userMsg)
				ackText := fmt.Sprintf("Mailbox recorded follow-up message (#%d) for task %s in context %s", len(t.History), t.ID, t.ContextID)
				t.Status = TaskStatus{
					State:     db.ackState,
					Timestamp: &now,
					Message: &Message{
						MessageID: newID("ack"),
						TaskID:    t.ID,
						ContextID: t.ContextID,
						Role:      "ROLE_AGENT",
						Parts:     []Part{{Text: ackText}},
					},
				}
				if err := db.saveLocked(tasks); err != nil {
					return nil, err
				}
				return cloneTask(t), nil
			}
		}
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, in.TaskID)
	}

	taskID := newID("task")
	ctxID := strings.TrimSpace(in.ContextID)
	if ctxID == "" {
		ctxID = newID("ctx")
	}

	userMsg := Message{
		MessageID: msgID,
		TaskID:    taskID,
		ContextID: ctxID,
		Role:      "ROLE_USER",
		Parts:     []Part{{Text: in.Text}},
	}
	ackText := fmt.Sprintf("Queued in A2A Mailbox (task=%s, context=%s)", taskID, ctxID)
	task := &Task{
		ID:        taskID,
		ContextID: ctxID,
		Status: TaskStatus{
			State:     db.ackState,
			Timestamp: &now,
			Message: &Message{
				MessageID: newID("ack"),
				TaskID:    taskID,
				ContextID: ctxID,
				Role:      "ROLE_AGENT",
				Parts:     []Part{{Text: ackText}},
			},
		},
		Artifacts: []Artifact{
			{
				ArtifactID: newID("art"),
				Name:       "mailbox-receipt",
				Parts:      []Part{{Text: ackText}},
			},
		},
		History:  []Message{userMsg},
		Metadata: map[string]any{"backend": "db"},
	}
	for k, v := range in.Metadata {
		task.Metadata[k] = v
	}

	tasks = append(tasks, task)
	if err := db.saveLocked(tasks); err != nil {
		return nil, err
	}
	return cloneTask(task), nil
}

// UpdateMetadata attaches metadata and an optional status update to a stored task.
func (db *DBBackend) UpdateMetadata(taskID string, meta map[string]any, statusMsg string, newState string) (*Task, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks, err := db.loadLocked()
	if err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if t.ID == taskID {
			if t.Metadata == nil {
				t.Metadata = make(map[string]any)
			}
			for k, v := range meta {
				t.Metadata[k] = v
			}
			now := time.Now().UTC()
			if newState != "" {
				t.Status.State = NormalizeProtocolState(newState)
				t.Status.Timestamp = &now
			}
			if statusMsg != "" {
				t.Status.Message = &Message{
					MessageID: newID("ack"),
					TaskID:    t.ID,
					ContextID: t.ContextID,
					Role:      "ROLE_AGENT",
					Parts:     []Part{{Text: statusMsg}},
				}
				t.Artifacts = []Artifact{
					{
						ArtifactID: newID("art"),
						Name:       "mailbox-receipt",
						Parts:      []Part{{Text: statusMsg}},
					},
				}
			}
			if err := db.saveLocked(tasks); err != nil {
				return nil, err
			}
			return cloneTask(t), nil
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
}

// GetTask retrieves a task by ID.
func (db *DBBackend) GetTask(_ context.Context, taskID string) (*Task, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks, err := db.loadLocked()
	if err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if t.ID == strings.TrimSpace(taskID) {
			return cloneTask(t), nil
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
}

// ListTasks lists tasks matching optional contextID and status filters.
func (db *DBBackend) ListTasks(_ context.Context, contextID, status string) ([]*Task, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks, err := db.loadLocked()
	if err != nil {
		return nil, err
	}
	wantCtx := strings.TrimSpace(contextID)
	wantState := NormalizeProtocolState(status)

	var out []*Task
	for _, t := range tasks {
		if wantCtx != "" && t.ContextID != wantCtx {
			continue
		}
		if wantState != "" && t.Status.State != wantState {
			continue
		}
		out = append(out, cloneTask(t))
	}
	return out, nil
}

// ReplyTask records an operator/worker reply artifact on a task and updates its state.
func (db *DBBackend) ReplyTask(_ context.Context, taskID, replyText, newState string) (*Task, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks, err := db.loadLocked()
	if err != nil {
		return nil, err
	}
	state := NormalizeProtocolState(newState)
	if state == "" {
		state = "TASK_STATE_COMPLETED"
	}
	now := time.Now().UTC()
	for _, t := range tasks {
		if t.ID == strings.TrimSpace(taskID) {
			agentMsg := Message{
				MessageID: newID("reply"),
				TaskID:    t.ID,
				ContextID: t.ContextID,
				Role:      "ROLE_AGENT",
				Parts:     []Part{{Text: replyText}},
			}
			t.History = append(t.History, agentMsg)
			t.Status = TaskStatus{
				State:     state,
				Timestamp: &now,
				Message:   &agentMsg,
			}
			t.Artifacts = []Artifact{
				{
					ArtifactID: newID("art"),
					Name:       "mailbox-reply",
					Parts:      []Part{{Text: replyText}},
				},
			}
			if err := db.saveLocked(tasks); err != nil {
				return nil, err
			}
			return cloneTask(t), nil
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
}

// CancelTask transitions a task to TASK_STATE_CANCELED.
func (db *DBBackend) CancelTask(_ context.Context, taskID string) (*Task, error) {
	return db.ReplyTask(context.Background(), taskID, "Task canceled in A2A Mailbox", "TASK_STATE_CANCELED")
}

func (db *DBBackend) loadLocked() ([]*Task, error) {
	data, err := os.ReadFile(db.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading mailbox db: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}
	var f dbFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("decoding mailbox db: %w", err)
	}
	return f.Tasks, nil
}

func (db *DBBackend) saveLocked(tasks []*Task) error {
	data, err := json.MarshalIndent(dbFile{Tasks: tasks}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(db.filePath, data, 0o600)
}

func cloneTask(t *Task) *Task {
	raw, _ := json.Marshal(t)
	var cp Task
	_ = json.Unmarshal(raw, &cp)
	return &cp
}

func newID(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}

// NormalizeProtocolState converts user-friendly or protocol states to A2A v1.0 TASK_STATE_* constants.
func NormalizeProtocolState(s string) string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return ""
	}
	upper := strings.ToUpper(strings.ReplaceAll(trimmed, "-", "_"))
	upper = strings.TrimPrefix(upper, "TASK_STATE_")
	switch upper {
	case "SUBMITTED":
		return "TASK_STATE_SUBMITTED"
	case "WORKING", "IN_PROGRESS":
		return "TASK_STATE_WORKING"
	case "COMPLETED", "CLOSED":
		return "TASK_STATE_COMPLETED"
	case "FAILED":
		return "TASK_STATE_FAILED"
	case "CANCELED", "CANCELLED":
		return "TASK_STATE_CANCELED"
	case "REJECTED":
		return "TASK_STATE_REJECTED"
	case "INPUT_REQUIRED", "OPEN":
		return "TASK_STATE_INPUT_REQUIRED"
	case "AUTH_REQUIRED":
		return "TASK_STATE_AUTH_REQUIRED"
	default:
		return "TASK_STATE_" + upper
	}
}

// IsTerminalProtocolState reports whether a task state is terminal.
func IsTerminalProtocolState(s string) bool {
	switch NormalizeProtocolState(s) {
	case "TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED", "TASK_STATE_REJECTED":
		return true
	default:
		return false
	}
}
