package hooks

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

const hookResultPrefix = "A2A_HOOK_RESULT:"

// Config defines lifecycle hooks for `a2a-mailbox` and its responders/workers.
type Config struct {
	BeforeArrive string
	AfterArrive  string
	BeforeRead   string
	AfterRead    string
	BeforeUpdate string
	AfterUpdate  string
	Out          io.Writer
	ExtraEnv     map[string]string
}

// HasAny reports whether at least one hook is configured.
func (c Config) HasAny() bool {
	return strings.TrimSpace(c.BeforeArrive) != "" ||
		strings.TrimSpace(c.AfterArrive) != "" ||
		strings.TrimSpace(c.BeforeRead) != "" ||
		strings.TrimSpace(c.AfterRead) != "" ||
		strings.TrimSpace(c.BeforeUpdate) != "" ||
		strings.TrimSpace(c.AfterUpdate) != ""
}

// LoadDotEnv parses a `.env` file (supporting `[a2a-mailbox]`, `[workers]`, and `[hooks]` comment sections).
func LoadDotEnv(path string) (map[string]string, error) {
	out := make(map[string]string)
	if strings.TrimSpace(path) == "" {
		return out, nil
	}
	clean := filepath.Clean(path)
	f, err := os.Open(clean)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		out[key] = val
	}
	return out, scanner.Err()
}

// HookedBackend wraps a `store.Backend` and fires lifecycle hooks on arrive, read, and update events.
type HookedBackend struct {
	inner store.Backend
	cfg   Config
	outMu sync.Mutex
}

// NewBackend wraps inner with lifecycle hooks.
func NewBackend(inner store.Backend, cfg Config) *HookedBackend {
	if cfg.Out == nil {
		cfg.Out = os.Stdout
	}
	return &HookedBackend{
		inner: inner,
		cfg:   cfg,
	}
}

func (h *HookedBackend) Name() string {
	return h.inner.Name()
}

func (h *HookedBackend) RegisterMessage(ctx context.Context, msg store.IncomingMessage) (*store.Task, error) {
	// 1. before_arrive
	if _, err := h.runHook(ctx, "before_arrive", h.cfg.BeforeArrive, &store.Task{
		ID:        msg.TaskID,
		ContextID: msg.ContextID,
		Metadata:  msg.Metadata,
	}, msg.Text, ""); err != nil {
		return nil, fmt.Errorf("before_arrive hook rejected message: %w", err)
	}

	// 2. Register in underlying backend
	task, err := h.inner.RegisterMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	// 3. after_arrive (may optionally emit `A2A_HOOK_RESULT: {...}` to auto-update/complete the task!)
	hookAction, err := h.runHook(ctx, "after_arrive", h.cfg.AfterArrive, task, msg.Text, "")
	if err != nil {
		return task, fmt.Errorf("after_arrive hook failed: %w", err)
	}
	if hookAction != nil {
		hookAction.TaskID = task.ID
		updated, err := h.RespondTask(ctx, *hookAction)
		if err != nil {
			return task, err
		}
		return updated, nil
	}
	return task, nil
}

func (h *HookedBackend) GetTask(ctx context.Context, taskID string) (*store.Task, error) {
	// 1. before_read
	if _, err := h.runHook(ctx, "before_read", h.cfg.BeforeRead, &store.Task{ID: taskID}, "", ""); err != nil {
		return nil, fmt.Errorf("before_read hook failed: %w", err)
	}

	task, err := h.inner.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}

	// 2. after_read
	lastText := extractLastText(task)
	if _, err := h.runHook(ctx, "after_read", h.cfg.AfterRead, task, lastText, ""); err != nil {
		return nil, fmt.Errorf("after_read hook failed: %w", err)
	}
	return task, nil
}

func (h *HookedBackend) ListTasks(ctx context.Context, contextID, status string) ([]*store.Task, error) {
	if _, err := h.runHook(ctx, "before_read", h.cfg.BeforeRead, &store.Task{ContextID: contextID}, "", ""); err != nil {
		return nil, fmt.Errorf("before_read hook failed: %w", err)
	}
	tasks, err := h.inner.ListTasks(ctx, contextID, status)
	if err != nil {
		return nil, err
	}
	if _, err := h.runHook(ctx, "after_read", h.cfg.AfterRead, &store.Task{ContextID: contextID}, fmt.Sprintf("listed %d tasks", len(tasks)), ""); err != nil {
		return nil, fmt.Errorf("after_read hook failed: %w", err)
	}
	return tasks, nil
}

func (h *HookedBackend) RespondTask(ctx context.Context, req store.RespondRequest) (*store.Task, error) {
	// 1. before_update
	override, err := h.runHook(ctx, "before_update", h.cfg.BeforeUpdate, &store.Task{
		ID:     req.TaskID,
		Status: store.TaskStatus{State: req.State},
	}, req.Text, req.WorkerID)
	if err != nil {
		return nil, fmt.Errorf("before_update hook rejected update: %w", err)
	}
	if override != nil {
		if override.Text != "" {
			req.Text = override.Text
		}
		if override.State != "" {
			req.State = override.State
		}
		if len(override.Files) > 0 {
			req.Files = append(req.Files, override.Files...)
		}
	}

	// 2. Perform update in underlying backend
	updated, err := h.inner.RespondTask(ctx, req)
	if err != nil {
		return nil, err
	}

	// 3. after_update
	if _, err := h.runHook(ctx, "after_update", h.cfg.AfterUpdate, updated, req.Text, req.WorkerID); err != nil {
		return updated, fmt.Errorf("after_update hook failed: %w", err)
	}
	return updated, nil
}

func (h *HookedBackend) ReplyTask(ctx context.Context, taskID, replyText, newState string) (*store.Task, error) {
	return h.RespondTask(ctx, store.RespondRequest{
		TaskID: taskID,
		Text:   replyText,
		State:  newState,
	})
}

func (h *HookedBackend) CancelTask(ctx context.Context, taskID string) (*store.Task, error) {
	return h.RespondTask(ctx, store.RespondRequest{
		TaskID: taskID,
		Text:   "Task canceled in A2A Mailbox",
		State:  "TASK_STATE_CANCELED",
	})
}

func (h *HookedBackend) runHook(
	ctx context.Context,
	eventName string,
	hookCmd string,
	task *store.Task,
	messageText string,
	workerID string,
) (*store.RespondRequest, error) {
	trimmed := strings.TrimSpace(hookCmd)
	if trimmed == "" {
		return nil, nil
	}
	if strings.ContainsAny(trimmed, ";|&$`<>") {
		return nil, fmt.Errorf("invalid shell characters in hook command %q", trimmed)
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return nil, nil
	}
	resolved, err := exec.LookPath(fields[0])
	if err != nil {
		return nil, fmt.Errorf("resolving hook executable %q: %w", fields[0], err)
	}

	var taskID, ctxID, state string
	if task != nil {
		taskID = task.ID
		ctxID = task.ContextID
		state = task.Status.State
	}
	if messageText == "" && task != nil {
		messageText = extractLastText(task)
	}

	payload := map[string]any{
		"event":     eventName,
		"taskId":    taskID,
		"contextId": ctxID,
		"state":     state,
		"workerId":  workerID,
		"text":      messageText,
		"task":      task,
	}
	payloadBytes, _ := json.Marshal(payload)
	taskJSON, _ := json.Marshal(task)

	cmd := exec.CommandContext(ctx, filepath.Clean(resolved), fields[1:]...)
	cmd.Stdin = bytes.NewReader(payloadBytes)

	env := os.Environ()
	for k, v := range h.cfg.ExtraEnv {
		env = append(env, k+"="+v)
	}
	env = append(env,
		"A2A_HOOK_EVENT="+eventName,
		"A2A_TASK_ID="+taskID,
		"A2A_CONTEXT_ID="+ctxID,
		"A2A_TASK_STATE="+state,
		"A2A_WORKER_ID="+workerID,
		"A2A_MESSAGE_TEXT="+messageText,
		"A2A_TASK_JSON="+string(taskJSON),
	)
	cmd.Env = env

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	var resultReq *store.RespondRequest
	var displayLines []string
	for _, line := range strings.Split(stdout.String(), "\n") {
		trimmedLine := strings.TrimSpace(line)
		if strings.HasPrefix(trimmedLine, hookResultPrefix) {
			rawJSON := strings.TrimSpace(strings.TrimPrefix(trimmedLine, hookResultPrefix))
			var parsed store.RespondRequest
			if json.Unmarshal([]byte(rawJSON), &parsed) == nil {
				parsed.State = store.NormalizeProtocolState(parsed.State)
				resultReq = &parsed
			}
			continue
		}
		displayLines = append(displayLines, line)
	}

	displayOut := strings.Join(displayLines, "\n")
	if strings.TrimSpace(displayOut) != "" {
		h.outMu.Lock()
		_, _ = io.WriteString(h.cfg.Out, displayOut)
		if !strings.HasSuffix(displayOut, "\n") {
			_, _ = io.WriteString(h.cfg.Out, "\n")
		}
		h.outMu.Unlock()
	}

	if runErr != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg != "" {
			return nil, fmt.Errorf("%w: %s", runErr, errMsg)
		}
		return nil, runErr
	}
	return resultReq, nil
}

func extractLastText(t *store.Task) string {
	if t == nil || len(t.History) == 0 {
		return ""
	}
	last := t.History[len(t.History)-1]
	for _, p := range last.Parts {
		if strings.TrimSpace(p.Text) != "" {
			return p.Text
		}
	}
	return ""
}
