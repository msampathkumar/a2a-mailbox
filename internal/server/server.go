package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

const maxBodyBytes = 4 << 20 // 4 MiB

// Config configures the executor-less A2A Mailbox HTTP server.
type Config struct {
	Name         string
	Description  string
	Version      string
	BaseURL      string
	ResponseMode string // "task" (default) or "message"
	Backend      store.Backend
}

// NewHandler builds an A2A v1.0 HTTP+JSON compliant handler backed by cfg.Backend.
func NewHandler(cfg Config) http.Handler {
	if strings.TrimSpace(cfg.Name) == "" {
		cfg.Name = "A2A Mailbox"
	}
	if strings.TrimSpace(cfg.Description) == "" {
		cfg.Description = "Executor-less A2A inbox that registers and stores incoming tasks (" + cfg.Backend.Name() + " backend)"
	}
	if strings.TrimSpace(cfg.Version) == "" {
		cfg.Version = "0.1.0"
	}

	mux := http.NewServeMux()
	h := &handler{cfg: cfg}

	mux.HandleFunc("GET /.well-known/agent-card.json", h.handleAgentCard)
	mux.HandleFunc("GET /.well-known/agent.json", h.handleAgentCard)
	mux.HandleFunc("POST /message:send", h.handleSendMessage)
	mux.HandleFunc("GET /tasks", h.handleListTasks)
	mux.HandleFunc("GET /tasks/{idAndAction}", h.handleGetTask)
	mux.HandleFunc("POST /tasks/{idAndAction}", h.handleTaskAction)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "backend": cfg.Backend.Name()})
	})

	return mux
}

type handler struct {
	cfg Config
}

func (h *handler) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	baseURL := strings.TrimSpace(h.cfg.BaseURL)
	if baseURL == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		baseURL = scheme + "://" + r.Host
	}

	card := map[string]any{
		"name":        h.cfg.Name,
		"description": h.cfg.Description,
		"version":     h.cfg.Version,
		"supportedInterfaces": []map[string]any{
			{
				"protocolBinding": "HTTP+JSON",
				"protocolVersion": "1.0",
				"url":             baseURL,
			},
		},
		"capabilities": map[string]any{
			"streaming": false,
		},
		"defaultInputModes":  []string{"text/plain", "text/markdown", "application/json", "application/pdf", "application/octet-stream", "image/png", "image/jpeg"},
		"defaultOutputModes": []string{"text/plain", "text/markdown", "application/json", "application/pdf", "application/octet-stream", "image/png", "image/jpeg"},
		"skills": []map[string]any{
			{
				"id":          "mailbox_inbox",
				"name":        "A2A Mailbox Inbox (" + h.cfg.Backend.Name() + ")",
				"description": "Registers incoming A2A requests into " + h.cfg.Backend.Name() + " without running an inline executor",
			},
		},
	}
	writeJSON(w, http.StatusOK, card)
}

func (h *handler) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req struct {
		Message struct {
			MessageID string         `json:"messageId"`
			TaskID    string         `json:"taskId"`
			ContextID string         `json:"contextId"`
			Role      string         `json:"role"`
			Parts     []store.Part   `json:"parts"`
			Metadata  map[string]any `json:"metadata"`
		} `json:"message"`
		Metadata map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload: "+err.Error())
		return
	}

	var texts []string
	for _, p := range req.Message.Parts {
		if strings.TrimSpace(p.Text) != "" {
			texts = append(texts, p.Text)
		} else if strings.TrimSpace(p.Filename) != "" {
			texts = append(texts, "[file: "+p.Filename+"]")
		} else if strings.TrimSpace(p.URL) != "" {
			texts = append(texts, "[url: "+p.URL+"]")
		}
	}
	text := strings.Join(texts, " ")
	if strings.TrimSpace(text) == "" && len(req.Message.Parts) == 0 {
		writeError(w, http.StatusBadRequest, "message must contain non-empty parts")
		return
	}

	task, err := h.cfg.Backend.RegisterMessage(r.Context(), store.IncomingMessage{
		MessageID: req.Message.MessageID,
		TaskID:    req.Message.TaskID,
		ContextID: req.Message.ContextID,
		Text:      text,
		Parts:     req.Message.Parts,
		Metadata:  req.Metadata,
	})
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrTaskNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, store.ErrTaskTerminal) {
			status = http.StatusBadRequest
		}
		writeError(w, status, err.Error())
		return
	}

	mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
	if mode == "" {
		mode = strings.ToLower(strings.TrimSpace(h.cfg.ResponseMode))
	}
	if mode == "message" && task.Status.Message != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"message": task.Status.Message,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"task": task,
	})
}

func (h *handler) handleListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ctxID := q.Get("contextId")
	if ctxID == "" {
		ctxID = q.Get("context")
	}
	status := q.Get("status")

	tasks, err := h.cfg.Backend.ListTasks(r.Context(), ctxID, status)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tasks == nil {
		tasks = []*store.Task{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tasks": tasks,
	})
}

func (h *handler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("idAndAction"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "task ID is required")
		return
	}
	task, err := h.cfg.Backend.GetTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (h *handler) handleTaskAction(w http.ResponseWriter, r *http.Request) {
	idAndAction := strings.TrimSpace(r.PathValue("idAndAction"))
	idx := strings.LastIndex(idAndAction, ":")
	if idx <= 0 {
		writeError(w, http.StatusBadRequest, "unsupported task action")
		return
	}
	taskID := strings.TrimSpace(idAndAction[:idx])
	action := strings.TrimSpace(idAndAction[idx+1:])

	switch action {
	case "cancel":
		task, err := h.cfg.Backend.CancelTask(r.Context(), taskID)
		if err != nil {
			if errors.Is(err, store.ErrTaskNotFound) {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, task)

	case "claim", "promote":
		body, _ := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		var payload struct {
			WorkerID string `json:"workerId"`
			Text     string `json:"text"`
			State    string `json:"state"`
		}
		_ = json.Unmarshal(body, &payload)
		if strings.TrimSpace(payload.Text) == "" {
			if action == "promote" {
				payload.Text = "Simple message promoted to active task"
			} else {
				payload.Text = "Task claimed by responder"
			}
		}
		targetState := "TASK_STATE_WORKING"
		if strings.TrimSpace(payload.State) != "" {
			targetState = payload.State
		}
		task, err := h.cfg.Backend.RespondTask(r.Context(), store.RespondRequest{
			TaskID:   taskID,
			State:    targetState,
			WorkerID: payload.WorkerID,
			Text:     payload.Text,
		})
		if err != nil {
			if errors.Is(err, store.ErrTaskNotFound) {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, task)

	case "reply", "update":
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		if err != nil {
			writeError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		var req store.RespondRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON payload: "+err.Error())
			return
		}
		req.TaskID = taskID
		task, err := h.cfg.Backend.RespondTask(r.Context(), req)
		if err != nil {
			if errors.Is(err, store.ErrTaskNotFound) {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, task)

	default:
		writeError(w, http.StatusBadRequest, "unsupported task action: "+action)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": msg,
		},
	})
}
