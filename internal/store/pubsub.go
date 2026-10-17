package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// PubSubBackend stores tasks in DBBackend and publishes every incoming request to a Cloud Pub/Sub topic.
type PubSubBackend struct {
	db         *DBBackend
	endpoint   string
	project    string
	topic      string
	authBearer string
	httpClient *http.Client
}

// NewPubSubBackend configures a Cloud Pub/Sub publisher backend.
func NewPubSubBackend(db *DBBackend, endpoint, project, topic, authBearer string) (*PubSubBackend, error) {
	if db == nil {
		return nil, errors.New("db backend is required")
	}
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "https://pubsub.googleapis.com"
	}
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("invalid pubsub endpoint %q", endpoint)
	}
	if strings.TrimSpace(project) == "" {
		project = "default-project"
	}
	if strings.TrimSpace(topic) == "" {
		topic = "a2a-mailbox"
	}
	return &PubSubBackend{
		db:         db,
		endpoint:   strings.TrimRight(parsed.String(), "/"),
		project:    strings.TrimSpace(project),
		topic:      strings.TrimSpace(topic),
		authBearer: strings.TrimSpace(authBearer),
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (p *PubSubBackend) Name() string { return "pubsub" }

// RegisterMessage stores the task and publishes a Cloud Pub/Sub event message.
func (p *PubSubBackend) RegisterMessage(ctx context.Context, msg IncomingMessage) (*Task, error) {
	task, err := p.db.RegisterMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	msgID, err := p.publishEvent(ctx, "a2a.mailbox.message_received", task, msg.Text)
	if err != nil {
		return nil, fmt.Errorf("publishing to pubsub: %w", err)
	}

	ack := fmt.Sprintf("Queued in A2A Mailbox -> Pub/Sub topic %s (messageId=%s, task=%s, context=%s)",
		p.topic, msgID, task.ID, task.ContextID)
	return p.db.UpdateMetadata(task.ID, map[string]any{
		"backend":         "pubsub",
		"pubsubTopic":     p.topic,
		"pubsubMessageId": msgID,
	}, ack, "")
}

func (p *PubSubBackend) GetTask(ctx context.Context, taskID string) (*Task, error) {
	return p.db.GetTask(ctx, taskID)
}

func (p *PubSubBackend) ListTasks(ctx context.Context, contextID, status string) ([]*Task, error) {
	return p.db.ListTasks(ctx, contextID, status)
}

func (p *PubSubBackend) RespondTask(ctx context.Context, req RespondRequest) (*Task, error) {
	task, err := p.db.RespondTask(ctx, req)
	if err != nil {
		return nil, err
	}
	_, _ = p.publishEvent(ctx, "a2a.mailbox.task_replied", task, req.Text)
	return task, nil
}

func (p *PubSubBackend) ReplyTask(ctx context.Context, taskID, replyText, newState string) (*Task, error) {
	return p.RespondTask(ctx, RespondRequest{
		TaskID: taskID,
		Text:   replyText,
		State:  newState,
	})
}

func (p *PubSubBackend) CancelTask(ctx context.Context, taskID string) (*Task, error) {
	return p.ReplyTask(ctx, taskID, "Task canceled in A2A Mailbox", "TASK_STATE_CANCELED")
}

func (p *PubSubBackend) publishEvent(ctx context.Context, eventType string, task *Task, text string) (string, error) {
	envelope := map[string]any{
		"eventType": eventType,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"taskId":    task.ID,
		"contextId": task.ContextID,
		"state":     task.Status.State,
		"text":      text,
		"task":      task,
	}
	rawData, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}

	pubReq := map[string]any{
		"messages": []map[string]any{
			{
				"data": base64.StdEncoding.EncodeToString(rawData),
				"attributes": map[string]string{
					"eventType": eventType,
					"taskId":    task.ID,
					"contextId": task.ContextID,
					"state":     task.Status.State,
				},
			},
		},
	}
	bodyBytes, err := json.Marshal(pubReq)
	if err != nil {
		return "", err
	}

	publishURL := fmt.Sprintf("%s/v1/projects/%s/topics/%s:publish", p.endpoint, p.project, p.topic)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, publishURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.authBearer != "" {
		req.Header.Set("Authorization", "Bearer "+p.authBearer)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("pubsub HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var parsed struct {
		MessageIDs []string `json:"messageIds"`
	}
	if json.Unmarshal(respBody, &parsed) == nil && len(parsed.MessageIDs) > 0 {
		return parsed.MessageIDs[0], nil
	}
	return "published", nil
}
