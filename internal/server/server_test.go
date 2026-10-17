package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/msampathkumar/a2a-mailbox/internal/server"
	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

func TestHandler_AgentCard_SendMessage_GetTask_ListTasks_CancelTask(t *testing.T) {
	t.Parallel()

	db, err := store.NewDBBackend(filepath.Join(t.TempDir(), "mailbox.json"), "TASK_STATE_INPUT_REQUIRED")
	if err != nil {
		t.Fatalf("NewDBBackend() error = %v", err)
	}

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := server.NewHandler(server.Config{
			Name:        "Test Mailbox Agent",
			Description: "Executor-less inbox",
			Version:     "0.1.0",
			BaseURL:     srv.URL,
			Backend:     db,
		})
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	// 1. Fetch Agent Card
	resp, err := http.Get(srv.URL + "/.well-known/agent-card.json")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET agent-card.json status = %v, err = %v", resp, err)
	}
	var card struct {
		Name                string `json:"name"`
		SupportedInterfaces []struct {
			ProtocolBinding string `json:"protocolBinding"`
			ProtocolVersion string `json:"protocolVersion"`
			URL             string `json:"url"`
		} `json:"supportedInterfaces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		t.Fatalf("decode card error = %v", err)
	}
	_ = resp.Body.Close()
	if card.Name != "Test Mailbox Agent" || len(card.SupportedInterfaces) == 0 || card.SupportedInterfaces[0].ProtocolVersion != "1.0" {
		t.Fatalf("card = %+v", card)
	}

	// 2. POST /message:send
	sendPayload := []byte(`{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"Queue deployment #42"}]}}`)
	sendResp, err := http.Post(srv.URL+"/message:send", "application/json", bytes.NewReader(sendPayload))
	if err != nil || sendResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /message:send status = %v, err = %v", sendResp, err)
	}
	var streamEnv struct {
		Task store.Task `json:"task"`
	}
	if err := json.NewDecoder(sendResp.Body).Decode(&streamEnv); err != nil {
		t.Fatalf("decode send response error = %v", err)
	}
	_ = sendResp.Body.Close()
	if streamEnv.Task.ID == "" || streamEnv.Task.ContextID == "" {
		t.Fatalf("streamEnv.Task = %+v", streamEnv.Task)
	}

	// 3. GET /tasks/{id}
	getResp, err := http.Get(srv.URL + "/tasks/" + streamEnv.Task.ID)
	if err != nil || getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks/{id} status = %v, err = %v", getResp, err)
	}
	var fetchedTask store.Task
	if err := json.NewDecoder(getResp.Body).Decode(&fetchedTask); err != nil {
		t.Fatalf("decode get task error = %v", err)
	}
	_ = getResp.Body.Close()
	if fetchedTask.ID != streamEnv.Task.ID {
		t.Fatalf("fetchedTask.ID = %q, want %q", fetchedTask.ID, streamEnv.Task.ID)
	}

	// 4. GET /tasks?contextId=...
	listResp, err := http.Get(srv.URL + "/tasks?contextId=" + streamEnv.Task.ContextID)
	if err != nil || listResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks status = %v, err = %v", listResp, err)
	}
	var listEnv struct {
		Tasks []store.Task `json:"tasks"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listEnv); err != nil {
		t.Fatalf("decode list tasks error = %v", err)
	}
	_ = listResp.Body.Close()
	if len(listEnv.Tasks) != 1 || listEnv.Tasks[0].ID != streamEnv.Task.ID {
		t.Fatalf("listEnv.Tasks = %+v", listEnv.Tasks)
	}

	// 5. POST /tasks/{id}:cancel
	cancelResp, err := http.Post(srv.URL+"/tasks/"+streamEnv.Task.ID+":cancel", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil || cancelResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /tasks/{id}:cancel status = %v, err = %v", cancelResp, err)
	}
	var canceledTask store.Task
	if err := json.NewDecoder(cancelResp.Body).Decode(&canceledTask); err != nil {
		t.Fatalf("decode cancel task error = %v", err)
	}
	_ = cancelResp.Body.Close()
	if canceledTask.Status.State != "TASK_STATE_CANCELED" {
		t.Fatalf("canceledTask.Status.State = %q, want TASK_STATE_CANCELED", canceledTask.Status.State)
	}
}
