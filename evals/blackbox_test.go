package evals_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildMailboxBinary compiles `a2a-mailbox` into a temp directory for black-box subprocess testing.
func buildMailboxBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "a2a-mailbox")
	cmd := exec.Command("go", "build", "-trimpath", "-o", binPath, "..")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to compile a2a-mailbox binary: %v\n%s", err, stderr.String())
	}
	return binPath
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free local port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestBlackBox_CompiledBinary_Serve_List_Reply_Show(t *testing.T) {
	t.Parallel()

	bin := buildMailboxBinary(t)
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	port := freeLocalPort(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	// 1. Verify `a2a-mailbox info` and `a2a-mailbox --version`
	infoOut, err := exec.Command(bin, "info").CombinedOutput()
	if err != nil || !strings.Contains(string(infoOut), `"name":"mailbox"`) {
		t.Fatalf("a2a-mailbox info failed: %v, out=%s", err, string(infoOut))
	}
	verOut, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(verOut)) != "a2a-mailbox 0.1.0" {
		t.Fatalf("a2a-mailbox --version failed: %v, out=%s", err, string(verOut))
	}

	// 2. Start `a2a-mailbox serve` as an external background OS process.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvCmd := exec.CommandContext(ctx, bin,
		"serve",
		"--host", "127.0.0.1",
		"--port", fmt.Sprintf("%d", port),
		"--backend", "db",
		"--db", dbPath,
		"--name", "BlackBox Mailbox Server",
	)
	var srvStdout, srvStderr bytes.Buffer
	srvCmd.Stdout = &srvStdout
	srvCmd.Stderr = &srvStderr
	if err := srvCmd.Start(); err != nil {
		t.Fatalf("failed to start a2a-mailbox serve: %v", err)
	}
	t.Cleanup(func() {
		_ = srvCmd.Process.Signal(os.Interrupt)
		cancel()
		_ = srvCmd.Wait()
	})

	// Wait for `/healthz` to become ready.
	ready := false
	for i := 0; i < 50; i++ {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("a2a-mailbox serve did not become ready on %s\nstdout: %s\nstderr: %s",
			baseURL, srvStdout.String(), srvStderr.String())
	}

	// 3. Post an A2A message over HTTP to the live `a2a-mailbox serve` process.
	payload := `{"message":{"messageId":"msg-bb-1","role":"ROLE_USER","parts":[{"text":"Provision black-box test cluster"}]}}`
	resp, err := http.Post(baseURL+"/message:send", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /message:send failed: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /message:send status = %d, body = %s", resp.StatusCode, string(respBody))
	}

	var sendResp struct {
		Task struct {
			ID        string `json:"id"`
			ContextID string `json:"contextId"`
			Status    struct {
				State string `json:"state"`
			} `json:"status"`
		} `json:"task"`
	}
	if err := json.Unmarshal(respBody, &sendResp); err != nil {
		t.Fatalf("decode /message:send response: %v", err)
	}
	taskID := sendResp.Task.ID
	if taskID == "" || sendResp.Task.Status.State != "TASK_STATE_INPUT_REQUIRED" {
		t.Fatalf("unexpected task response: %+v", sendResp)
	}

	// 4. Run `a2a-mailbox list --db <dbPath>` via the compiled binary.
	listOut := runCmd(t, "", bin, "list", "--db", dbPath)
	if !strings.Contains(listOut, taskID) || !strings.Contains(listOut, "Provision black-box test cluster") {
		t.Fatalf("a2a-mailbox list missing task %s:\n%s", taskID, listOut)
	}

	// 5. Run `a2a-mailbox reply --db <dbPath> --state completed <taskID> "Cluster provisioned"` via the compiled binary.
	replyOut := runCmd(t, "", bin, "reply", "--db", dbPath, "--state", "completed", taskID, "Cluster provisioned")
	if !strings.Contains(replyOut, "TASK_STATE_COMPLETED") {
		t.Fatalf("a2a-mailbox reply output missing TASK_STATE_COMPLETED:\n%s", replyOut)
	}

	// 6. Run `a2a-mailbox show --db <dbPath> <taskID>` via the compiled binary.
	showOut := runCmd(t, "", bin, "show", "--db", dbPath, taskID)
	if !strings.Contains(showOut, "Cluster provisioned") || !strings.Contains(showOut, "TASK_STATE_COMPLETED") {
		t.Fatalf("a2a-mailbox show output missing reply or completed state:\n%s", showOut)
	}

	// 7. Query the running HTTP server (`GET /tasks/<taskID>`) to confirm it reflects the CLI reply immediately.
	getResp, err := http.Get(baseURL + "/tasks/" + taskID)
	if err != nil {
		t.Fatalf("GET /tasks/%s failed: %v", taskID, err)
	}
	getBody, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if !strings.Contains(string(getBody), "TASK_STATE_COMPLETED") || !strings.Contains(string(getBody), "Cluster provisioned") {
		t.Fatalf("GET /tasks/%s did not reflect completed reply:\n%s", taskID, string(getBody))
	}
}
