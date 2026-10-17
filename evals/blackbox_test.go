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
	"sync"
	"testing"
	"time"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

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
	var srvStdout, srvStderr syncBuffer
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

	// 5. Run `a2a-mailbox claim --db <dbPath> --worker worker-bb-1 <taskID>` via the compiled binary.
	claimOut := runCmd(t, "", bin, "claim", "--db", dbPath, "--worker", "worker-bb-1", taskID, "Claimed cluster provisioning")
	if !strings.Contains(claimOut, "TASK_STATE_WORKING") {
		t.Fatalf("a2a-mailbox claim output missing TASK_STATE_WORKING:\n%s", claimOut)
	}

	// 6. Responder asks requester for clarification + attaches a template file (`--state input-required --file`).
	templateFile := filepath.Join(tmpDir, "cluster-spec.yaml")
	if err := os.WriteFile(templateFile, []byte("nodes: 3\nzone: required\n"), 0o600); err != nil {
		t.Fatalf("writing template file: %v", err)
	}
	clarifyOut := runCmd(t, "", bin, "reply", "--db", dbPath, "--worker", "worker-bb-1", "--state", "input-required", "--file", templateFile, taskID, "Please specify target zone in cluster-spec.yaml")
	if !strings.Contains(clarifyOut, "TASK_STATE_INPUT_REQUIRED") || !strings.Contains(clarifyOut, "files=1") {
		t.Fatalf("a2a-mailbox reply(input-required) output unexpected:\n%s", clarifyOut)
	}

	// 7. Requester replies on the same taskID with the answer over HTTP (`POST /message:send`).
	followUpPayload := fmt.Sprintf(`{"message":{"messageId":"msg-bb-2","taskId":%q,"contextId":%q,"role":"ROLE_USER","parts":[{"text":"Target zone is us-west1-b"}]}}`, taskID, sendResp.Task.ContextID)
	followResp, err := http.Post(baseURL+"/message:send", "application/json", strings.NewReader(followUpPayload))
	if err != nil || followResp.StatusCode != http.StatusOK {
		t.Fatalf("follow-up POST /message:send failed: %v", err)
	}
	_ = followResp.Body.Close()

	// 8. Run `a2a-mailbox work --once --db <dbPath> --file <report> --reply "Cluster provisioned"` via the compiled binary.
	reportFile := filepath.Join(tmpDir, "provisioning-receipt.txt")
	if err := os.WriteFile(reportFile, []byte("CLUSTER_ID=bb-us-west1-b\n"), 0o600); err != nil {
		t.Fatalf("writing report file: %v", err)
	}
	workOut := runCmd(t, "", bin, "work", "--once", "--db", dbPath, "--worker", "worker-bb-1", "--state", "completed", "--file", reportFile, "--reply", "Cluster provisioned")
	if !strings.Contains(workOut, "TASK_STATE_COMPLETED") {
		t.Fatalf("a2a-mailbox work output missing TASK_STATE_COMPLETED:\n%s", workOut)
	}

	// 9. Run `a2a-mailbox show --db <dbPath> <taskID>` via the compiled binary.
	showOut := runCmd(t, "", bin, "show", "--db", dbPath, taskID)
	for _, want := range []string{"Cluster provisioned", "TASK_STATE_COMPLETED", "cluster-spec.yaml", "provisioning-receipt.txt"} {
		if !strings.Contains(showOut, want) {
			t.Fatalf("a2a-mailbox show output missing %q:\n%s", want, showOut)
		}
	}

	// 10. Query the running HTTP server (`GET /tasks/<taskID>`) to confirm it reflects the completed state and file artifacts.
	getResp, err := http.Get(baseURL + "/tasks/" + taskID)
	if err != nil {
		t.Fatalf("GET /tasks/%s failed: %v", taskID, err)
	}
	getBody, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if !strings.Contains(string(getBody), "TASK_STATE_COMPLETED") || !strings.Contains(string(getBody), "provisioning-receipt.txt") {
		t.Fatalf("GET /tasks/%s did not reflect completed reply and file artifact:\n%s", taskID, string(getBody))
	}
}

func TestBlackBox_PythonHookDemo_AutoReportAndFinishOnArrive(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not found on PATH")
	}

	bin := buildMailboxBinary(t)
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mailbox.json")
	port := freeLocalPort(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	scriptAbs, err := filepath.Abs(filepath.Join("..", "examples", "report_responder.py"))
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}

	// Write a `.env` file configuring [a2a-mailbox], [workers], and [hooks]
	envFile := filepath.Join(tmpDir, "demo.env")
	envContent := fmt.Sprintf(`# [a2a-mailbox]
A2A_MAILBOX_HOST=127.0.0.1
A2A_MAILBOX_PORT=%d
A2A_MAILBOX_BACKEND=db
A2A_MAILBOX_DB_PATH=%s

# [workers]
A2A_WORKER_ID=python-report-responder

# [hooks]
A2A_HOOK_AFTER_ARRIVE="python3 %s"
`, port, dbPath, scriptAbs)
	if err := os.WriteFile(envFile, []byte(envContent), 0o600); err != nil {
		t.Fatalf("WriteFile(demo.env): %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvCmd := exec.CommandContext(ctx, bin, "serve", "--env-file", envFile)
	var srvStdout, srvStderr syncBuffer
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
		t.Fatalf("server did not become ready on %s\nstderr: %s", baseURL, srvStderr.String())
	}

	// Send an incoming mail to the mailbox
	payload := `{"message":{"messageId":"msg-py-demo-1","role":"ROLE_USER","parts":[{"text":"Generate executive summary for Q4 AI agent adoption across 8 teams"}]}}`
	resp, err := http.Post(baseURL+"/message:send", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /message:send failed: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	// 1. Verify the task returned is already TASK_STATE_COMPLETED with the Python report artifact!
	if !strings.Contains(string(respBody), "TASK_STATE_COMPLETED") || !strings.Contains(string(respBody), "python-report-responder") {
		t.Fatalf("expected TASK_STATE_COMPLETED from Python after_arrive hook, got:\n%s", string(respBody))
	}

	// 2. Verify the server's STDOUT printed the formatted Python report box!
	stdoutStr := srvStdout.String()
	for _, want := range []string{
		"📬 A2A-MAILBOX INCOMING MAIL REPORT",
		"Trigger Event   : after_arrive",
		"Generate executive summary for Q4 AI agent adoption",
		"Action Taken    : Report printed to STDOUT & Task COMPLETED",
	} {
		if !strings.Contains(stdoutStr, want) {
			t.Fatalf("server STDOUT missing %q:\n%s", want, stdoutStr)
		}
	}
}
